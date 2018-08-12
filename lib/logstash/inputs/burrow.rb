# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/http_client"
require "socket" # for Socket.gethostname
require "manticore"
require "rufus/scheduler"

class LogStash::Inputs::Burrow < LogStash::Inputs::Base
  include LogStash::PluginMixins::HttpClient

  config_name "burrow"

  # The Burrow Client configuration, with at least url
  # client_config => {
  #         url => "http://localhost:8000"
  # }
  config :client_config, :validate => :hash, :required => true
  # The Burrow API version
  config :api_version, :default => "v3"
  # Schedule of when to periodically poll from the url
  # Format: A hash with
  #   + key: "cron" | "every" | "in" | "at"
  #   + value: string
  # Examples:
  #   a) { "every" => "1h" }
  #   b) { "cron" => "* * * * * UTC" }
  # See: rufus/scheduler for details about different schedule options and value string format
  config :schedule, :validate => :hash, :default => %w(every 30s)

  default :codec, "json"

  Schedule_types = %w(cron every at in)

  def register
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)

    @logger.info("Registering burrow Input", :type => @type, :schedule => @schedule, :timeout => @timeout)

    if @api_version != "v3"
      raise LogStash::ConfigurationError, "at the moment only Burrow API version v3 is supported."
    end

    if @codec.class.config_name != "json"
      raise LogStash::ConfigurationError, "this plugin needs codec to be json."
    end

    setup_request!
  end

  def stop
    Stud.stop!(@interval_thread) if @interval_thread
    @scheduler.stop if @scheduler
  end

  private

  def setup_request!
    @request = normalize_request(client_config)
  end

  def normalize_request(client_spec)
    if client_spec.is_a?(String)
      res = [client_spec + ('/' unless client_spec.end_with?('/')) + @api_version << "/kafka"]
    elsif client_spec.is_a?(Hash)
      # The client will expect keys / values
      spec = Hash[client_spec.clone.map {|k, v| [k.to_sym, v]}] # symbolize keys

      # method and url aren't really part of the options, so we pull them out
      spec.delete(:method)
      url = spec.delete(:url)

      raise LogStash::ConfigurationError, "Invalid URL #{url}" unless URI::DEFAULT_PARSER.regexp[:ABS_URI].match(url)

      # Manticore wants auth options that are like {:auth => {:user => u, :pass => p}}
      # We allow that because earlier versions of this plugin documented that as the main way to
      # to do things, but now prefer top level "user", and "password" options
      # So, if the top level user/password are defined they are moved to the :auth key for manticore
      # if those attributes are already in :auth they still need to be transformed to symbols
      auth = spec[:auth]
      user = spec.delete(:user) || (auth && auth["user"])
      password = spec.delete(:password) || (auth && auth["password"])

      if user.nil? ^ password.nil?
        raise LogStash::ConfigurationError, "'user' and 'password' must both be specified for input HTTP poller!"
      end

      if user && password
        spec[:auth] = {
            user: user,
            pass: password,
            eager: true
        }
      end
      res = [url + ('/' unless url.end_with?('/')) + @api_version << "/kafka", spec]
    else
      raise LogStash::ConfigurationError, "Invalid URL or request spec: '#{client_spec}', expected a String or Hash!"
    end

    validate_request!(res)
    res
  end

  private

  def validate_request!(request)
    url, spec = request

    raise LogStash::ConfigurationError, "Invalid URL #{url}" unless URI::DEFAULT_PARSER.regexp[:ABS_URI].match(url)

    raise LogStash::ConfigurationError, "No URL provided for request! #{url}" unless url
    if spec && spec[:auth]
      unless spec[:auth][:user]
        raise LogStash::ConfigurationError, "Auth was specified, but 'user' was not!"
      end
      unless spec[:auth][:pass]
        raise LogStash::ConfigurationError, "Auth was specified, but 'password' was not!"
      end
    end

    request
  end

  public

  def run(queue)
    setup_schedule(queue)
  end

  def setup_schedule(queue)
    #schedule hash must contain exactly one of the allowed keys
    msg_invalid_schedule = "Invalid config. schedule hash must contain " +
        "exactly one of the following keys - cron, at, every or in"
    raise Logstash::ConfigurationError, msg_invalid_schedule if @schedule.keys.length != 1
    schedule_type = @schedule.keys.first
    schedule_value = @schedule[schedule_type]
    raise LogStash::ConfigurationError, msg_invalid_schedule unless Schedule_types.include?(schedule_type)

    @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
    #as of v3.0.9, :first_in => :now doesn't work. Use the following workaround instead
    opts = schedule_type == "every" ? {:first_in => 0.01} : {}
    @scheduler.send(schedule_type, schedule_value, opts) {run_once(queue)}
    @scheduler.join
  end

  def run_once(queue)
    request(queue, @request)
  end


  private

  def request(queue, request)
    @logger.debug? && @logger.debug?("Fetching URL", :client_config => request)
    started = Time.now

    url, spec = request
    client.get(url, spec).
        on_success do |cluster_response|
      body = cluster_response.body
      if body && body.size > 0
        @codec.decode(body) do |clusters|
          cluster_list = clusters.get('clusters')
          @logger.debug? && @logger.debug?("found clusters", :cluster_list => cluster_list)
          cluster_list.each {|cluster|
            consumer_url = url + "/" << cluster + "/consumer"
            client.get(consumer_url, spec).
                on_success do |consumers_response|
              body = consumers_response.body
              if body && body.size > 0
                @codec.decode(body) do |consumers|
                  consumer_list = consumers.get('consumers')
                  @logger.debug? && @logger.debug?("found consumers", :consumer_list => consumer_list)
                  consumer_list.each {|consumer|
                    lag_url = url + "/" + cluster + "/consumer" + "/" + consumer + "/lag"
                    client.get(lag_url, spec).
                        on_success do |consumer_lag_response|
                      handle_success(queue, [lag_url, spec], consumer_lag_response, Time.now - started)
                    end.
                        on_failure do |exception|
                      handle_failure(queue, [lag_url, spec], exception, Time.now - started)
                    end.call
                  } unless consumer_list.nil?
                end
              end
            end.
                on_failure do |exception|
              handle_failure(queue, [consumer_url, spec], exception, Time.now - started)
            end.call
          } unless cluster_list.nil?
        end
      end
    end.
        on_failure do |exception|
      handle_failure(queue, request, exception, Time.now - started)
    end.call
  end

  private

  def handle_success(queue, request, response, execution_time)
    body = response.body
    # If there is a usable response. HEAD request are `nil` and empty get
    # responses come up as "" which will cause the codec to not yield anything
    if body && body.size > 0
      decode_and_flush(@codec, body) do |decoded|
        event = decoded
        handle_decoded_event(queue, request, response, event, execution_time)
      end
    else
      event = ::LogStash::Event.new
      handle_decoded_event(queue, request, response, event, execution_time)
    end
  end

  private

  def decode_and_flush(codec, body, &yielder)
    codec.decode(body, &yielder)
    codec.flush(&yielder)
  end

  private

  def handle_decoded_event(queue, request, response, event, _execution_time)
    decorate(event)
    queue << event
  rescue StandardError, java.lang.Exception => e
    @logger.error? && @logger.error("Error eventifying response!",
                                    :exception => e,
                                    :exception_message => e.message,
                                    :client_config => request,
                                    :response => response
    )
  end

  private

  # Beware, on old versions of manticore some uncommon failures are not handled
  def handle_failure(queue, request, exception, execution_time)
    event = LogStash::Event.new

    event.tag("_http_request_failure")

    # This is also in the metadata, but we send it anyone because we want this
    # persisted by default, whereas metadata isn't. People don't like mysterious errors
    event.set("http_request_failure", {
        "request" => structure_request(request),
        "error" => exception.to_s,
        "backtrace" => exception.backtrace,
        "runtime_seconds" => execution_time
    })

    queue << event
  rescue StandardError, java.lang.Exception => e
    @logger.error? && @logger.error("Cannot read URL or send the error as an event!",
                                    :exception => e,
                                    :exception_message => e.message,
                                    :exception_backtrace => e.backtrace)

    # If we are running in debug mode we can display more information about the
    # specific request which could give more details about the connection.
    @logger.debug? && @logger.debug("Cannot read URL or send the error as an event!",
                                    :exception => e,
                                    :exception_message => e.message,
                                    :exception_backtrace => e.backtrace,
                                    :client_config => request)
  end

  private

  # Turn [method, url, spec] request into a hash for friendlier logging / ES indexing
  def structure_request(request)
    url, spec = request
    # Flatten everything into the 'spec' hash, also stringify any keys to normalize
    Hash[(spec || {}).merge({
                                "url" => url,
                            }).map {|k, v| [k.to_s, v]}]
  end
end
