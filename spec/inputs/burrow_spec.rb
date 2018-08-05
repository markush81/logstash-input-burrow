require "logstash/devutils/rspec/spec_helper"
require 'logstash/inputs/burrow'
require 'flores/random'
require "timecop"
# Workaround for the bug reported in https://github.com/jruby/jruby/issues/4637
require 'rspec/matchers/built_in/raise_error.rb'

describe LogStash::Inputs::Burrow do
  let(:queue) {Queue.new}
  let(:default_schedule) {
    {"every" => "30s"}
  }

  let(:default_url) {"http://localhost:8000"}
  let(:default_client_config) {{"url" => default_url}}
  let(:default_opts) {
    {
        "schedule" => default_schedule,
        "client_config" => default_client_config
    }
  }
  let(:klass) {LogStash::Inputs::Burrow}

  describe "instances" do
    subject {klass.new(default_opts)}

    before do
      subject.register
    end

    describe "#run" do
      it "should setup a scheduler" do
        runner = Thread.new do
          subject.run(double("queue"))
          expect(subject.instance_variable_get("@scheduler")).to be_a_kind_of(Rufus::Scheduler)
        end
        runner.kill
        runner.join
      end
    end

    describe "#run_once" do
      it "should issue an request for each url" do
        normalized_request = subject.send(:normalize_request, default_client_config)
        expect(subject).to receive(:request).with(queue, normalized_request).once

        subject.send(:run_once, queue) # :run_once is a private method
      end
    end

    describe "normalizing a request spec" do
      shared_examples "a normalized request" do
        it "should set the options to the URL string" do
          expect(normalized[0]).to eql(spec_url + "/v3/kafka")
        end
      end

      let(:normalized) {subject.send(:normalize_request, client_config)}

      describe "a string URL" do
        let(:client_config) {default_client_config}
        let(:spec_url) {default_url}

        include_examples("a normalized request")
      end

      describe "URL specs" do

        context "missing an URL" do
          let(:client_config) {}

          it "should raise an error" do
            expect {normalized}.to raise_error(LogStash::ConfigurationError)
          end
        end

        shared_examples "auth" do
          context "with auth enabled but no pass" do
            let(:auth) {{"user" => "foo"}}

            it "should raise an error" do
              expect {normalized}.to raise_error(LogStash::ConfigurationError)
            end
          end

          context "with auth enabled, a path, but no user" do
            let(:client_config) {{"auth" => {"password" => "bar"}}}
            it "should raise an error" do
              expect {normalized}.to raise_error(LogStash::ConfigurationError)
            end
          end
          context "with auth enabled correctly" do
            let(:auth) {{"user" => "foo", "password" => "bar"}}

            it "should raise an error" do
              expect {normalized}.not_to raise_error
            end

            it "should properly set the auth parameter" do
              expect(normalized[1][:auth]).to eq({
                                                     :user => auth["user"],
                                                     :pass => auth["password"],
                                                     :eager => true
                                                 })
            end
          end
        end

        # The new 'right' way to do things
        describe "auth with direct auth options" do
          let(:client_config) {{"url" => "http://localhost", "user" => auth["user"], "password" => auth["password"]}}

          include_examples("auth")
        end
      end
    end

    describe "#structure_request" do
      it "Should turn a simple request into the expected structured request" do
        expected = {"url" => "http://example.net"}
        expect(subject.send(:structure_request, ["http://example.net"])).to eql(expected)
      end

      it "should turn a complex request into the expected structured one" do
        headers = {
            "X-Fry" => " Like a balloon, and... something bad happens! "
        }
        expected = {
            "url" => "http://example.net",
            "headers" => headers
        }
        expect(subject.send(:structure_request, ["http://example.net", {"headers" => headers}])).to eql(expected)
      end
    end
  end

  describe "events" do

    shared_examples "unprocessable_requests" do
      let(:burow) {LogStash::Inputs::Burrow.new(settings)}
      subject(:event) {
        burow.send(:run_once, queue)
        queue.pop(true)
      }

      before do
        burow.register
        allow(burow).to receive(:handle_failure).and_call_original
        allow(burow).to receive(:handle_success)
        event # materialize the subject
      end

      it "should enqueue a message" do
        expect(event).to be_a(LogStash::Event)
      end

      it "should enqueue a message with 'http_request_failure' set" do
        expect(event.get("http_request_failure")).to be_a(Hash)
      end

      it "should tag the event with '_http_request_failure'" do
        expect(event.get("tags")).to include('_http_request_failure')
      end

      it "should invoke handle failure exactly once" do
        expect(burow).to have_received(:handle_failure).once
      end

      it "should not invoke handle success at all" do
        expect(burow).not_to have_received(:handle_success)
      end
    end

    context "with a non responsive server" do
      context "due to a non-existant host" do # Fail with handlers
        let(:url) {"http://thouetnhoeu89ueoueohtueohtneuohn"}
        let(:code) {nil} # no response expected

        let(:settings) {default_opts.merge("client_config" => {"url" => url})}

        include_examples("unprocessable_requests")
      end

      context "due to a bogus port number" do # fail with return?
        let(:invalid_port) {Flores::Random.integer(65536..1000000)}

        let(:url) {"http://127.0.0.1:#{invalid_port}"}
        let(:settings) {default_opts.merge("client_config" => {"url" => url})}
        let(:code) {nil} # No response expected

        include_examples("unprocessable_requests")
      end
    end

    describe "a valid request and decoded response" do
      let(:cluster) {{"clusters" => ["default"]}}
      let(:consumer) {{"consumers" => ["console-1"]}}
      let(:lag) {{"message" => "consumer status returned"}}

      let(:opts) {default_opts}
      let(:instance) {
        klass.new(opts)
      }
      let(:code) {202}

      subject(:event) {
        queue.pop(true)
      }

      before do
        instance.register
        instance.client.stub(default_url + "/v3/kafka",
                             :body => LogStash::Json.dump(cluster),
                             :code => code)

        instance.client.stub(default_url + "/v3/kafka/default/consumer",
                             :body => LogStash::Json.dump(consumer),
                             :code => code)

        instance.client.stub(default_url + "/v3/kafka/default/consumer/console-1/lag",
                             :body => LogStash::Json.dump(lag),
                             :code => code)

        allow(instance).to receive(:decorate)
        instance.send(:run_once, queue)
      end

      it "should have a matching message" do
        expect(event.to_hash).to include(lag)
      end

      it "should decorate the event" do
        expect(instance).to have_received(:decorate).once
      end

      context "with empty cluster" do
        let(:cluster) {{ "clusters" => [] }}

        it "should have no event" do
          expect(instance).to have_received(:decorate).exactly(0).times
        end
      end

      context "with empty consumers" do
        let(:consumer) {{ "consumers" => [] }}

        it "should have no event" do
          expect(instance).to have_received(:decorate).exactly(0).times
        end
      end

      context "with a complex URL spec" do
        let(:client_config) {
          {
              "url" => default_url,
              "headers" => {
                  "X-Fry" => "I'm having one of those things, like a headache, with pictures..."
              }
          }
        }
        let(:opts) {
          {
              "schedule" => {
                  "cron" => "* * * * * UTC"
              },
              "client_config" => client_config
          }
        }

        it "should have a matching message" do
          expect(event.to_hash).to include(lag)
        end
      end
    end
  end

  describe "stopping" do
    let(:config) {default_opts}
    it_behaves_like "an interruptible input plugin"
  end
end
