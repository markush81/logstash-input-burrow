# Logstash Burrow input plugin

[![Travis Build Status](https://travis-ci.com/markush81/logstash-input-burrow.svg)](https://travis-ci.com/markush81/logstash-input-burrow)

This plugin is based off [logstash-input-http_poller](https://github.com/logstash-plugins/logstash-input-http_poller) by @logstash-plugins.

This [Logstash](https://github.com/elastic/logstash) input plugin allows you to call [Burrow HTTP API](https://github.com/linkedin/Burrow) and send its output as events.

The license is Apache 2.0.

## Config Example

```ruby
input {
  burrow {
    client => {
        url => "http://localhost:8000"
    }
    # Supports "cron", "every", "at" and "in" schedules by rufus scheduler
    # this should relate to Burrow offset-refresh
    schedule => { every => "60s"}
  }
}

output {
  stdout {
    codec => rubydebug
  }
}
```

## Build Plugin

```bash
bundle install
```

## Test Plugin

```bash
bundle exec rspec
```

### Install Plugin from rubygems.org

```bash
logstash-plugin install logstash-input-burrow
```

## Install Plugin

```bash
gem build logstash-input-burrow.gemspec

logstash-plugin install logstash-input-burrow-1.0.0.gem 
```