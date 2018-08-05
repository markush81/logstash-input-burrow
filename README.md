# Logstash Burrow input plugin

[![Travis Build Status](https://travis-ci.org/markush81/logstash-input-burrow.svg)](https://travis-ci.org/markush81/logstash-input-burrow)

This plugin is based off [logstash-input-http_poller](logstash-input-http_poller) by @elastic.

This [Logstash](https://github.com/elastic/logstash) input plugin allows you to call [Burrow HTTP API](https://github.com/linkedin/Burrow), decode the output of it into event(s), and send them on their way.

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Config Example

For config examples see `burrow.rb` in `lib/logstash/inputs/` in this repo.

## Build Plugin

```
bundle install
```

## Test Plugin

```
bundle exec rspec
```

## Install Plugin

```
gem build logstash-input-burrow.gemspec

logstash-plugin install ~/Developer/logstash-plugins/logstash-input-burrow/logstash-input-burrow-1.0.0.gem 
```