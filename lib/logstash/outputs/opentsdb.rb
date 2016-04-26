# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "logstash/json"
require "stud/buffer"

# This output lets you output Metrics to Opentsdb
#
# The configuration here attempts to be as friendly as possible
# and minimize the need for multiple definitions to write to
# multiple metrics and still be efficient
#
# You can learn more at http://opentsdb.net[Opentsdb homepage]
class LogStash::Outputs::Opentsdb < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "opentsdb"

  # The hostname or IP address to reach your Opentsdb instance
  config :host, :validate => :string, :required => true

  # The port for Opentsdb
  config :port, :validate => :number, :default => 4242

  # Http basic auth user
  config :user, :validate => :string, :default => nil

  # Http basic auth password
  config :password, :validate => :password, :default => nil

  # Enable SSL/TLS secured communication to Opentsdb
  config :ssl, :validate => :boolean, :default => false

  # Metric name - supports sprintf formatting
  config :metric, :validate => :string, :required => true

  # The name of the field containing the value to use. This can be dynamic using the `%{foo}` syntax.
  #     `"the_value"`
  #     `"%{get_value_from}"`
  config :value, :validate => :string, :required => true

  # Allow the override of the timestamp column in the event?
  #
  # By default the time will be determined by the value of `@timestamp`.
  #
  # Setting this to `true` allows you to explicitly set the timestamp column yourself
  #
  # Note: **`time` must be an epoch value in either seconds, milliseconds or microseconds**
  config :allow_time_override, :validate => :boolean, :default => false

  # Which field to use as the time for the event
  config :time_override_field, :validate => :string, :default => "time"

  # Set the level of precision of `timestamp`
  config :use_millis, :validate => :boolean, :default => false

  # Allow value coercion
  #
  # this will attempt to convert data point value to the appropriate type before posting
  # otherwise sprintf-filtered numeric values could get sent as strings
  #
  # currently supported datatypes are `integer` and `float`
  #
  config :coerce_value, :validate => :string, :default => "float"

  # A hash containing the names and values of fields to send to Opentsdb as tags.
  config :tags, :validate => :hash

  # This setting controls how many events will be buffered before sending a batch
  # of events. Note that these are only batched for the same measurement
  config :flush_size, :validate => :number, :default => 100

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 1

  # How long to wait after a failure to retry, 0 is no-wait but discouraged
  config :backoff, :validate => :number, :default => 5

  # Number of retries to perform
  config :retries, :validate => :number, :default => 0

  public
  def register
    require 'manticore'
    require 'cgi'

    @client = Manticore::Client.new(retry_non_idempotent: true, stale_check: true)

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
  end # def register

  public
  def receive(event)

    @logger.debug? and @logger.debug("Opentsdb output: Received event: #{event}")

    # An Opentsdb event looks like this:
    # {
    #     "metric": "sys.cpu.nice",
    #     "timestamp": 1346846400,
    #     "value": 18,
    #     "tags": {
    #        "host": "web01",
    #        "dc": "lga"
    #     }
    # }
    #
    # Since we'll be buffering them to send as a batch, we'll only collect
    # the values going into the points array

    if (@allow_time_override && event.has_key?(@time_override_field))
      time = event[@time_override_field]
    else
      time  = timestamp_at_precision(event, @use_millis)
      logger.error("Cannot override value of time without 'allow_time_override'. Using event timestamp") if @allow_time_override
    end

    tags = extract_tags(event)

    event_hash = {
      "metric"    => event.sprintf(@metric),
      "timestamp" => time.to_i,
      "value"     => coerce_value(event[event.sprintf(@value)])
    }
    event_hash["tags"] = tags unless tags.empty?

    buffer_receive(event_hash)
  end # def receive

  # A batch POST for Opentsdb looks like this:
  # [
  # {
  #   "metric": "sys.cpu.nice",
  #   "timestamp": 1346846400,
  #   "value": 18,
  #   "tags": {
  #            "host": "web01",
  #            "dc": "lga"
  #           }
  # },
  # {
  #   "metric": "sys.cpu.nice",
  #   "timestamp": 1346846400,
  #   "value": 9,
  #   "tags": {
  #            "host": "web02",
  #            "dc": "lga"
  #           }
  # }
  # ]
  def flush(events, teardown = false)
    @logger.debug? and @logger.debug("Flushing #{events.size} events - Teardown? #{teardown}")
    try = 0
    until post(LogStash::Json.dump(events))
      break if (@retries <= 0 or try >= @retries)
      sleep(@backoff) if @backoff > 0
      try += 1
    end
  end # def flush

  def post(body)
    begin
      query_params = "details"
      protocol = @ssl ? "https" : "http"
      base_url = "#{protocol}://#{@host}:#{@port}/api/put"
      url = "#{base_url}?#{query_params}"

      @logger.debug? and @logger.debug("POSTing to #{url}")
      @logger.debug? and @logger.debug("Post body: #{body}")
      req = {:body => body}
      req[:auth] = {:user => @user, :password => @password} unless (@user.nil? or @password.nil?)
      response = @client.post!(url, req)

    rescue EOFError
      @logger.warn("EOF while writing request or reading response header from Opentsdb",
                   :host => @host, :port => @port)
      return false # abort this flush
    rescue Manticore::ManticoreException
      return false
    end

    if read_body?(response)
      # Consume the body for error checking
      # This will also free up the connection for reuse.
      body = ""
      begin
        response.read_body { |chunk| body += chunk }
      rescue EOFError
        @logger.warn("EOF while reading response body from Opentsdb",
                     :host => @host, :port => @port)
        return false # abort this flush
      end

      @logger.debug? and @logger.debug("Body: #{body}")
    end

    unless response && (200..299).include?(response.code)
      @logger.error("Error writing to Opentsdb",
                    :response => response, :response_body => body,
                    :request_body => body)
      return false
    else
      @logger.debug? and @logger.debug("Post response: #{response}")
      return true
    end
  end # def post

  def close
    buffer_flush(:final => true)
  end # def teardown

  # Coerce value in the event data to the appropriate type. This requires
  # foreknowledge of what's in the data point, which is less than ideal.
  def coerce_value(value)
    case @coerce_value.to_sym
    when :integer
      value.to_i

    when :float
      value.to_f

    when :string
      value.to_s

    else
      @logger.warn("Don't know how to convert to #{value_type}. Returning value unchanged")
      value
    end
  end

  def extract_tags(event)
    tags = {}
    @tags.map{|k,v|
      tags[event.sprintf(k)] = event.sprintf(v)
    }
    tags
  end

  # Returns the numeric value of the given timestamp in the requested precision.
  # precision must be one of the valid values for time_precision
  def timestamp_at_precision( event, use_millis )
    time_format = '%{+%s}'
    time_format += '%{+SSS}' if use_millis
    event.sprintf(time_format)
  end

  # Only read the response body if its status is not 1xx, 204, or 304. TODO: Should
  # also not try reading the body if the request was a HEAD
  def read_body?( response )
    ! (!response.nil? || [204,304].include?(response.code) || (100..199).include?(response.code))
  end

end # class LogStash::Outputs::Opentsdb
