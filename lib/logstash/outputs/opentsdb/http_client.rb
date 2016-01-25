require "logstash/outputs/opentsdb"
require "cabin"
require "base64"
require "json"
require "opentsdb"
require "opentsdb/transport/transport/http/manticore"

module LogStash
  module Outputs
    class Opentsdb
      class HttpClient
        attr_reader :client, :options, :client_options, :sniffer_thread
        # This is here in case we use DEFAULT_OPTIONS in the future
        # DEFAULT_OPTIONS = {
        #   :setting => value
        # }

        def initialize(options={})
          @logger = options[:logger]
          # Again, in case we use DEFAULT_OPTIONS in the future, uncomment this.
          # @options = DEFAULT_OPTIONS.merge(options)
          @options = options
          @client = build_client(@options)
          # mutex to prevent requests and sniffing to access the
          # connection pool at the same time
          @request_mutex = Mutex.new
          start_sniffing!
        end

        def bulk(actions)
          @request_mutex.synchronize { non_threadsafe_bulk(actions) }
        end

        def non_threadsafe_bulk(actions)
          return if actions.empty?
          bulk_body = actions.map do |args, source|
            time_format = '%{+%s}'
            time_format += '%{+SSS}' if args[:use_millis]
            {'timestamp' => event.sprintf(time_format),
             'value' => event.sprintf("%{#{args[:value]}}"),
             'metric' => args[:_metric],
             'tags' => args[:_tags]}
          end.to_json

          @client.bulk(:body => bulk_body)
        end

        def start_sniffing!
          if options[:sniffing]
            @sniffer_thread = Thread.new do
              loop do
                @request_mutex.synchronize { sniff! }
                sleep (options[:sniffing_delay].to_f || 30)
              end
            end
          end
        end

        def stop_sniffing!
          @sniffer_thread.kill() if @sniffer_thread
        end

        def sniff!
          client.transport.reload_connections! if options[:sniffing]
          hosts_by_name = client.transport.hosts.map {|h| h["name"]}.sort
          @logger.debug({"count" => hosts_by_name.count, "hosts" => hosts_by_name})
        rescue StandardError => e
          @logger.error("Error while sniffing connection",
                        :message => e.message,
                        :class => e.class.name,
                        :backtrace => e.backtrace)
        end

        private

        # Builds a client and returns an Opentsdb::Client
        #
        # The `options` is a hash where the following symbol keys have meaning:
        #
        # * `:hosts` - array of String. Set a list of hosts to use for communication.
        # * `:port` - number. set the port to use to communicate with Opentsdb
        # * `:user` - String. The user to use for authentication.
        # * `:password` - String. The password to use for authentication.
        # * `:timeout` - Float. A duration value, in seconds, after which a socket
        #    operation or request will be aborted if not yet successfull
        # * `:client_settings` - a hash; see below for keys.
        #
        # The `client_settings` key is a has that can contain other settings:
        #
        # * `:ssl` - Boolean. Enable or disable SSL/TLS.
        # * `:proxy` - String. Choose a HTTP HTTProxy to use.
        # * `:path` - String. The leading path for prefixing Opentsdb
        #   requests. This is sometimes used if you are proxying Opentsdb access
        #   through a special http path, such as using mod_rewrite.
        def build_client(options)
          hosts = options[:hosts] || ["127.0.0.1"]
          client_settings = options[:client_settings] || {}
          timeout = options[:timeout] || 0

          uris = hosts.map do |host|
            proto = client_settings[:ssl] ? "https"  : "http"
            if host =~ /:\d+\z/
              "#{proto}://#{host}#{client_settings[:path]}"
            else
              # Use default port of 4242 if none provided with host.
              "#{proto}://#{host}:4242#{client_settings[:path]}"
            end
          end

          @client_options = {
            :hosts => uris,
            :ssl => client_settings[:ssl],
            :transport_options => {
              :socket_timeout => timeout,
              :request_timeout => timeout,
              :proxy => client_settings[:proxy]
            },
            :transport_class => ::Opentsdb::Transport::Transport::HTTP::Manticore
          }

          if options[:user] && options[:password] then
            token = Base64.strict_encode64(options[:user] + ":" + options[:password])
            @client_options[:headers] = { "Authorization" => "Basic #{token}" }
          end

          @logger.debug? && @logger.debug("Opentsdb HTTP client options", client_options)

          Opentsdb::Client.new(client_options)
        end

      end
    end
  end
end
