require "logstash/outputs/opentsdb/buffer"

module LogStash
  module Outputs
    class Opentsdb
      module Common
        attr_reader :client, :hosts

        RETRYABLE_CODES = [429, 503]
        SUCCESS_CODES = [200, 201]

        def register
          @stopping = Concurrent::AtomicBoolean.new(false)
          setup_hosts # properly sets @hosts
          build_client
          setup_buffer_and_handler

          @logger.info("New Opentsdb output", :class => self.class.name, :hosts => @hosts)
        end

        def receive(event)
          @buffer << [event_action_params(event),event]
        end

        # Receive an array of events and immediately attempt to index them (no buffering)
        def multi_receive(events)
          retrying_submit(events.map{|event|[event_action_params(event),event]})
        end

        def flush
          @buffer.flush
        end

        def setup_hosts
          @hosts = Array(@hosts)
          if @hosts.empty?
            @logger.info("No 'host' set in opentsdb output. Defaulting to localhost")
            @hosts.replace(["localhost"])
          end
        end

        def setup_buffer_and_handler
          @buffer = ::LogStash::Outputs::Opentsdb::Buffer.new(@logger, @flush_size, @idle_flush_time) do |actions|
            retrying_submit(actions)
          end
        end

        def retrying_submit(actions)
          # Initially we submit the full list of actions
          submit_actions = actions

          while submit_actions && submit_actions.length > 0
            begin
              submit_actions = submit(submit_actions)
            rescue => e
              @logger.warn("Encountered an unexpected error submitting a bulk request! Will retry.",
                           :message => e.message,
                           :class => e.class.name,
                           :backtrace => e.backtrace)
            end

            sleep @retry_max_interval if submit_actions && submit_actions.length > 0
          end
        end

        def submit(actions)
          opentsdb_actions = actions.map { |val, params| [val, params.to_hash]}

          bulk_response = safe_bulk(opentsdb_actions,actions)

          # If there are no errors, we're done here!
          return unless bulk_response["errors"]

          actions_to_retry = []
          bulk_response["items"].each_with_index do |response,idx|
            status = response['status']
            if SUCCESS_CODES.include?(status)
              next
            elsif RETRYABLE_CODES.include?(status)
              @logger.warn "retrying failed action with response code: #{status}"
              actions_to_retry << action
            else
              @logger.warn "Failed action. ", status: status, action: action, response: response
            end
          end

          actions_to_retry
        end

        # get the action parameters for the given event
        def event_action_params(event)
          {
            :_metric => event.sprintf(@metric),
            :_tags => @tags.map{|k,v|{event.sprintf(k)=>event.sprintf(v)}}.reduce({},&:merge),
            :_use_millis => @use_millis,
            :_value => event.sprintf(@value)
          }
        end

        # Rescue retryable errors during bulk submission
        def safe_bulk(opentsdb_actions,actions)
          @client.bulk(opentsdb_actions)
        rescue Manticore::SocketException, Manticore::SocketTimeout => e
          # If we can't even connect to the server let's just print out the URL (:hosts is actually a URL)
          # and let the user sort it out from there
          @logger.error(
            "Attempted to send a bulk request to Opentsdb configured at '#{@client.client_options[:hosts]}',"+
            " but Opentsdb appears to be unreachable or down!",
            :client_config => @client.client_options,
            :error_message => e.message,
            :class => e.class.name
          )
          @logger.debug("Failed actions for last bad bulk request!", :actions => actions)

          # We retry until there are no errors! Errors should all go to the retry queue
          sleep @retry_max_interval
          retry unless @stopping.true?
        rescue => e
          # For all other errors print out full connection issues
          @logger.error(
            "Attempted to send a bulk request to Opentsdb configured at '#{@client.client_options[:hosts]}'," +
            " but an error occurred and it failed! Are you sure you can reach opentsdb from this machine using " +
            "the configuration provided?",
            :client_config => @client.client_options,
            :error_message => e.message,
            :error_class => e.class.name,
            :backtrace => e.backtrace
          )

          @logger.debug("Failed actions for last bad bulk request!", :actions => actions)

          raise e
        end
      end
    end
  end
end
