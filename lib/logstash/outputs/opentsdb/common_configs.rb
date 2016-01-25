module LogStash
  module Outputs
    class Opentsdb
      module CommonConfigs
        def self.included(mod)
          # The metric to write events to. This can be dynamic using the `%{foo}` syntax.
          mod.config :metric, :validate => :string, :required => true

          # The tags to add to the events. String expansion `%{foo}` works here.
          mod.config :tags, :validate => :hash, :required => true

          # The name of the field containing the value to use. This can be dynamic using the `%{foo}` syntax.
          #     `"the_value"`
          #     `"%{get_value_from}"`
          mod.config :value, :validate => :string, :required => true

          # Sets the host(s) of the remote instance. If given an array it will load balance requests across the hosts specified in the `hosts` parameter.
          #     `"127.0.0.1:4242"`
          #     `["127.0.0.1:4242","127.0.0.2:4242"]`
          mod.config :hosts, :validate => :array, :default => ["127.0.0.1:4242"]

          # Whether to use millisecond precision in timestamps written to opentsdb.
          mod.config :use_millis, :validate => bool, :default => false

          # This plugin uses the put API for improved indexing performance.
          # To make efficient API calls, we will buffer a certain number of
          # events before flushing that out to Opentsdb. This setting
          # controls how many events will be buffered before sending a batch
          # of events. Increasing the `flush_size` has an effect on Logstash's heap size.
          # Remember to also increase the heap size using `LS_HEAP_SIZE` if you are sending big documents
          # or have increased the `flush_size` to a higher value.
          mod.config :flush_size, :validate => :number, :default => 500

          # The amount of time since last flush before a flush is forced.
          #
          # This setting helps ensure slow event rates don't get stuck in Logstash.
          # For example, if your `flush_size` is 100, and you have received 10 events,
          # and it has been more than `idle_flush_time` seconds since the last flush,
          # Logstash will flush those 10 events automatically.
          #
          # This helps keep both fast and slow log streams moving along in
          # near-real-time.
          mod.config :idle_flush_time, :validate => :number, :default => 1

          # Set max interval between bulk retries.
          mod.config :retry_max_interval, :validate => :number, :default => 2

        end
      end
    end
  end
end
