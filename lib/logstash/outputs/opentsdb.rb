# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "socket"
require "logstash/environment"
require "logstash/json"
require "concurrent"
require "stud/buffer"
require "thread" # for safe queueing
require "uri" # for escaping user input

# This output allows you to pull metrics from your logs and ship them to
# opentsdb. Opentsdb is an open source tool for storing and graphing metrics.
#
# This output only speaks the HTTP protocol. HTTP is the preferred protocol for interacting with Opentsdb as of version 2.0
# We strongly encourage the use of HTTP over the telnet protocol for a number of reasons. HTTP is only marginally slower,
# yet far easier to administer and work with. When using the HTTP protocol we can know if a write failed without having
# to query the data back. For those still wishing to use the telnet protocol please see
# the https://github.com/logstash-plugins/logstash-output-opentsdb plugin.
#
# You can learn more about Opentsdb at <http://opentsdb.net/>
#
# ==== Retry Policy
#
# This plugin uses the Opentsdb http multiple put API to optimize its imports into Opentsdb. These requests may experience
# either partial or total failures.
#
# The following errors are retried infinitely:
#
# - Network errors (inability to connect)
# - 429 (Too many requests) and
# - 503 (Service unavailable) errors
#
# ==== DNS Caching
#
# This plugin uses the JVM to lookup DNS entries and is subject to the value of https://docs.oracle.com/javase/7/docs/technotes/guides/net/properties.html[networkaddress.cache.ttl],
# a global setting for the JVM.
#
# As an example, to set your DNS TTL to 1 second you would set
# the `LS_JAVA_OPTS` environment variable to `-Dnetwordaddress.cache.ttl=1`.
#
# Keep in mind that a connection with keepalive enabled will
# not reevaluate its DNS value while the keepalive is in effect.
class LogStash::Outputs::Opentsdb < LogStash::Outputs::Base
  require "logstash/outputs/opentsdb/http_client"
  require "logstash/outputs/opentsdb/http_client_builder"
  require "logstash/outputs/opentsdb/common_configs"
  require "logstash/outputs/opentsdb/common"

  # Protocol agnostic (i.e. non-http, non-java specific) configs go here
  include(LogStash::Outputs::Opentsdb::CommonConfigs)

  # Protocol agnostic methods
  include(LogStash::Outputs::Opentsdb::Common)

  config_name "opentsdb"

  # Username to authenticate to a secure Opentsdb cluster
  config :user, :validate => :string
  # Password to authenticate to a secure Opentsdb cluster
  config :password, :validate => :password

  # HTTP Path at which the Opentsdb server lives. Use this if you must run Opentsdb behind a proxy that remaps
  # the root path for the Opentsdb HTTP API lives.
  config :path, :validate => :string, :default => "/"

  # Enable SSL/TLS secured communication to Opentsdb cluster
  config :ssl, :validate => :boolean, :default => false

  # Option to validate the server's certificate. Disabling this severely compromises security.
  # For more information on disabling certificate verification please read
  # https://www.cs.utexas.edu/~shmat/shmat_ccs12.pdf
  config :ssl_certificate_verification, :validate => :boolean, :default => true

  # The .cer or .pem file to validate the server's certificate
  config :cacert, :validate => :path

  # The JKS truststore to validate the server's certificate.
  # Use either `:truststore` or `:cacert`
  config :truststore, :validate => :path

  # Set the truststore password
  config :truststore_password, :validate => :password

  # The keystore used to present a certificate to the server.
  # It can be either .jks or .p12
  config :keystore, :validate => :path

  # Set the truststore password
  config :keystore_password, :validate => :password

  # Set the address of a forward HTTP proxy.
  # Can be either a string, such as `http://localhost:123` or a hash in the form
  # of `{host: 'proxy.org' port: 80 scheme: 'http'}`.
  # Note, this is NOT a SOCKS proxy, but a plain HTTP proxy
  config :proxy

  # Set the timeout for network operations and requests sent Opentsdb. If
  # a timeout occurs, the request will be retried.
  config :timeout, :validate => :number

  def build_client
    @client = ::LogStash::Outputs::Opentsdb::HttpClientBuilder.build(@logger, @hosts, params)
  end

  def close
    @stopping.make_true
    @buffer.stop
  end

  @@plugins = Gem::Specification.find_all{|spec| spec.name =~ /logstash-output-opentsdb-/ }

  @@plugins.each do |plugin|
    name = plugin.name.split('-')[-1]
    require "logstash/outputs/opentsdb/#{name}"
  end

end # class LogStash::Outputs::Opentsdb
