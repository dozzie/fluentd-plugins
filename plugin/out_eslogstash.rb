#!/usr/bin/ruby1.9.1
#
# ElasticSearch output plugin
# ===========================
#
# This plugin was written to have no external dependencies and to be fed with
# data from out_xform.
#
# Plugin uses ElasticSearch's bulk interface for writes.
#
# Index for a record is derived from record's timestamp using
# `Time.strftime()', type is taken directly from record's tag.
#
# fluent.conf
# -----------
#
# <match **>
#   type eslogstash
#
#   # default values
#   #url http://localhost:9200/
#   #index_pattern logstash-%Y.%m.%d
#   #tag_strip something
#
#   flush_interval 10s
#   #buffer_type file
#   #buffer_path /var/spool/fluentd/eslogstash.buffer.*
# </match>
#
#-----------------------------------------------------------------------------

require 'net/http'
require 'uri'
require 'json'

#-----------------------------------------------------------------------------

class ElasticSearchLogStash < Fluent::BufferedOutput
  Fluent::Plugin.register_output('eslogstash', self)

  config_param :url, :string, :default => "http://localhost:9200/"
  config_param :index_pattern, :string, :default => "logstash-%Y.%m.%d"
  config_param :tag_strip, :string, :default => nil

  # unneeded
  #def configure(conf)
  #  super
  #end

  # unneeded
  #def start
  #  super
  #end

  # unneeded
  #def shutdown
  #  super
  #end

  def format(tag, time, record)
    json = record.to_json
    if not json.valid_encoding?
      # replace non-UTF characters, if any
      json.encode! "utf-8", "binary", :undef => :replace
    end
    if @tag_strip != nil and tag.start_with? @tag_strip + "."
      tag[0..@tag_strip.length] = "" # this includes dot
    end
    return "#{tag}\t#{time}\t#{json}\n"
  end

  def write(chunk)
    es = ElasticSearchClient.new @url
    chunk.open {|io|
      while not io.eof
        tag, time, record = io.readline.split "\t", 3
        time = Time.at time.to_i
        # this was stored as valid UTF-8, now make it UTF-8 back
        record.force_encoding "utf-8"
        record.gsub!(/\n+$/, "")

        index = time.strftime(@index_pattern)
        es.store(index, tag, record)
      end
    }
    es.close
  end

  #--------------------------------------------------------
  # ElasticSearch client {{{

  class ElasticSearchClient
    class HTTPError < StandardError
      attr_reader :code
      attr_reader :message
      def initialize(code, message)
        @code = code
        @message = message
      end

      def to_s
        return "HTTP #{@code}: #{@message}"
      end
    end

    def initialize(url)
      @url = URI(url)
      @http = Net::HTTP.new @url.host, @url.port
      #@http.open_timeout = 5
      #@http.read_timeout = 5
      @path = @url.path.gsub(%r{/$}, "")

      @buffer = []
      @max_messages = 256
    end

    def close
      flush
    end

    def store(index, type, document)
      operation = {"index" => {"_index" => index, "_type" => type}}
      @buffer << "#{operation.to_json}\n#{document}\n"
      flush if @buffer.length > @max_messages
    end

    def flush
      return if @buffer.empty?
      reply = @http.post "#{@path}/_bulk", @buffer.join("")
      # NOTE: Errno::ECONNREFUSED and Timeout::Error are not caught
      if not reply.is_a? Net::HTTPSuccess
        raise HTTPError.new(reply.code, reply.message)
      end

      # TODO: do something with `reply' and `JSON.load(reply.body)'

      @buffer.clear
    end
  end

  # }}}
  #--------------------------------------------------------

end

#-----------------------------------------------------------------------------
# vim:ft=ruby:foldmethod=marker
