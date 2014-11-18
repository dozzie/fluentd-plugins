#!/usr/bin/ruby1.9.1
#
# ZeroMQ JSON publisher
# ================
#
# fluent.conf
# -----------
#
# <match **>
#   type zmqj
#   bind tcp://127.0.0.1:9000
#   full true  # default: false, just the message body
# </match>
#
# Messages
# --------
#
# When sending full messages, the resulting message is a JSON object with
# three keys:
#   * "tag" (string)
#   * "time" (integer)
#   * "message" (object)
#
#-----------------------------------------------------------------------------

require 'json'
require 'zmq'

class ZeroMQJSON < Fluent::Output
  Fluent::Plugin.register_output('zmqj', self)

  config_param :bind, :string
  config_param :full, :bool, :default => false

  def initialize
    @zmq_context = ZMQ::Context.new(1) # what is 1?
  end

  def start
  end

  def shutdown
    @zmq_out.close
    @zmq_context = nil
  end

  def configure(conf)
    if conf['bind'] == nil
      raise Fluent::ConfigError, "No bind address specified"
    end
    @full = conf['full']

    @zmq_out = @zmq_context.socket(ZMQ::PUB)
    @zmq_out.bind(conf['bind'])
  end

  def emit(tag, es, chain)
    if @full
      es.each {|time, record|
        m = { "tag" => tag, "time" => time.to_i, "message" => record }
        @zmq_out.send(m.to_json)
      }
    else
      es.each {|time, record|
        @zmq_out.send(record.to_json)
      }
    end
  end
end

#-----------------------------------------------------------------------------
# vim:ft=ruby:foldmethod=marker
