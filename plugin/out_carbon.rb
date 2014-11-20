#!/usr/bin/ruby1.9.1
#
# Graphite (Carbon) output
# ========================
#
# fluent.conf
# -----------
#
# <match **>
#   type carbon
#
#   host localhost
#   #port 2003
#   #protocol tcp
#
#   buffer_type file
#   buffer_path ...
#   flush_interval 10s
# </match>
#
# Messages
# --------
#
#   {"metric": "foo.bar.baz", "value": 1024.23}
#
# Example out_xform code
# ----------------------
#
#   def seismometer_to_carbon(time, tag, record)
#     if record.has_key?('v') && record.has_key?('event') && \
#        record['event'].has_key?('vset')
#       metric = "#{record['location']['host']}.#{record['event']['name']}"
#       value = record['event']['vset']['value']['value']
#       yield time, 'metric', { 'metric' => metric, 'value'  => value }
#     end
#   end
#
#-----------------------------------------------------------------------------

require 'socket'

class CarbonOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('carbon', self)

  config_param :host, :string
  config_param :port, :integer, :default => 2003
  config_param :protocol, :string, :default => 'tcp'

  def configure(conf)
    super
    if @host == nil
      raise Fluent::ConfigError, "No Carbon address provided"
    end
  end

  def format(tag, time, record)
    puts "got a record"
    if record.has_key?("metric") && record.has_key?("value")
      puts "## #{record['metric']} #{record['value']} #{time.to_i}"
      return "#{record['metric']} #{record['value']} #{time.to_i}\n"
    else
      puts "## ignored"
      return nil
    end
  end

  def write(chunk)
    puts "writing bunch of records"
    sock = TCPSocket.new @host, @port
    sock.puts chunk.read
    sock.close
  end
end

#-----------------------------------------------------------------------------
# vim:ft=ruby:foldmethod=marker
