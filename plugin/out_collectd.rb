#!/usr/bin/ruby1.9.1
#
# collectd output
# ===============
#
# fluent.conf
# -----------
#
# <match **>
#   type collectd
#
#   socket /var/run/collectd-unixsock
#
#   buffer_type file
#   buffer_path ...
#   flush_interval 10s
# </match>
#
# Messages
# --------
#
#   {"host": "foo.example.net",
#     "plugin": "seismometer", "plugin-instance": null,
#     "type": "memory", "type-instance": null,
#     "interval": null,
#     "values": [...]}
#
#   "host" is mandatory
#   "plugin" is optional and defaults to "seismometer"
#   "plugin-instance" is optional
#   "type" is mandatory
#   "type-instance" is optional
#   "interval" is optional
#   "values" is mandatory and must be an array
#
#-----------------------------------------------------------------------------

require "json"

# TODO
#   * if no data mapping found in types.db, just split the message to separate
#     PUTVAL statements
#   * support for multiple types.db files
#   * detecting interval of various data sent through this instance

class CollectdOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('collectd', self)

  config_param :socket, :string, :default => "/var/run/collectd-unixsock"

  def configure(conf)
    super
  end

  def format(tag, time, record)
    # check for mandatory keys
    return "" if record["host"]   == nil ||
                 record["type"]   == nil ||
                 record["values"] == nil ||
                 (!record["values"].instance_of? Array)
    # PUTVAL %s/%s/%s interval=%d %d:%s
    host   = record["host"]
    plugin = record["plugin"] || "seismometer"
    type   = record["type"]
    values = record["values"].map {|v| v.nil? ? "U" : v.to_s}
    plugin += "-" + record["plugin-instance"] if record["plugin-instance"]
    type   += "-" + record["type-instance"]   if record["type-instance"]
    interval = (record["interval"].nil?) ? nil : record["interval"].to_i

    rec = [time.to_i, "#{host}/#{plugin}/#{type}", interval, values.join(":")]

    return "#{rec.to_json}\n"
  end

  def write(chunk)
    sock = UNIXSocket.new @socket

    chunk.open {|io|
      while not io.eof
        time, path, interval, values = JSON.load io.readline

        if interval != nil
          sock.write "PUTVAL #{path} interval=#{interval} #{time}:#{values}\n"
        else
          sock.write "PUTVAL #{path} #{time}:#{values}\n"
        end
        result  = sock.gets("\n").split(/\s+/, 2)
        code    = result[0].to_i
        message = result[1].strip
        if code < 0
          $log.warn "writing metric failed",
                    :collectd => { :code => code, :message => message },
                    :format  => colld, :message => message
        end
      end
    }

    sock.close
  end
end

# vim:ft=ruby:foldmethod=marker
