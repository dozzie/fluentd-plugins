#!/usr/bin/ruby1.9.1
#
# Transformation plugin with Ruby script
# ======================================
#
# fluent.conf
# -----------
#
# <match **>
#   type xform
#   xform_script /etc/fluent/xform/script.rb
#   #xform_function process  # optional if xform_script has only one function
#
#   # if no <store> defined, all messages are submitted back to the engine
#   <store log.**>
#     type ...
#     ...
#   </store>
#   <store metric.**>
#     type elasticsearch
#     ...
#   </store>
# </match>
#
# Transformation script
# ---------------------
#
# def transform(time, tag, record) # can be named differently
#   new_time, new_tag, new_record = ...
#   yield new_time, new_tag, new_record
# end
#
# `yield' may be called as many times as needed, creating new messages or
# swallowing current altogether.
#
# Note that if the `transform()' raises an exception, the message is lost.
#
#-----------------------------------------------------------------------------

class XForm < Fluent::Output
  Fluent::Plugin.register_output('xform', self)

  config_param :xform_script, :string
  config_param :xform_function, :string, :default => nil
  # also: <store></store> elements

  #unless method_defined?(:log)
  #  define_method(:log) { $log }
  #end

  def initialize
    @xform_function = nil # filename
    @xform_script = nil   # function name
    @xform = nil          # callable object

    @matches = []
  end

  def configure(conf)
    super

    #-------------------------------------------------------
    # load xform script {{{

    # config_param() calls didn't work and I don't know why; they're left as
    # a reference
    @xform_function = conf["xform_function"]
    @xform_script   = conf["xform_script"]

    functions = load_functions(@xform_script)

    if @xform_function
      @xform = functions[@xform_function.to_sym]
    elsif functions.length == 1
      @xform_function, @xform = functions.shift
    else
      if @xform_function != nil
        raise ConfigError, "Function #{@xform_function} not loaded from script #{@xform_script}"
      else
        raise ConfigError, "No function loaded from script #{@xform_script}"
      end
    end

    # }}}
    #-------------------------------------------------------
    # configure <store> elements {{{

    conf.elements.select {|e|
      e.name == 'store'
    }.each {|e|
      type = e['type']
      pattern = e.arg
      if not type
        raise ConfigError, "Missing 'type' parameter on <store #{e.arg}> directive"
      end

      $log.info "adding store", :pattern => pattern, :type => type

      output = Fluent::Plugin.new_output(type)
      output.configure(e)

      match = Fluent::Match.new(pattern, output)
      @matches << match
    }

    if @matches.length == 0
      # XXX: emit rewritten records back to Fluent::Engine if no output
      # provided
      @matches << Fluent::Match.new('**', XFormInternalEmitter.new)
    end

    # }}}
    #-------------------------------------------------------

  end

  def start
    @matches.each {|m| m.start}
  end

  def shutdown
    @matches.each {|m|
      begin
        m.shutdown
      rescue => e
        $log.warn "unexpected error while shutting down",
                  :plugin => m.class, :plugin_id => m.plugin_id,
                  :error_class => e.class, :error => e
        $log.warn_backtrace
      end
    }
  end

  def emit(tag, es, chain)
    es.each {|time, record|
      begin
        @xform.call(time, tag, record) {|new_time, new_tag, new_record|
          send_record_through(new_time, new_tag, new_record)
        }
      rescue => e
        $log.warn "error when processing message",
                  :error_class => e.class, :error => e,
                  :script => @xform_script, :function => @xform_function,
                  :time => time, :tag => tag, :message => record
        $log.warn_backtrace
      end
    }
    chain.next
  end

  #---------------------------------------------------------------------------
  # helpers {{{

  # find a matching output and feed it with the message
  def send_record_through(time, tag, record)
    m = @matches.find {|m| m.match(tag)}
    if m != nil
      m.emit(tag, Fluent::OneEventStream.new(time, record))
    else
      $log.warn "missed xform match", :tag => tag
    end
  end

  # load functions from file
  def load_functions(file)
    context = Object.new
    new_methods = []
    context.define_singleton_method(:singleton_method_added) {|id|
      new_methods << id unless id == :singleton_method_added # skip myself
    }

    context.instance_eval File.read(file)
    return Hash[ new_methods.map{|m| [m, context.method(m)]} ]
  end

  # helper to emit events back to Fluent::Engine
  class XFormInternalEmitter
    def start
      # nothing
    end

    def shutdown
      # nothing
    end

    def emit(tag, es, chain)
      Fluent::Engine.emit_stream(tag, es)
    end
  end

  # }}}
  #---------------------------------------------------------------------------
end

#-----------------------------------------------------------------------------
# vim:ft=ruby:foldmethod=marker
