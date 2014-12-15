require 'redis'
require 'logger'

class RedisHelper
  attr_accessor :logger, :exc_list, :pre_proc, :post_proc, :exc_obj
  attr_reader :config, :handle_exc, :redis
  alias :handle_exc? :handle_exc

#    args = {"redis" => nil,
#            "config" => nil,
#            "logger" => nil,
#            "handle_exc" => true,
#            "exc_list" => [SystemExit],
#            "pre_proc" => nil,
#            "post_proc" => nil,
#            "exc_obj" => nil}
  def initialize(args={})
    args = {"redis" => nil,
            "config" => nil,
            "logger" => nil,
            "handle_exc" => true,
            "exc_list" => [SystemExit],
            "pre_proc" => nil,
            "post_proc" => nil,
            "exc_obj" => nil}.merge(args)
    @handle_exc = args["handle_exc"]
    @exc_list = args["exc_list"]
    @pre_proc = args["pre_proc"]
    @post_proc = args["post_proc"]
    @exc_obj = args["exc_obj"]
    if args["logger"].nil?
        @logger = Logger.new(STDOUT)
    else
        @logger = args["logger"]
    end

    @config = args["config"]

    if args["redis"].nil?
      if !@config.nil?
        @redis = Redis.new(@config)
      else
        @redis = Redis.new()
      end
    else
      @redis = args["redis"]
    end

    if @handle_exc
      alias _get _get_exc
      alias _set _set_exc
    else
      alias _get _get_noexc
      alias _set _set_noexc
    end
  end
 
  def _get_noexc(key, post_proc, exc_msg=nil, exc_obj=nil)
    value = redis[key]
    return (post_proc.nil?)?value:post_proc.call(value), nil
  end

  def _get_exc(key, post_proc, exc_msg, exc_obj)
    begin
      value = redis[key]
      return (post_proc.nil?)?value:post_proc.call(value), nil
    rescue *@exc_list
      raise
    rescue
      if !exc_msg.nil?
        @logger.error("Unable to access(get) redis server. #{exc_msg}!")
      else
        @logger.error("Unable to access(get) redis server!.Key=#{key}")
      end
      @logger.error("#{$!.message}\n#{$!.backtrace.join("\n ")}")

      # return nil,  exception or a
      # user supplied exc_obj.
      # if the exc_obj passed in is nil
      # check to see if an exc_obj was supplied
      # when the instance was created
      # return exc_obj if one was supplied or
      # return the latest ruby exception stored in $!
      return nil, (exc_obj.nil?)?((@exc_obj.nil?)?$!:@exc_obj):exc_obj
    end
  end

  def _set_noexc(key, value, pre_proc, exc_msg=nil, exc_obj=nil)
    pp = pre_proc
    pp = @pre_proc if pp.nil? 
    value = pp.call(value) if !pp.nil?
    @redis[key] = value
    return nil
  end

  def _set_exc(key, value, pre_proc, exc_msg, exc_obj)
    begin
      pp = pre_proc
      pp = @pre_proc if pp.nil? 
      value = pp.call(value) if !pp.nil?
      @redis[key] = value
      return nil
    rescue *@exc_list
      raise
    rescue
      if !exc_msg.nil?
        @logger.error("Unable to access(set) redis server. #{exc_msg}!")
      else
        @logger.error("Unable to access(set) redis server!.Key=#{key}")
      end
      @logger.error("#{$!.message}\n#{$!.backtrace.join("\n ")}")

      # return exception or a
      # user supplied exc_obj.
      # if the exc_obj passed in is nil
      # check to see if an exc_obj was supplied
      # when the instance was created
      # return exc_obj if one was supplied or
      # return the latest ruby exception stored in $!
      return (exc_obj.nil?)?((@exc_obj.nil?)?$!:@exc_obj):exc_obj
    end
  end

  def _doget(k, exc_msg, skip_errors,
             hash, exc_obj, post_proc,
             &code_block)
    v, exc = _get(k, post_proc, exc_msg, exc_obj)
    return exc if (skip_errors and !exc.nil?)
    code_block.call(k, v, exc) if block_given?
    hash[k] = v.nil?exc:v
    return exc
  end

  def _getvalues(key_or_keys=nil, exc_msg=nil,
                 skip_errors=true, exc_obj=nil,
                 post_proc=nil, &code_block)
    if !key_or_keys.nil?
      retval = Hash.new
      keys = [key_or_keys].flatten
      keys.each do |k|
        _doget(k, exc_msg, skip_errors,
               retval_hash, exc_obj,
               post_proc, &code_block)
      end
      retval
    else
      code_block.call(self)
    end
  end

  def _validate_getargs(args, &code_block)
    args = {"key_or_keys" => nil,
            "exc_msg" => nil,
            "skip_errors" => true,
            "exc_obj" => nil,
            "post_proc" => nil}.merge(args)

    if args["key_or_keys"].nil? and !block_given?
      raise ArgumentError, 'key/s and/or code_block is required'
    end
    return args
  end

  def get(args={}, &code_block)
    args = _validate_getargs(args, &code_block)
    retval = _getvalues(args["key_or_keys"], args["exc_msg"],
                        args["skip_errors"], args["exc_obj"],
                        args["post_proc"], &code_block)
    retval = retval.values.first if retval.length == 1
    retval
  end

  def get_pipelined(args={}, &code_block)
    begin
      args = _validate_getargs(args, &code_block)
      retval = @redis.pipelined {
        save_post_proc = @post_proc
        @post_proc = nil
        _getvalues(args["key_or_keys"], args["exc_msg"],
               args["skip_errors"], args["exc_obj"],
               nil, &code_block)
        @post_proc = save_post_proc
      }
      pp = args["post_proc"]
      pp = @post_proc if pp.nil?
      retval.map! {|v|  pp.call(v) if !v.nil?} if !pp.nil?
      retval
    rescue *@exc_list
      raise
    rescue
      if !args["exc_msg"].nil?
        @logger.error("Unable to access(get_pipelined) redis server. #{args["exc_msg"]}!")
      else
        @logger.error("Unable to access(get_pipelined) redis server!")
      end
      @logger.error("#{$!.message}\n#{$!.backtrace.join("\n ")}")
      
      # [exception or a
      # user supplied exc_obj].
      # if the exc_obj passed in is nil
      # check to see if an exc_obj was supplied
      # when the instance was created
      # return exc_obj if one was supplied or
      # return the latest ruby exception stored in $!
      return [(exc_obj.nil?)?((@exc_obj.nil?)?$!:@exc_obj):exc_obj]
    end
  end

  def _doset(k, v,
             exc_msg, skip_errors,
             exc_obj, pre_proc,
             &code_block)
    if v.nil?
      code_block.call(k, v) if block_given?
    else
      exc = _set(k, v, pre_proc, exc_msg, exc_obj)
      return exc if (skip_errors and !exc.nil?)
      code_block.call(k, v, exc) if block_given?
      return exc
    end
  end

  def _setvalues(key_or_keys=nil, value_or_values=nil,
                 kv_dict=nil, exc_msg=nil,
                 skip_errors=true, exc_obj=nil,
                 pre_proc=nil, &code_block)
    if !key_or_keys.nil?
      keys = [key_or_keys].flatten
      values = [value_or_values].flatten
      keys.zip(values).each do |k, v|
        _doset(k, v,
               exc_msg, skip_errors,
               exc_obj, pre_proc, &code_block)
      end
    end
    if !kv_dict.nil?
      kv_dict.each do |k, v|
        _doset(k, v,
               exc_msg, skip_errors,
               exc_obj, pre_proc, &code_block)
      end
    end
    if key_or_keys.nil? and kv_dict.nil?
      code_block.call(self)
    end
  end

  def _validate_setargs(args, &code_block)
    args = {"key_or_keys" => nil,
            "value_or_values" => nil,
            "kv_dict" => nil,
            "exc_msg" => nil,
            "skip_errors" => true,
            "exc_obj" => nil,
            "pre_proc" => nil}.merge(args)

    if args["key_or_keys"].nil? and args["kv_dict"].nil? and !block_given?
      raise ArgumentError, 'key and value, kv_dict,  and/or code_block is required'
    elsif !args["key_or_keys"].nil? and args["value_or_values"].nil?
      raise ArgumentError, 'value cannot be nil if key is not nil'
    end
    return args
  end

#    args = {"key_or_keys" => nil,
#            "value_or_values" => nil,
#            "kv_dict" => nil,
#            "exc_msg" => nil,
#            "skip_errors" => true,
#            "exc_obj" => nil,
#            "pre_proc" => nil}.merge(args)
  def set(args={}, &code_block)
    args = _validate_setargs(args, &code_block)
    _setvalues(args["key_or_keys"], args["value_or_values"],
               args["kv_dict"], args["exc_msg"],
               args["skip_errors"], args["exc_obj"],
               args["pre_proc"], &code_block)
  end

#    args = {"key_or_keys" => nil,
#            "value_or_values" => nil,
#            "kv_dict" => nil,
#            "exc_msg" => nil,
#            "skip_errors" => true,
#            "exc_obj" => nil,
#            "pre_proc" => nil}.merge(args)
  def set_pipelined(args={}, &code_block)
    begin
      args = _validate_setargs(args, &code_block)
      @redis.pipelined {
        _setvalues(args["key_or_keys"], args["value_or_values"],
               args["kv_dict"], args["exc_msg"],
               args["skip_errors"], args["exc_obj"],
               args["pre_proc"], &code_block)
      }
    rescue *@exc_list
      raise
    rescue
      if !args["exc_msg"].nil?
        @logger.error("Unable to access(set_pipelined) redis server. #{args["exc_msg"]}!")
      else
        @logger.error("Unable to access(set_pipelined) redis server!")
      end
      @logger.error("#{$!.message}\n#{$!.backtrace.join("\n ")}")
      
      # exception or a
      # user supplied exc_obj.
      # if the exc_obj passed in is nil
      # check to see if an exc_obj was supplied
      # when the instance was created
      # return exc_obj if one was supplied or
      # return the latest ruby exception stored in $!
      return (exc_obj.nil?)?((@exc_obj.nil?)?$!:@exc_obj):exc_obj
    end
  end

  def [](key)
    v, exc = _get(key, @post_proc, "key=#{key}", @exc_obj)
    return v;
  end

  def []=(key, value)
    exc = _set(key, value, @pre_proc, "key=#{key}", @exc_obj)
    return exc;
  end

  def inspect
    "redis=#{redis=@redis.inspect}, config=#{@config}, "\
    "logger=#{@logger.inspect}, handle_exc=#{@handle_exc}, "\
    "exc_list=#{@exc_list}, pre_proc=#{@pre_proc.inspect}, "\
    "post_proc=#{post_proc.inspect}, exc_obj=#{@exc_obj.inspect}"
  end
  
  def to_s
    inspect
  end

  private :_get_noexc, :_get_exc, :_set_noexc, :_set_exc,
          :_validate_getargs, :_getvalues,
          :_validate_setargs, :_setvalues,
          :_doget, :_doset
end
