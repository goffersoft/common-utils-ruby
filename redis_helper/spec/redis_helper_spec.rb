require 'spec_helper'

describe RedisHelper do
  describe "RedisHelper class structure" do
    before :all do
      @redis_helper = RedisHelper.new
    end

    it "the following methods should be private -" \
       ":_get, :_get_noexc, :_get_exc, "\
       ":_set, :_set_noexc, :_set_exc, " \
       ":_validate_getargs, :_getvalues, " \
       ":_validate_setargs, :_setvalues, " \
       ":_doget, :_doset" do
      expect(@redis_helper.respond_to? :_get, true).to be true
      expect(@redis_helper.methods.include? :_get).to be false

      expect(@redis_helper.respond_to? :_get_noexc, true).to be true
      expect(@redis_helper.methods.include? :_get_noexec).to be false

      expect(@redis_helper.respond_to? :_get_exc, true).to be true
      expect(@redis_helper.methods.include? :_get_exc).to be false

      expect(@redis_helper.respond_to? :_doget, true).to be true
      expect(@redis_helper.methods.include? :_doget).to be false

      expect(@redis_helper.respond_to? :_validate_getargs, true).to be true
      expect(@redis_helper.methods.include? :_validate_getargs).to be false

      expect(@redis_helper.respond_to? :_getvalues, true).to be true
      expect(@redis_helper.methods.include? :_getvalues).to be false

      expect(@redis_helper.respond_to? :_set, true).to be true
      expect(@redis_helper.methods.include? :_set).to be false

      expect(@redis_helper.respond_to? :_set_noexc, true).to be true
      expect(@redis_helper.methods.include? :_set_noexec).to be false

      expect(@redis_helper.respond_to? :_set_exc, true).to be true
      expect(@redis_helper.methods.include? :_set_exc).to be false

      expect(@redis_helper.respond_to? :_doset, true).to be true
      expect(@redis_helper.methods.include? :_doset).to be false

      expect(@redis_helper.respond_to? :_validate_setargs, true).to be true
      expect(@redis_helper.methods.include? :_validate_setargs).to be false

      expect(@redis_helper.respond_to? :_setvalues, true).to be true
      expect(@redis_helper.methods.include? :_setvalues).to be false
    end

    it "the following methods should be public -" \
       ":get, :get_pipelined, :set, :set_pipelined, " \
       ":[], :[]=, :logger, :logger=, :exc_list, :exc_list=, "\
       ":pre_proc, :pre_proc=, :post_proc, :post_proc= "\
       ":exc_obj, exc_obj=, :handle_exc, :handle_exc? "\
       ":config, :redis" do
      expect(@redis_helper.methods.include? :get).to be true
      expect(@redis_helper.methods.include? :get_pipelined).to be true

      expect(@redis_helper.methods.include? :set).to be true
      expect(@redis_helper.methods.include? :set_pipelined).to be true

      expect(@redis_helper.methods.include? :[]).to be true
      expect(@redis_helper.methods.include? :[]=).to be true

      expect(@redis_helper.methods.include? :logger).to be true
      expect(@redis_helper.methods.include? :logger=).to be true

      expect(@redis_helper.methods.include? :exc_list).to be true
      expect(@redis_helper.methods.include? :exc_list=).to be true

      expect(@redis_helper.methods.include? :pre_proc).to be true
      expect(@redis_helper.methods.include? :pre_proc=).to be true

      expect(@redis_helper.methods.include? :post_proc).to be true
      expect(@redis_helper.methods.include? :post_proc=).to be true

      expect(@redis_helper.methods.include? :exc_obj).to be true
      expect(@redis_helper.methods.include? :exc_obj=).to be true

      expect(@redis_helper.methods.include? :config).to be true
      expect(@redis_helper.respond_to? :config=, true).to be false
      expect(@redis_helper.methods.include? :config=).to be false

      expect(@redis_helper.methods.include? :handle_exc).to be true
      expect(@redis_helper.methods.include? :handle_exc?).to be true
      expect(@redis_helper.respond_to? :handle_exc=, true).to be false
      expect(@redis_helper.methods.include? :handle_exc=).to be false

      expect(@redis_helper.methods.include? :redis).to be true
      expect(@redis_helper.respond_to? :redis=, true).to be false
      expect(@redis_helper.methods.include? :redis=).to be false
    end
  end

  describe "#new" do
    before :all do
      @logger = Logger.new(STDOUT)
      @config = { :host => "127.0.0.1", :port => 6379 }
      @redis_regexp = /(127.0.0.1.*6379|6379.*127.0.0.1)/
      @def_exc_list = [SystemExit]
      @redis = Redis.new
      @pre_proc = "pre proc block for redis sets"
      @post_proc = "post proc block for redis gets"
      @exc_obj = "object returned incase of an exception"
      @exc_list = "list of exceptions"
      @all_args = {"redis" => @redis,
                   "config" => @config,
                   "logger" => @logger,
                   "handle_exc" => true,
                   "exc_list" => @exc_list,
                   "pre_proc" => @pre_proc,
                   "post_proc" => @post_proc,
                   "exc_obj" => @exc_obj}
    end

    it "takes no parameters and returns a RedisHelper object" do
      @redis_helper = RedisHelper.new
      expect(@redis_helper).to be_an_instance_of RedisHelper
      expect(@redis_helper.redis).to be_an_instance_of Redis
      expect(@redis_helper.config).to be nil
      expect(@redis_helper.logger).to be_an_instance_of Logger
      expect(@redis_helper.handle_exc).to be true
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_noexc)).to be false
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_noexc)).to be false
      expect(@redis_helper.pre_proc).to be nil
      expect(@redis_helper.post_proc).to be nil
      expect(@redis_helper.exc_list).to eq @def_exc_list
    end

    it "takes 1 parameter - handle_exc=false and returns a RedisHelper object" do
      @redis_helper = RedisHelper.new({"handle_exc" => false})
      expect(@redis_helper).to be_an_instance_of RedisHelper
      expect(@redis_helper.redis).to be_an_instance_of Redis
      expect(@redis_helper.config).to be nil
      expect(@redis_helper.logger).to be_an_instance_of Logger
      expect(@redis_helper.handle_exc).to be false
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_exc)).to be false
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_exc)).to be false
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_noexc)).to be true
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_noexc)).to be true
      expect(@redis_helper.exc_obj).to be nil
      expect(@redis_helper.pre_proc).to be nil
      expect(@redis_helper.post_proc).to be nil
      expect(@redis_helper.exc_list).to eq @def_exc_list
    end

    it "takes 5 parameters - pre_proc, post_proc, exc_obj, exc_list, logger "\
       "and returns a RedisHelper object" do
      @redis_helper = RedisHelper.new({"pre_proc" => @pre_proc,
                                       "post_proc" => @post_proc,
                                       "exc_obj" => @exc_obj,
                                       "exc_list" => @exc_list,
                                       "logger" => @logger})
      expect(@redis_helper).to be_an_instance_of RedisHelper
      expect(@redis_helper.redis).to be_an_instance_of Redis
      expect(@redis_helper.config).to be nil
      expect(@redis_helper.logger).to eq @logger
      expect(@redis_helper.handle_exc).to be true
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_noexc)).to be false
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_noexc)).to be false
      expect(@redis_helper.exc_obj).to eq @exc_obj
      expect(@redis_helper.pre_proc).to eq @pre_proc
      expect(@redis_helper.post_proc).to eq @post_proc
      expect(@redis_helper.exc_list).to eq @exc_list
    end

    it "takes 1 parameter - config and returns a RedisHelper object"\
       "and returns a RedisHelper object" do
      @redis_helper = RedisHelper.new({"config" => @config})
      expect(@redis_helper).to be_an_instance_of RedisHelper
      expect(@redis_helper.redis).to be_an_instance_of Redis
      expect(@redis_helper.redis.inspect).to match(@redis_regexp)
      expect(@redis_helper.config).to eq @config
      expect(@redis_helper.logger).to be_an_instance_of Logger
      expect(@redis_helper.handle_exc).to be true
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_noexc)).to be false
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_noexc)).to be false
      expect(@redis_helper.exc_obj).to be nil 
      expect(@redis_helper.pre_proc).to  be nil
      expect(@redis_helper.post_proc).to be nil 
      expect(@redis_helper.exc_list).to eq @def_exc_list
    end

    it "takes 2 parameters - redis and config and returns a RedisHelper object" do
      @config1 = { :host => "10.10.10.10", :port => 9999 }
      @redis_helper = RedisHelper.new({"redis" => @redis, "config" => @config1})
      expect(@redis_helper).to be_an_instance_of RedisHelper
      expect(@redis_helper.redis).to eq @redis
      expect(@redis_helper.redis.inspect).to match(@redis_regexp)
      expect(@redis_helper.config).to eq @config1
      expect(@redis_helper.logger).to be_an_instance_of Logger
      expect(@redis_helper.handle_exc).to be true
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_noexc)).to be false
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_noexc)).to be false
      expect(@redis_helper.exc_obj).to be nil 
      expect(@redis_helper.pre_proc).to  be nil
      expect(@redis_helper.post_proc).to be nil 
      expect(@redis_helper.exc_list).to eq @def_exc_list
    end

    it "takes 8 parameters - pre_proc, post_proc, exc_obj, exc_list, logger "\
       "handle_exc, config, redis and returns a RedisHelper object" do
      @redis_helper = RedisHelper.new(@all_args)
      expect(@redis_helper).to be_an_instance_of RedisHelper
      expect(@redis_helper.redis).to eq @redis
      expect(@redis_helper.redis.inspect).to match(@redis_regexp)
      expect(@redis_helper.config).to eq @config
      expect(@redis_helper.logger).to eq @logger
      expect(@redis_helper.handle_exc).to be true
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_exc)).to be true
      expect(@redis_helper.method(:_set) ==
                    @redis_helper.method(:_set_noexc)).to be false
      expect(@redis_helper.method(:_get) ==
                    @redis_helper.method(:_get_noexc)).to be false
      expect(@redis_helper.exc_obj).to eq @exc_obj
      expect(@redis_helper.pre_proc).to eq @pre_proc 
      expect(@redis_helper.post_proc).to eq @post_proc
      expect(@redis_helper.exc_list).to eq @exc_list
    end
  end

  describe "#get and []", :require_redis_server => true do
    before :each do
      @redis_helper = RedisHelper.new()
      @redis_helper.redis["test_key"] = " "
      @redis_helper.redis["test_key1"] = " "
      @redis_helper.redis["test_key2"] = " "
      @list_of_keys = ["test_key", "test_key1", "test_key2"]
      @post_proc = Proc.new { |v| v + " post_proc" }
      @post_proc1 = Proc.new { |v| v + " post_proc1" }
      @redis_helper1 = RedisHelper.new({"post_proc" => @post_proc1})
      @expected_ret_val = {"test_key" => "test_value",
                           "test_key1" => "test_value1",
                           "test_key2" => "test_value2"}
    end

    it "gets data from the redis cache using []" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper["test_key"]).to eq "test_value"
    end

    it "gets data from the redis cache using "\
       "[] - RedisHelper.new({""post_proc"" => @post_proc})" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper1["test_key"]).to eq \
                          ("test_value" + " post_proc1")
    end

    it "gets data from the redis cache using get - one key" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper.get(
                  {"key_or_keys" => "test_key" })).to eq "test_value"
    end

    it "gets data from the redis cache using get - "\
       "one key / post_proc" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper.get(
                  {"key_or_keys" => "test_key", \
                   "post_proc" => @post_proc})).to eq \
                                  ("test_value" + " post_proc")
    end
    
    it "gets data from the redis cache using get - "\
       "one key - RedisHelper.new({""post_proc"" => @post_proc})" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper1.get(
                  {"key_or_keys" => "test_key"})).to eq \
                                  ("test_value" + " post_proc1")
    end
    
    it "gets data from the redis cache using get - "\
       "one key/post_proc - RedisHelper.new({""post_proc"" => @post_proc})" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper1.get(
                  {"key_or_keys" => "test_key", \
                   "post_proc" => @post_proc})).to eq \
                                  ("test_value" + " post_proc")
    end

    it "gets data from the redis cache using get - list of keys" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper.get(
                  {"key_or_keys" => @list_of_keys})).to eq \
                          @expected_ret_val
    end

    it "gets data from the redis cache using get "\
       "- list of keys / post_proc" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper.get(
                  {"key_or_keys" => @list_of_keys,\
                   "post_proc" => @post_proc})).to eq \
                          @expected_ret_val.inject({}) \
                            { |new_hash, (k,v)| new_hash[k] = \
                                 v + " post_proc"; new_hash}
    end
    
    it "gets data from the redis cache using get "\
       "- list of keys / RedisHelper.new({""post_proc"" => @post_proc1})" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper1.get(
                  {"key_or_keys" => @list_of_keys})).to eq \
                          @expected_ret_val.inject({}) \
                            { |new_hash, (k,v)| new_hash[k] = \
                                 v + " post_proc1"; new_hash}
    end
    
    it "gets data from the redis cache using get "\
       "- list of keys / post_proc / RedisHelper.new({""post_proc"" => @post_proc1})" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper1.get(
                  {"key_or_keys" => @list_of_keys,\
                   "post_proc" => @post_proc})).to eq \
                          @expected_ret_val.inject({}) \
                            { |new_hash, (k,v)| new_hash[k] = \
                                 v + " post_proc"; new_hash}
    end
  end
  
  describe "#get_pipelined", :require_redis_server => true do
    before :each do
      @redis_helper = RedisHelper.new()
      @redis_helper.redis["test_key"] = " "
      @redis_helper.redis["test_key1"] = " "
      @redis_helper.redis["test_key2"] = " "
      @list_of_keys = ["test_key", "test_key1", "test_key2"]
      @post_proc = Proc.new { |v| v + " post_proc" }
      @post_proc1 = Proc.new { |v| v + " post_proc1" }
      @redis_helper1 = RedisHelper.new({"post_proc" => @post_proc1})
      @expected_ret_val = {"test_key" => "test_value",
                           "test_key1" => "test_value1",
                           "test_key2" => "test_value2"}
    end

    it "gets data from the redis cache using get - one key" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper.get_pipelined(
                  {"key_or_keys" => "test_key" })).to eq ["test_value"]
    end

    it "gets data from the redis cache using get - "\
       "one key / post_proc" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper.get_pipelined(
                  {"key_or_keys" => "test_key", \
                   "post_proc" => @post_proc})).to eq \
                                  ["test_value" + " post_proc"]
    end
    
    it "gets data from the redis cache using get - "\
       "one key - RedisHelper.new({""post_proc"" => @post_proc})" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper1.get_pipelined(
                  {"key_or_keys" => "test_key"})).to eq \
                                  ["test_value" + " post_proc1"]
    end
    
    it "gets data from the redis cache using get - "\
       "one key/post_proc - RedisHelper.new({""post_proc"" => @post_proc})" do
      @redis_helper.redis["test_key"] = "test_value"
      expect(@redis_helper1.get_pipelined(
                  {"key_or_keys" => "test_key", \
                   "post_proc" => @post_proc})).to eq \
                                  ["test_value" + " post_proc"]
    end

    it "gets data from the redis cache using get - list of keys" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper.get_pipelined(
                  {"key_or_keys" => @list_of_keys})).to eq \
                          @expected_ret_val.values()
    end

    it "gets data from the redis cache using get "\
       "- list of keys / post_proc" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper.get_pipelined(
                  {"key_or_keys" => @list_of_keys,\
                   "post_proc" => @post_proc})).to eq \
                          (@expected_ret_val.inject({}) \
                            { |new_hash, (k,v)| new_hash[k] = \
                                 v + " post_proc"; new_hash}).values()
    end

    it "gets data from the redis cache using get "\
       "- list of keys / RedisHelper.new({""post_proc"" => @post_proc1})" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper1.get_pipelined(
                  {"key_or_keys" => @list_of_keys})).to eq \
                          (@expected_ret_val.inject({}) \
                            { |new_hash, (k,v)| new_hash[k] = \
                                 v + " post_proc1"; new_hash}).values()
    end

    it "gets data from the redis cache using get "\
       "- list of keys / post_proc / RedisHelper.new({""post_proc"" => @post_proc1})" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper1.get_pipelined(
                  {"key_or_keys" => @list_of_keys,\
                   "post_proc" => @post_proc})).to eq \
                          (@expected_ret_val.inject({}) \
                            { |new_hash, (k,v)| new_hash[k] = \
                                 v + " post_proc"; new_hash}).values()
    end

    it "gets data from the redis cache using code_block" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper.get_pipelined{ |r|
                 @list_of_keys.each do |v|
                   r[v]
                 end
             }).to eq @expected_ret_val.values
    end
    
    it "gets data from the redis cache using code_block / post_proc" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper.get_pipelined({"post_proc" => @post_proc}) { |r|
                 @list_of_keys.each do |v|
                   r[v]
                 end
             }).to eq (@expected_ret_val.inject({}) \
                        { |new_hash, (k,v)| new_hash[k] = \
                             v + " post_proc"; new_hash}).values()
    end

    it "gets data from the redis cache using code_block "\
       "/ RedisHelper.new({""post_proc"" => @post_proc1})" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper1.get_pipelined() { |r|
                 @list_of_keys.each do |v|
                   r[v]
                 end
             }).to eq (@expected_ret_val.inject({}) \
                        { |new_hash, (k,v)| new_hash[k] = \
                             v + " post_proc1"; new_hash}).values()
    end

    it "gets data from the redis cache using code_block / post_proc"\
       "/ RedisHelper.new({""post_proc"" => @post_proc1})" do
      @list_of_keys.each do |k|
        @redis_helper.redis[k] = @expected_ret_val[k] 
      end
      expect(@redis_helper.get_pipelined({"post_proc" => @post_proc}) { |r|
                 @list_of_keys.each do |v|
                   r[v]
                 end
             }).to eq (@expected_ret_val.inject({}) \
                        { |new_hash, (k,v)| new_hash[k] = \
                             v + " post_proc"; new_hash}).values()
    end
  end

  describe "#set and []=", :require_redis_server => true do
    before :each do
      @redis_helper = RedisHelper.new()
      @kv_set = {"test_key" => "redis_set_test_value",
                 "test_key1" => "redis_set_test_value1",
                 "test_key2" => "redis_set_test_value2"}
      @pre_proc = Proc.new { |v| v + " pre_proc" }
      @pre_proc1 = Proc.new { |v| v + " pre_proc1" }
      @redis_helper = RedisHelper.new()
      @redis_helper1 = RedisHelper.new({"pre_proc" => @pre_proc1})
      @kv_set.each do |k,v| @redis_helper.redis[k] = " " end
    end

    it "sets data in the redis cache using []=" do
      @redis_helper["test_key"] = "test_redis_helper_set"
      expect(@redis_helper["test_key"]).to eq "test_redis_helper_set"
    end

    it "sets data in the redis cache using "\
       "[]= - RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1["test_key"] = "test_redis_helper_set"
      expect(@redis_helper["test_key"]).to eq \
                        "test_redis_helper_set" + " pre_proc1"
    end

    it "sets data in the redis cache using set - one key/values" do
      @redis_helper.set({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1"})
      expect(@redis_helper["test_key"]).to eq "test_redis_helper_set1"
    end

    it "sets data in the redis cache using set - "\
       "one key/value and pre_proc" do
      @redis_helper.set({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1",
                         "pre_proc" => @pre_proc})
      expect(@redis_helper["test_key"]).to eq \
                         "test_redis_helper_set1" + " pre_proc"
    end

    it "sets data in the redis cache using set - "\
       "one key/value and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1"})
      expect(@redis_helper["test_key"]).to eq \
                         "test_redis_helper_set1" + " pre_proc1"
    end
    
    it "sets data in the redis cache using set - "\
       "one key/value/pre_proc and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1",
                         "pre_proc" => @pre_proc})
      expect(@redis_helper["test_key"]).to eq \
                         "test_redis_helper_set1" + " pre_proc"
    end

    it "sets data in the redis cache using set - multiple key/values "\
       "- kv_dict" do
      @redis_helper.set({"kv_dict" => @kv_set})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq @kv_set
    end

    it "sets data in the redis cache using set - multiple key/values "\
       "- kv_dict and pre_proc" do
      @redis_helper.set({"kv_dict" => @kv_set,
                         "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end
    
    it "sets data in the redis cache using set - multiple key/values "\
       "kv_dict and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set({"kv_dict" => @kv_set})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc1"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values "\
       "kv_dict / proc and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set({"kv_dict" => @kv_set,
                          "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values" do
      @redis_helper.set({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq @kv_set
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values and pre_proc" do
      @redis_helper.set({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values,
                         "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values and "\
       "RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc1"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values/pre_proc and "\
       "RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values,
                         "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end
  end

  describe "#set_pipelined", :require_redis_server => true do
    before :each do
      @redis_helper = RedisHelper.new()
      @kv_set = {"test_key" => "redis_set_test_value",
                 "test_key1" => "redis_set_test_value1",
                 "test_key2" => "redis_set_test_value2"}
      @pre_proc = Proc.new { |v| v + " pre_proc" }
      @pre_proc1 = Proc.new { |v| v + " pre_proc1" }
      @redis_helper = RedisHelper.new()
      @redis_helper1 = RedisHelper.new({"pre_proc" => @pre_proc1})
      @kv_set.each do |k,v| @redis_helper.redis[k] = " " end
    end

    it "sets data in the redis cache using set - one key/values" do
      @redis_helper.set_pipelined({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1"})
      expect(@redis_helper["test_key"]).to eq "test_redis_helper_set1"
    end

    it "sets data in the redis cache using set - "\
       "one key/value and pre_proc" do
      @redis_helper.set_pipelined({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1",
                         "pre_proc" => @pre_proc})
      expect(@redis_helper["test_key"]).to eq \
                         "test_redis_helper_set1" + " pre_proc"
    end

    it "sets data in the redis cache using set - "\
       "one key/value and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1"})
      expect(@redis_helper["test_key"]).to eq \
                         "test_redis_helper_set1" + " pre_proc1"
    end
    
    it "sets data in the redis cache using set - "\
       "one key/value/pre_proc and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined({"key_or_keys" => "test_key",
                         "value_or_values" => "test_redis_helper_set1",
                         "pre_proc" => @pre_proc})
      expect(@redis_helper["test_key"]).to eq \
                         "test_redis_helper_set1" + " pre_proc"
    end

    it "sets data in the redis cache using set - multiple key/values "\
       "- kv_dict" do
      @redis_helper.set_pipelined({"kv_dict" => @kv_set})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq @kv_set
    end

    it "sets data in the redis cache using set - multiple key/values "\
       "- kv_dict and pre_proc" do
      @redis_helper.set_pipelined({"kv_dict" => @kv_set,
                         "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end
    
    it "sets data in the redis cache using set - multiple key/values "\
       "kv_dict and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined({"kv_dict" => @kv_set})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc1"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values "\
       "kv_dict / proc and RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined({"kv_dict" => @kv_set,
                          "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values" do
      @redis_helper.set_pipelined({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq @kv_set
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values and pre_proc" do
      @redis_helper.set_pipelined({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values,
                         "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values and "\
       "RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc1"; new_hash}
    end

    it "sets data in the redis cache using set - multiple key/values - "\
       "key_or_keys/value_or_values/pre_proc and "\
       "RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined({"key_or_keys" => @kv_set.keys,
                         "value_or_values" => @kv_set.values,
                         "pre_proc" => @pre_proc})
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end

    it "sets data in the redis cache using code_block" do
      @redis_helper.set_pipelined{ |r|
                 @kv_set.each do |k, v|
                   r[k] = v
                 end                    
      }
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq @kv_set
    end

    it "sets data in the redis cache using code_block / pre_proc" do
      @redis_helper.set_pipelined({"pre_proc" => @pre_proc}){ |r|
                 @kv_set.each do |k, v|
                   r[k] = v
                 end                    
      }
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end

    it "sets data in the redis cache using code_block "\
       "RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined { |r|
                 @kv_set.each do |k, v|
                   r[k] = v
                 end                    
      }
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc1"; new_hash}
    end

    it "sets data in the redis cache using code_block / pre_proc"\
       "RedisHelper.new({""pre_proc"" => @pre_proc})" do
      @redis_helper1.set_pipelined({"pre_proc" => @pre_proc}){ |r|
                 @kv_set.each do |k, v|
                   r[k] = v
                 end                    
      }
      expect(@redis_helper.get(
                  {"key_or_keys" => @kv_set.keys})).to eq \
                  @kv_set.inject({}) \
                    { |new_hash, (k,v)| new_hash[k] = \
                                 v + " pre_proc"; new_hash}
    end
  end
end
