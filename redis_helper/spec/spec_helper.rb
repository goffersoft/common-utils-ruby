require_relative '../lib/redis_helper'
 
require 'yaml'

def redis_server_enabled?
  begin
    redis = Redis.new
    redis['spec_test'] = 'test'
    x = redis['spec_test']
    raise if x != 'test'
    true
  rescue
    false 
  end
end

def feature_list_to_exclude
  feature_list = []
  feature_list.push(:require_redis_server) if !redis_server_enabled?
end

RSpec.configure do |config|
  config.filter_run_excluding *feature_list_to_exclude
end
