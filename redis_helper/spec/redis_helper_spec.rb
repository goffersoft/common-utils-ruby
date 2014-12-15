require 'spec_helper'

describe RedisHelper do
  describe "#new" do
    before :all do
      @redis_helper = RedisHelper.new
    end

    it "takes no parameters and returns a RedisHelper object" do
      expect(@redis_helper).to be_an_instance_of RedisHelper
    end
  end
end
