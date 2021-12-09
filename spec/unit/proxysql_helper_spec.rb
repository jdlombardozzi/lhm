require File.expand_path(File.dirname(__FILE__)) + '/unit_helper'

describe Lhm::ProxySQLHelper do

  after(:each) do
    Lhm::ProxySQLHelper.disable_tags = false
  end

  it "should not tag if the flag is active" do
    Lhm::ProxySQLHelper.disable_tags = true
    assert_equal("dummy", Lhm::ProxySQLHelper.tagged("dummy"))
  end

  it "should tag if the flag is not active (default)" do
    assert_equal("/*maintenance:lhm*/ dummy", Lhm::ProxySQLHelper.tagged("dummy"))
  end
end