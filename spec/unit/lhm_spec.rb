# Copyright (c) 2011 - 2013, SoundCloud Ltd.

require File.expand_path(File.dirname(__FILE__)) + '/unit_helper'

describe Lhm do

  before(:each) do
    Lhm.remove_class_variable :@@logger if Lhm.class_variable_defined? :@@logger
  end

  describe 'logger' do

    it 'should use the default parameters if no logger explicitly set' do
      value(Lhm.logger).must_be_kind_of Logger
      value(Lhm.logger.level).must_equal Logger::INFO
      value(Lhm.logger.instance_eval { @logdev }.dev).must_equal STDOUT
    end

    it 'should use s new logger if set' do
      l = Logger.new('omg.ponies')
      l.level = Logger::ERROR
      Lhm.logger = l

      value(Lhm.logger.level).must_equal Logger::ERROR
      value(Lhm.logger.instance_eval { @logdev }.dev).must_be_kind_of File
      value(Lhm.logger.instance_eval { @logdev }.dev.path).must_equal 'omg.ponies'
    end
  end

  describe 'api' do

    before(:each) do
      @connection = mock()
    end

    it 'should create a new connection when calling setup' do
      Lhm.setup(@connection)
      value(Lhm.connection).must_be_kind_of(Lhm::Connection)
    end

    it 'should create a new connection when none is created' do
      ActiveRecord::Base.stubs(:connection).returns(@connection)
      value(Lhm.connection).must_be_kind_of(Lhm::Connection)
    end
  end
end
