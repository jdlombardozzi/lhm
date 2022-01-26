require File.expand_path(File.dirname(__FILE__)) + '/../unit_helper'

require 'lhm/throttler/threads_running'

describe Lhm::Throttler::ThreadsRunning do
  include UnitHelper

  before :each do
    @throttler = Lhm::Throttler::ThreadsRunning.new
  end

  describe '#stride' do
    describe 'default value' do
      it 'is present' do
        assert_equal(@throttler.stride, 2_000)
      end
    end

    describe 'when set by user' do
      before do
        @throttler = Lhm::Throttler::ThreadsRunning.new(stride: 100_500)
      end

      it 'returns that value' do
        assert_equal(@throttler.stride, 100_500)
      end
    end
  end

  describe '#throttle_seconds' do
    describe 'with no mysql activity' do
      before do
        def @throttler.threads_running
          0
        end
      end

      it 'does not alter the currently set timeout' do
        timeout = @throttler.timeout_seconds
        assert_equal(timeout, @throttler.send(:throttle_seconds))
      end
    end

    describe 'with an overloaded mysql' do
      before do
        def @throttler.threads_running
          100
        end
      end

      it 'doubles the currently set timeout' do
        timeout = @throttler.timeout_seconds
        assert_equal(timeout * 2, @throttler.send(:throttle_seconds))
      end

      it 'does not increase the timeout past the maximum' do
        @throttler.timeout_seconds = @throttler.max_timeout_seconds
        assert_equal(@throttler.max_timeout_seconds, @throttler.send(:throttle_seconds))
      end
    end

    describe 'with an idle mysql after it has previously been busy' do
      before do
        def @throttler.threads_running
          0
        end
      end

      it 'halves the currently set timeout' do
        @throttler.timeout_seconds *= 2 * 2
        timeout = @throttler.timeout_seconds
        assert_equal(timeout / 2, @throttler.send(:throttle_seconds))
      end

      it 'does not decrease the timeout past the minimum on repeated runs' do
        @throttler.timeout_seconds = @throttler.initial_timeout_seconds * 2
        assert_equal(@throttler.initial_timeout_seconds, @throttler.send(:throttle_seconds))
        assert_equal(@throttler.initial_timeout_seconds, @throttler.send(:throttle_seconds))
      end
    end
  end
end
