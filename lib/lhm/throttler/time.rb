module Lhm
  module Throttler
    class Time
      include Command
      include BackoffReduction

      DEFAULT_TIMEOUT = 0.1
      DEFAULT_STRIDE = 2_000
      DEFAULT_BACKOFF_REDUCTION_FACTOR = 0.2 # 20%
      MIN_STRIDE_SIZE = 1

      attr_accessor :timeout_seconds
      attr_accessor :stride

      def initialize(options = {})
        @timeout_seconds = options[:delay] || DEFAULT_TIMEOUT
        @stride = options[:stride] || DEFAULT_STRIDE

        super
      end

      def execute
        sleep timeout_seconds
      end
    end

    class LegacyTime < Time
      def initialize(timeout, stride)
        @timeout_seconds = timeout / 1000.0
        @stride = stride
      end
    end
  end
end
