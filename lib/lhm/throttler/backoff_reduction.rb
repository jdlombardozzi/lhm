module Lhm
  module Throttler
    module BackoffReduction
      DEFAULT_BACKOFF_REDUCTION_FACTOR = 0.2
      MIN_STRIDE_SIZE = 1

      def initialize(options = {})
        @backoff_reduction_factor = options[:backoff_reduction_factor] || DEFAULT_BACKOFF_REDUCTION_FACTOR
        @min_stride_size = options[:min_stride_size] || MIN_STRIDE_SIZE

        if @backoff_reduction_factor >= 1 || @backoff_reduction_factor <= 0
          raise ArgumentError, 'backoff_reduction_factor must be between greater than 0, and less than 1'
        end

        if @min_stride_size < 1
          raise ArgumentError, 'min_stride_size must be an integer greater than 0'
        end

        if !@min_stride_size.is_a?(Integer)
          raise ArgumentError, 'min_stride_size must be an integer'
        end

        if @min_stride_size > @stride
          raise ArgumentError, 'min_stride_size must be less than or equal to stride'
        end
      end

      def backoff_stride
        new_stride = (@stride * (1 - @backoff_reduction_factor)).to_i

        if new_stride == @stride
          raise RuntimeError, "Cannot backoff any further"
        end

        if new_stride < @min_stride_size
          raise RuntimeError, "Cannot reduce stride below #{@min_stride_size}"
        end
        @stride = new_stride
      end
    end
  end
end
