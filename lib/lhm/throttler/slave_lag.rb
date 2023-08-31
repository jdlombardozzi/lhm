require 'lhm/throttler/replica_lag'

module Lhm
  module Throttler
    class SlaveLag < ReplicaLag
      def initialize(options = {})
        Lhm.logger.warn("Class `SlaveLag` is deprecated. Use `ReplicaLag` class instead.")
        super(options)
      end
    end
  end
end
