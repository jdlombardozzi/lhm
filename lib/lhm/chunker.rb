require 'lhm/chunker/composite_range'
require 'lhm/chunker/range'

module Lhm
  module Chunker
    CLASSES = {
      :composite => Chunker::CompositeRange,
      :range => Chunker::Range
    }

    def setup_chunker(type, migration, connection, origin_key_columns, options = {})
      @chunker = Factory.create_chunker(type, migration, connection, origin_key_columns, options)
    end

    class Factory
      def self.create_chunker(type, migration, connection, origin_key_columns, options = {})
        # @todo Add custom option
        case type
        when Symbol
          CLASSES[type].new(migration, connection, origin_key_columns, options)
        when String
          CLASSES[type.to_sym].new(migration, connection, origin_key_columns, options)
        when Class
          type.new(migration, connection, origin_key_columns, options)
        else
          raise ArgumentError, 'type argument must be a Symbol, String or Class'
        end
      end
    end
  end
end
