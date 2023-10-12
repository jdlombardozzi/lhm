require 'delegate'
require 'forwardable'
require 'lhm/sql_retry'

module Lhm
    # Lhm::Connection inherits from SingleDelegator. It will forward any unknown method calls to the ActiveRecord
    # connection.
  class Connection < SimpleDelegator
    extend Forwardable

    # Will delegate the following function to @sql_retry object, while leaving them accessible from the Lhm::Connection
    # object
    def_delegators :@sql_retry, :reconnect_with_consistent_host, :reconnect_with_consistent_host=, :retry_config=

    alias ar_connection __getobj__

    def initialize(connection:, options: {})
      @sql_retry = Lhm::SqlRetry.new(
        connection,
        retry_options: options[:retriable] || {},
        reconnect_with_consistent_host: options[:reconnect_with_consistent_host] || false
      )

      # Creates delegation for the ActiveRecord Connection
      super(connection)
    end

    def ar_connection=(connection)
      raise Lhm::Error.new("Lhm::Connection requires an active record connection to operate") if connection.nil?

      @sql_retry.connection = connection
      # Sets connection as the delegated object
      __setobj__(connection)
    end

    # ActiveRecord::Base overridden methods to incorporate custom retry logic
    # All other methods will be delegated
    def execute(query, should_retry: false, log_prefix: nil)
      if should_retry
        exec_with_retries(:execute, query, log_prefix)
      else
        exec(:execute, query)
      end
    end

    def update(query, should_retry: false, log_prefix: nil)
      if should_retry
        exec_with_retries(:update, query, log_prefix)
      else
        exec(:update, query)
      end
    end

    def select_value(query, should_retry: false, log_prefix: nil)
      if should_retry
        exec_with_retries(:select_value, query, log_prefix)
      else
        exec(:select_value, query)
      end
    end

    def select_values(query, should_retry: false, log_prefix: nil)
      if should_retry
        exec_with_retries(:select_values, query, log_prefix)
      else
        exec(:select_values, query)
      end
    end

    def select_one(query, should_retry: false, log_prefix: nil)
      if should_retry
        exec_with_retries(:select_one, query, log_prefix)
      else
        exec(:select_one, query)
      end
    end

    def select_all(query, should_retry: false, log_prefix: nil)
      if should_retry
        exec_with_retries(:select_all, query, log_prefix)
      else
        exec(:select_all, query)
      end
    end

    private

    def exec(method, sql)
      ar_connection.public_send(method, Lhm::ProxySQLHelper.tagged(sql))
    end

    def exec_with_retries(method, sql, log_prefix=nil)
      effective_log_prefix = log_prefix || file
      @sql_retry.with_retries(log_prefix: effective_log_prefix) do |conn|
        conn.public_send(method, Lhm::ProxySQLHelper.tagged(sql))
      end
    end

    # Returns camelized file name of caller (e.g. chunk_insert.rb -> ChunkInsert)
    def file
      # Find calling file and extract name
      /[\/]*(\w+).rb:\d+:in/.match(relevant_caller)
      name = $1&.camelize || "Connection"
      "#{name}"
    end

    def relevant_caller
      lhm_stack = caller.select { |x| x.include?("/lhm") }
      first_candidate_index = lhm_stack.find_index { |line| !line.include?(__FILE__) }

      # Find the file that called the `#execute` (fallbacks to current file)
      return lhm_stack.first unless first_candidate_index
      lhm_stack.at(first_candidate_index)
    end
  end
end
