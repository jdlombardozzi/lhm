require 'delegate'

module Lhm
  class Connection < SimpleDelegator

    # Lhm::Connection inherits from SingleDelegator. It will forward any unknown method calls to the ActiveRecord
    # connection.
    alias connection __getobj__
    alias connection= __setobj__

    def initialize(connection:, default_log_prefix: nil, retry_options: {})
      @default_log_prefix = default_log_prefix
      @retry_options = retry_options || default_retry_config
      @sql_retry = Lhm::SqlRetry.new(
        connection,
        retry_options,
      )

      # Creates delegation for the ActiveRecord Connection
      super(connection)
    end

    def execute(query, retry_options = {})
      exec_with_retries(:execute, query, retry_options)
    end

    def update(query, retry_options = {})
      exec_with_retries(:update, query, retry_options)
    end

    def select_value(query, retry_options = {})
      exec_with_retries(:select_value, query, retry_options)
    end

    private

    def exec_with_retries(method, sql, retry_options = {})
      retry_options[:log_prefix] ||= file
      @sql_retry.with_retries(retry_options) do |conn|
        conn.public_send(method, sql)
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
      first_candidate_index = lhm_stack.find_index {|line| !line.include?(__FILE__)}

      # Find the file that called the `#execute` (fallbacks to current file)
      return lhm_stack.first unless first_candidate_index
      lhm_stack.at(first_candidate_index)
    end
  end
end