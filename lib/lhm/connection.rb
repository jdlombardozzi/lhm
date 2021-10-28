
module Lhm
  class Connection

    def initialize(connection:, default_log_prefix: nil, retry_options: {})
      @default_log_prefix = default_log_prefix
      @connection = connection
      @retry_options = retry_options || default_retry_config
      @sql_retry = Lhm::SqlRetry.new(
        connection,
        retry_options,
      )
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

    # Delegate ALL unknown method calls to ActiveRecord's connection.
    # This ensures that there will be no breaking changes.
    def method_missing(m, *args, &block)
      @connection.public_send(m, *args, &block)
    end

    private

    def exec_with_retries(method, sql, retry_options = {})
      retry_options[:log_prefix] ||= file
      @sql_retry.with_retries(retry_options) do |conn|
        conn.send(method, sql)
      end
    end

    # Returns camelized file name of caller (e.g. chunk_insert.rb -> ChunkInsert)
    def file
      # check order
      /[\/]*(\w+).rb:\d+:in/.match(relevant_caller)
      name = $1&.camelize || "Connection"
      "#{name}"
    end

    def relevant_caller
      lhm_stack = caller.select { |x| x.include?("/lhm") }

      # Find the file that called the `#execute` (fallbacks to current file)
      lhm_stack.at(3) || lhm_stack.first
    end
  end
end