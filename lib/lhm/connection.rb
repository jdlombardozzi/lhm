require 'forwardable'
require 'active_record'

module Lhm
  class Connection
    extend Forwardable

    # Defines methods to be forwarded to the ActiveRecord connection
    begin
      # ASK: There's a loading issue with this class even though the MySQL2 adapter isa the one used. Would there be anyway to do this but cleaner?
      require 'active_record/connection_adapters/mysql2_adapter' unless defined?(ActiveRecord::ConnectionAdapters::Mysql2Adapter)

      RETRIABLE_METHODS = [:update, :execute]
      MODULES = [
        ActiveRecord::ConnectionAdapters::DatabaseStatements,
        ActiveRecord::ConnectionAdapters::SchemaStatements,
        ActiveRecord::ConnectionAdapters::Mysql2Adapter
      ]
      methods = MODULES.flat_map(&:instance_methods).uniq - RETRIABLE_METHODS

      def_delegators :@connection, *methods
    end

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

    private

    def exec_with_retries(method, sql, retry_options = {})
      retry_options[:log_prefix] ||= file
      @sql_retry.with_retries(retry_options) do |conn|
        conn.send(method, sql)
      end
    end

    # returns humanized file of caller
    def file
      # check order
      /[\/]*(\w+).rb:\d+:in/.match(relevant_caller)
      name = $1&.camelize || "Connection"
      "#{name}"
    end

    def relevant_caller
      lhm_stack = caller.filter { |x| x.include?("/lhm") }

      # Find the file that called the `#execute` (fallbacks to current file)
      lhm_stack.at(3) || lhm_stack.first
    end
  end
end