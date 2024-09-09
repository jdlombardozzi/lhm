# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt
require 'lhm/command'
require 'lhm/sql_helper'
require 'lhm/printer'
require 'lhm/chunk_insert'
require 'lhm/chunk_finder'

module Lhm
  module Chunker
    class CompositeRange
      include Command
      include SqlHelper

      attr_reader :connection

      LOG_PREFIX = "Chunker"

      # Copy from origin to destination in chunks of size `stride`.
      # Use the `throttler` class to sleep between each stride.
      def initialize(migration, connection = nil, origin_key_columns = [], options = {})
        @migration = migration
        @connection = connection
        # @chunk_finder = ChunkFinder.new(migration, connection, options)
        @options = options
        @raise_on_warnings = options.fetch(:raise_on_warnings, false)
        @verifier = options[:verifier]
        if @throttler = options[:throttler]
          @throttler.connection = @connection if @throttler.respond_to?(:connection=)
        end
        @origin_key_columns = origin_key_columns
        @printer = options[:printer] || Printer::Percentage.new
        @retry_options = options[:retriable] || {}
        @retry_helper = SqlRetry.new(
          @connection,
          retry_options: @retry_options
        )
      end

      def execute
        @start_time = Time.now

        # return if @chunk_finder.table_empty?
        # Initial values for the keyset pagination, will hold the last key for each value in origin_key_columns
        @next_to_insert = @origin_key_columns.each_with_object({}) do |column, hash|
          hash[column] = nil
        end

        loop do
          verify_can_run

          # Build the SELECT query with keyset pagination
          if @next_to_insert.values.all?(&:nil?)
            query = <<-SQL
              SELECT * FROM #{@migration.origin_name}
              ORDER BY #{@origin_key_columns.join(', ')}
              LIMIT #{@throttler.stride}
            SQL
          else
            query = <<-SQL
              SELECT * FROM #{@migration.origin_name}
              WHERE
                (#{@origin_key_columns.join(', ')}) > (#{@next_to_insert.values.join(',')})
              ORDER BY #{@origin_key_columns.join(', ')}
              LIMIT #{@throttler.stride}
            SQL
          end

          results = @connection.select_all(query, should_retry: true, log_prefix: LOG_PREFIX)

          # Break the loop if no rows are returned (i.e., end of the table)
          break if results.count == 0

          # Build the INSERT INTO query
          insert_query = "INSERT IGNORE INTO `#{@migration.destination_name}` (#{ @migration.destination_columns }) VALUES (%s)"
          query_values = []

          # Build values part of the insert query
          results.map do |row|
            values = []

            @migration.intersection.destination.each do |column|
              values << mysql_escape_value(row[column])
            end

            @origin_key_columns.each {|column| @next_to_insert[column] = mysql_escape_value(row[column]) }

            query_values << values.join(", ")
          end

          insert_query = insert_query % query_values.join('), (')

          begin
            affected_rows = @connection.update(insert_query, should_retry: true, log_prefix: 'ChunkCompositeInsert')
          rescue ActiveRecord::StatementInvalid => e
            if e.message.downcase.include?("transaction required more than 'max_binlog_cache_size' bytes of storage") && @throttler.respond_to?(:backoff_stride)
              Lhm.logger.info("Encountered max_binlog_cache_size error, attempting to reduce stride size")
              @throttler.backoff_stride
              next
            else
              raise e
            end
          end

          # Only log the chunker progress every 5 minutes instead of every iteration
          current_time = Time.now
          if current_time - @start_time > (5 * 60)
            Lhm.logger.info("Inserted #{affected_rows} rows into the destination table from (#{@origin_key_columns.join(', ')}) > (#{@next_to_insert.values.join(',')})")
            @start_time = current_time
          end

          # if affected_rows < expected_rows
          #   raise_on_non_pk_duplicate_warning
          # end

          if @throttler && affected_rows > 0
            @throttler.run
          end

          Lhm.logger.info(@next_to_insert)
        end

        @printer.end
      rescue => e
        @printer.exception(e) if @printer.respond_to?(:exception)
        raise
      end

      private

      def mysql_escape_value(value)
        if value.nil?
          "NULL"
        elsif value.is_a?(String)
          # Escape single quotes in strings and wrap in quotes
          "'#{value.gsub("'", "\\\\'")}'"
        elsif value.is_a?(Date)
          "'#{value.strftime('%Y-%m-%d')}'" # Format Date objects as 'YYYY-MM-DD'
        elsif value.is_a?(DateTime) || value.is_a?(Time)
          "'#{value.strftime('%Y-%m-%d %H:%M:%S')}'" # Format DateTime objects as 'YYYY-MM-DD HH:MM:SS'
        elsif [TrueClass, FalseClass].include?(value.class)
          # Convert boolean values to 1 or 0
          (value ? '1' : '0')
        else
          # Handle numeric types and other directly convertible types
          value.to_s
        end
      end

      def raise_on_non_pk_duplicate_warning
        @connection.select_all("SHOW WARNINGS", should_retry: true, log_prefix: LOG_PREFIX).each do |row|
          next if row["Message"].start_with?("Duplicate entry") && row["Message"].match?(/for key '(#{@migration.destination_name}\.)?PRIMARY'\z/)

          m = "Unexpected warning found for inserted row: #{row["Message"]}"
          Lhm.logger.warn(m)
          raise Error.new(m) if @raise_on_warnings
        end
      end

      def verify_can_run
        return unless @verifier
        @retry_helper.with_retries(log_prefix: LOG_PREFIX) do |retriable_connection|
          raise "Verification failed, aborting early" if !@verifier.call(retriable_connection)
        end
      end

      def validate
        nil
      end
    end
  end
end
