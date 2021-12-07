require 'retriable'
require 'lhm/sql_helper'
require 'lhm/proxysql_helper'

module Lhm
  # SqlRetry standardizes the interface for retry behavior in components like
  # Entangler, AtomicSwitcher, ChunkerInsert.
  #
  # By default if an error includes the message "Lock wait timeout exceeded", or
  # "Deadlock found when trying to get lock", SqlRetry will retry again
  # once the MySQL client returns control to the caller, plus one second.
  # It will retry a total of 10 times and output to the logger a description
  # of the retry with error information, retry count, and elapsed time.
  #
  # This behavior can be modified by passing `options` that are documented in
  # https://github.com/kamui/retriable. Additionally, a "log_prefix" option,
  # which is unique to SqlRetry can be used to prefix log output.
  class SqlRetry
    RECONNECT_SUCCESSFUL_MESSAGE = "LHM successfully reconnected to initial host:"
    CLOUDSQL_VERSION_COMMENT = "(Google)"

    MYSQL_VAR_NAMES = {
      hostname: "@@global.hostname",
      server_id: "@@global.server_id",
      version_comment: "@@version_comment",
    }

    # This internal error is used to trigger retries from the parent Retriable.retriable in #with_retries
    class ReconnectToHostSuccessful < Lhm::Error; end

    def initialize(connection, retry_options: {}, reconnect_with_consistent_host: false)
      @connection = connection
      @log_prefix = retry_options.delete(:log_prefix)
      @global_retry_config = default_retry_config.dup.merge!(retry_options)
      if (@reconnect_with_consistent_host = reconnect_with_consistent_host)
        @initial_hostname = hostname
        @initial_server_id = server_id
      end
    end

    # Complete explanation of algorithm: https://github.com/Shopify/lhm/pull/112
    def with_retries(retry_config = {})
      @log_prefix = retry_config.delete(:log_prefix)

      retry_config = @global_retry_config.dup.merge!(retry_config)

      Retriable.retriable(retry_config) do
        # Using begin -> rescue -> end for Ruby 2.4 compatibility
        begin
          if @reconnect_with_consistent_host
            raise Lhm::Error.new("Could not reconnected to initial MySQL host. Aborting to avoid data-loss") unless same_host_as_initial?
          end

          yield(@connection)
        rescue StandardError => e
          # Not all errors should trigger a reconnect. Some errors such be raised and abort the LHM (such as reconnecting to the wrong host).
          # The error will be raised the connection is still active (i.e. no need to reconnect) or if the connection is
          # dead (i.e. not active) and @reconnect_with_host is false (i.e. instructed not to reconnect)
          raise e if @connection.active? || (!@connection.active? && !@reconnect_with_consistent_host)
          reconnect_with_host_check! if @reconnect_with_consistent_host
        end
      end
    end

    attr_reader :global_retry_config
    attr_accessor :connection, :reconnect_with_consistent_host

    private

    def hostname
      mysql_single_value(MYSQL_VAR_NAMES[:hostname])
    end

    def server_id
      mysql_single_value(MYSQL_VAR_NAMES[:server_id])
    end

    def cloudsql?
      mysql_single_value(MYSQL_VAR_NAMES[:version_comment]).include?(CLOUDSQL_VERSION_COMMENT)
    end

    def mysql_single_value(name)
      query = Lhm::ProxySQLHelper.tagged("SELECT #{name} LIMIT 1")

      @connection.execute(query).to_a.first.tap do |record|
        return record&.first
      end
    end

    def same_host_as_initial?
      return @initial_server_id == server_id if cloudsql?
      @initial_hostname == hostname
    end

    def log_with_prefix(message, level = :info)
      message.prepend("[#{@log_prefix}] ") if @log_prefix
      Lhm.logger.public_send(level, message)
    end

    def reconnect_with_host_check!
      log_with_prefix("Lost connection to MySQL, will retry to connect to same host")
      begin
        Retriable.retriable(host_retry_config) do
          # tries to reconnect. On failure will trigger a retry
          @connection.reconnect!

          if same_host_as_initial?
            # This is not an actual error, but controlled way to get the parent `Retriable.retriable` to retry
            # the statement that failed (since the Retriable gem only retries on errors).
            raise ReconnectToHostSuccessful.new("LHM successfully reconnected to initial host: #{@initial_hostname} (server_id: #{@initial_server_id})")
          else
            # New Master --> abort LHM (reconnecting will not change anything)
            raise Lhm::Error.new("Reconnected to wrong host. Started migration on: #{@initial_hostname} (server_id: #{@initial_server_id}), but reconnected to: #{hostname} (server_id: #{@initial_server_id}).")
          end
        end
      rescue StandardError => e
        # The parent Retriable.retriable is configured to retry if it encounters an error with the success message.
        # Therefore, if the connection is re-established successfully AND the host is the same, LHM can retry the query
        # that originally failed.
        raise e if reconnect_successful?(e)
        # If the connection was not successful, the parent retriable will raise any error that originated from the
        # `@connection.reconnect!`
        # Therefore, this error will cause the LHM to abort
        raise Lhm::Error.new("LHM tried the reconnection procedure but failed. Latest error: #{e.message}")
      end
    end

    def reconnect_successful?(e)
      e.is_a?(ReconnectToHostSuccessful)
    end

    # For a full list of configuration options see https://github.com/kamui/retriable
    def default_retry_config
      {
        on: {
          StandardError => [
            /Lock wait timeout exceeded/,
            /Timeout waiting for a response from the last query/,
            /Deadlock found when trying to get lock/,
            /Query execution was interrupted/,
            /Lost connection to MySQL server during query/,
            /Max connect timeout reached/,
            /Unknown MySQL server host/,
            /connection is locked to hostgroup/,
            /The MySQL server is running with the --read-only option so it cannot execute this statement/,
          ],
          ReconnectToHostSuccessful => [
            /#{RECONNECT_SUCCESSFUL_MESSAGE}/
          ]
        },
        multiplier: 1, # each successive interval grows by this factor
        base_interval: 1, # the initial interval in seconds between tries.
        tries: 20, # Number of attempts to make at running your code block (includes initial attempt).
        rand_factor: 0, # percentage to randomize the next retry interval time
        max_elapsed_time: Float::INFINITY, # max total time in seconds that code is allowed to keep being retried
        on_retry: Proc.new do |exception, try_number, total_elapsed_time, next_interval|
          if reconnect_successful?(exception)
            log_with_prefix("#{exception.message} -- triggering retry", :info)
          else
            log_with_prefix("#{exception.class}: '#{exception.message}' - #{try_number} tries in #{total_elapsed_time} seconds and #{next_interval} seconds until the next try.", :error)
          end
        end
      }.freeze
    end

    def host_retry_config
      {
        on: {
          StandardError => [
            /Lost connection to MySQL server at 'reading initial communication packet'/
          ]
        },
        multiplier: 1, # each successive interval grows by this factor
        base_interval: 0.25, # the initial interval in seconds between tries.
        tries: 20, # Number of attempts to make at running your code block (includes initial attempt).
        rand_factor: 0, # percentage to randomize the next retry interval time
        max_elapsed_time: Float::INFINITY, # max total time in seconds that code is allowed to keep being retried
        on_retry: Proc.new do |exception, try_number, total_elapsed_time, next_interval|
          log_with_prefix("#{exception.class}: '#{exception.message}' - #{try_number} tries in #{total_elapsed_time} seconds and #{next_interval} seconds until the next try.", :error)
        end
      }.freeze
    end
  end
end
