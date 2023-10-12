# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/migration'
require 'lhm/sql_retry'

module Lhm
  # Switches origin with destination table using an atomic rename.
  #
  # It should only be used if the MySQL server version is not affected by the
  # bin log affecting bug #39675. This can be verified using
  # Lhm::SqlHelper.supports_atomic_switch?.
  class AtomicSwitcher
    include Command

    attr_reader :connection

    LOG_PREFIX = "AtomicSwitcher"

    def initialize(migration, connection = nil)
      @migration = migration
      @connection = connection
      @origin = migration.origin
      @destination = migration.destination
    end

    def atomic_switch
      "RENAME TABLE `#{ @origin.name }` TO `#{ @migration.archive_name }`, " \
      "`#{ @destination.name }` TO `#{ @origin.name }`"
    end

    def validate
      unless @connection.data_source_exists?(@origin.name) &&
        @connection.data_source_exists?(@destination.name)
        error "`#{ @origin.name }` AND `#{ @destination.name }` MUST EXIST"
      end
    end

    private

    def execute
      @connection.execute(atomic_switch, should_retry: true, log_prefix: LOG_PREFIX)
    end
  end
end
