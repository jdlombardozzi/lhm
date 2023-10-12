# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

if ENV['COV']
  require 'simplecov'
  SimpleCov.start
end

require 'minitest/autorun'
require 'minitest/spec'
require 'minitest/mock'
require 'mocha/minitest'
require 'after_do'
require 'byebug'
require 'pathname'
require 'lhm'
require 'active_record'

$project = Pathname.new(File.dirname(__FILE__) + '/..').cleanpath
$spec = $project.join('spec')
$fixtures = $spec.join('fixtures')

$db_name = 'test'

Database = Struct.new(:adapter, :client, :error_class, :timeout_error) do
  def query(connection, sql)
    results = connection.query(sql)
    results = results.each_hash if adapter == "trilogy"
    results
  end
end

DATABASE =
  case ENV['DATABASE_ADAPTER']
  when 'trilogy'
    require 'trilogy'

    if ActiveRecord.version < ::Gem::Version.new('7.1.0')
      require 'activerecord-trilogy-adapter'
      require 'trilogy_adapter/connection'

      ActiveRecord::Base.public_send :extend, TrilogyAdapter::Connection
    end

    Database.new('trilogy', Trilogy, Trilogy::BaseError, Trilogy::TimeoutError)
  else
    require 'mysql2'
    Database.new('mysql2', Mysql2::Client, Mysql2::Error, Mysql2::Error::TimeoutError)
  end

logger = Logger.new STDOUT
logger.level = Logger::WARN
Lhm.logger = logger

# Want test to be efficient without having to wait the normal value of 120s
Lhm::SqlRetry::RECONNECT_RETRY_MAX_ITERATION = 4

def without_verbose(&block)
  old_verbose, $VERBOSE = $VERBOSE, nil
  yield
ensure
  $VERBOSE = old_verbose
end

def printer
  printer = Lhm::Printer::Base.new

  def printer.notify(*) ;end
  def printer.end(*) [] ;end

  printer
end

def throttler
  Lhm::Throttler::Time.new(:stride => 100)
end

def init_test_db
  db_config = YAML.load_file(File.expand_path(File.dirname(__FILE__)) + '/integration/database.yml')
  conn = DATABASE.client.new(
    :host => '127.0.0.1',
    :username => db_config['master']['user'],
    :password => db_config['master']['password'],
    :port => db_config['master']['port']
  )

  conn.query("DROP DATABASE IF EXISTS #{$db_name}")
  conn.query("CREATE DATABASE #{$db_name}")
end

init_test_db


