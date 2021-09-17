# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt
require 'test_helper'

module UnitHelper
  LOG_EXPRESSION = /([\w]+),\s+\[([^\]\s]+)\s+#([^\]]+)]\s+(\w+)\s+--\s+(\w+)?:\s+(.+)/

  def fixture(name)
    File.read $fixtures.join(name)
  end

  def strip(sql)
    sql.strip.gsub(/\n */, "\n")
  end

  def log_expression_message(msg)
    msg.gsub(LOG_EXPRESSION) do |match|
      severity  = $1
      date      = $2
      pid       = $3
      label     = $4
      app       = $5
      message   = $6
    end
  end
end
