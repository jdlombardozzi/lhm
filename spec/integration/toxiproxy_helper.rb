# frozen_string_literal: true
require 'toxiproxy'

module ToxiproxyHelper
  class << self

    def included(base)
      Toxiproxy.reset

      # listen on localhost, but toxiproxy is in a container itself, thus the upstream uses the Podman-Compose DNS
      Toxiproxy.populate(
        [
          {
            name: 'mysql_master',
            listen: '0.0.0.0:22220',
            upstream: 'mysql-1:3306'
          },
          {
            name: 'mysql_proxysql',
            listen: '0.0.0.0:22222',
            upstream: 'proxysql:3306'
          }
        ])
    end

    def with_kill_and_restart(target, restart_after)
      thread = Thread.new do
        sleep(restart_after) unless restart_after.nil?
        Toxiproxy[target].enable
      end

      Toxiproxy[target].disable

      yield

    ensure
      thread.join
    end
  end
end
