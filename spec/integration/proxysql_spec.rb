describe "ProxySQL integration" do
  it "Should contact the writer" do
    conn = DATABASE.client.new(
      host: '127.0.0.1',
      username: "writer",
      password: "password",
      port: "13005",
      )

    assert_equal DATABASE.query(conn, "SELECT @@global.hostname as host").each.first["host"], "mysql-1"
  end

  it "Should contact the reader" do
    conn = DATABASE.client.new(
      host: '127.0.0.1',
      username: "reader",
      password: "password",
      port: "13005",
      )

    assert_equal DATABASE.query(conn, "SELECT @@global.hostname as host").each.first["host"], "mysql-2"
  end

  it "Should override default hostgroup from user if rule matches" do
    conn = DATABASE.client.new(
      host: '127.0.0.1',
      username: "reader",
      password: "password",
      port: "13005",
      )

    assert_equal DATABASE.query(conn, "SELECT @@global.hostname as host #{Lhm::ProxySQLHelper::ANNOTATION}").each.first["host"], "mysql-1"
  end
end
