#!/bin/bash
# Wait for writer
echo "Waiting for MySQL-1: "
while ! (mysqladmin ping --host="127.0.0.1" --port=13006 --user=root --password=password --silent 2> /dev/null); do
  echo -ne "."
  sleep 1
done
# Wait for reader
echo "Waiting for MySQL-2: "
while ! (mysqladmin ping --host="127.0.0.1" --port=13007 --user=root --password=password --silent 2> /dev/null); do
  echo -ne "."
  sleep 1
done
# Wait for proxysql
echo "Waiting for ProxySQL:"
while ! (mysqladmin ping --host="127.0.0.1" --port=13005 --user=root --password=password --silent 2> /dev/null); do
  echo -ne "."
  sleep 1
done

echo "All DBs are ready"
