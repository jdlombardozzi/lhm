#!/bin/bash
# Wait for writer
while ! mysqladmin ping --host="127.0.0.1" --port=33006 --user=root --password=password --silent; do
    sleep 1
done
# Wait for reader
while ! mysqladmin ping --host="127.0.0.1" --port=33007 --user=root --password=password --silent; do
    sleep 1
done
# Wait for proxysql
while ! mysqladmin ping --host="127.0.0.1" --port=33005 --user=root --password=password --silent; do
    sleep 1
done