#!/bin/bash
while ! mysqladmin ping --host="127.0.0.1" --port=33005 --user=root --password=password --silent; do
    sleep 1
done