#!/usr/bin/env bash

base_dir=$(dirname "$0")
cd ${base_dir};

# fetch dependencies. example: mysql. See Chart.yaml
helm dependency update

# Note:
# - initdb files must end with .sql
# - When injecting yaml config via terminal, the period ('.') must be escaped and quoted
helm install thirdeye . --set-file mysql.initializationFiles."initdb\.sql"="./initdb.sql" $@
