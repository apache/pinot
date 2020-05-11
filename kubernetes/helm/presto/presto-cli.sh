#!/usr/bin/env bash

if [[ ! -f "/tmp/presto-cli" ]]; then
  curl -L https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.228/presto-cli-0.228-executable.jar -o /tmp/presto-cli
  chmod +x /tmp/presto-cli
fi

/tmp/presto-cli $@