#
# Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/bin/bash
# Abort on Error
set -e

export PING_SLEEP=30s
export BUILD_OUTPUT=build.out

touch $BUILD_OUTPUT

dump_output() {
   # nicely terminate the ping output loop
   kill $PING_LOOP_PID
   sleep 5s
   echo Tailing the last 700 lines of output:
   tail -700 $BUILD_OUTPUT
}
error_handler() {
  echo ERROR: An error was encountered with the build.
  dump_output
  exit 1
}
# If an error occurs, run our error handler to output a tail of the build
trap 'error_handler' ERR

# Set up a repeating loop to send some output to Travis.

bash -c "while true; do echo \$(date) - building ...; sleep $PING_SLEEP; done" &
PING_LOOP_PID=$!

# Build script
export MAVEN_OPTS="-Xmx4g -Xms4g -XX:MaxPermSize=512m"
mvn test -X -Dmaven.test.failure.ignore=true -Dassembly.skipAssembly=true  >> $BUILD_OUTPUT 2>&1

# The build finished without returning an error so dump a tail of the output
dump_output
