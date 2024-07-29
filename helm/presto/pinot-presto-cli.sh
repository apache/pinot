#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

if [[ ! -f "/tmp/presto-cli" ]]; then
  curl -L https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.228/presto-cli-0.228-executable.jar -o /tmp/presto-cli
  chmod +x /tmp/presto-cli
fi

if [[ $(nc -z  localhost 18080) != 0 ]]; then
  kubectl port-forward service/presto-coordinator 18080:8080 -n pinot-quickstart > /dev/null &
fi

/tmp/presto-cli --server localhost:18080 --catalog pinot --schema default
pkill -f "kubectl port-forward service/presto-coordinator 18080:8080 -n pinot-quickstart"

