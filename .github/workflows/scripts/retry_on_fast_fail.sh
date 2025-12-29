#!/bin/bash -x
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
# Run a command piped on stdin and trigger a retry exit code only when the
# command fails within a configurable threshold.

set -euo pipefail

RETRY_EXIT_CODE=${RETRY_EXIT_CODE:-75}
RETRY_FAIL_MAX_SECONDS=${RETRY_FAIL_MAX_SECONDS:-1200}

tmp_script=$(mktemp)
cat > "$tmp_script"

start=$(date +%s)
set +e
bash "$tmp_script"
rc=$?
duration=$(( $(date +%s) - start ))

if [ "$rc" -ne 0 ] && [ "$duration" -le "$RETRY_FAIL_MAX_SECONDS" ]; then
  echo "Command failed after ${duration}s (<=${RETRY_FAIL_MAX_SECONDS}s); signalling retry with exit code ${RETRY_EXIT_CODE}"
  exit "$RETRY_EXIT_CODE"
fi

exit "$rc"
