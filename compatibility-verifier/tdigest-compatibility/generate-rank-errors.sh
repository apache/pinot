#!/usr/bin/env bash
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

# Regenerate with the pinned legacy utility and 3.3 jar, then compare all case keys, inputs and rank errors.
# Use --update to replace the checked-in reference after intentionally changing the seeded input corpus.
set -euo pipefail
TDIGEST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TDIGEST_BASELINE=f818bae6d75a91228eb3aac95c921bca4aeb2128
TDIGEST_WORK="$TDIGEST_ROOT/pinot-segment-local/target/tdigest-rank-oracle"
TDIGEST_REFERENCE="$TDIGEST_ROOT/pinot-core/src/test/resources/data/tdigest-3.3-k1-rank-errors.csv"
mkdir -p "$TDIGEST_WORK"
if ! git -C "$TDIGEST_ROOT" cat-file -e "$TDIGEST_BASELINE^{commit}" 2>/dev/null; then
  # CI checks out shallow history. Fetch only the immutable public baseline; leave the worktree and POMs alone.
  git -C "$TDIGEST_ROOT" fetch --no-tags --depth=1 https://github.com/apache/pinot.git "$TDIGEST_BASELINE"
fi
git -C "$TDIGEST_ROOT" show \
  "$TDIGEST_BASELINE:pinot-segment-local/src/main/java/org/apache/pinot/segment/local/utils/TDigestUtils.java" \
  > "$TDIGEST_WORK/TDigestUtils.java"
"$TDIGEST_ROOT/mvnw" -f "$TDIGEST_ROOT/pom.xml" -N -B -ntp \
  org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
  -Dartifact=com.tdunning:t-digest:3.3:jar "-DoutputDirectory=$TDIGEST_WORK"
javac -cp "$TDIGEST_WORK/t-digest-3.3.jar" -d "$TDIGEST_WORK" \
  "$TDIGEST_WORK/TDigestUtils.java" \
  "$TDIGEST_ROOT/compatibility-verifier/tdigest-compatibility/GenerateRankErrors.java"
# The historical duplicate-boundary assertions abort otherwise valid seeded 3.3 cases.
java -da -cp "$TDIGEST_WORK:$TDIGEST_WORK/t-digest-3.3.jar" GenerateRankErrors > "$TDIGEST_WORK/rank-errors.csv"
if [[ "${1:-}" == --update ]]; then
  sed '/^# mergeOrders=/,$d' "$TDIGEST_REFERENCE" > "$TDIGEST_WORK/reference-header.csv"
  cat "$TDIGEST_WORK/reference-header.csv" "$TDIGEST_WORK/rank-errors.csv" > "$TDIGEST_REFERENCE"
else
  sed -n '/^# mergeOrders=/,$p' "$TDIGEST_REFERENCE" > "$TDIGEST_WORK/checked-in-rank-errors.csv"
  diff -u "$TDIGEST_WORK/checked-in-rank-errors.csv" "$TDIGEST_WORK/rank-errors.csv"
fi
printf 'Independent t-digest 3.3 oracle: 270 case/input fingerprints and rank envelopes verified\n'
