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

# Run after LegacyTDigestCompatibilityTest. Legacy jars are test-tool artifacts, not Pinot dependencies.
set -euo pipefail
TDIGEST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TDIGEST_FIXTURES="$TDIGEST_ROOT/pinot-segment-local/target/tdigest-compat-fixtures"
TDIGEST_WORK="$TDIGEST_ROOT/pinot-segment-local/target/tdigest-legacy-reader"
test -s "$TDIGEST_FIXTURES/manifest.tsv"
mkdir -p "$TDIGEST_WORK"
for TDIGEST_VERSION in 3.2 3.3; do
  "$TDIGEST_ROOT/mvnw" -f "$TDIGEST_ROOT/pom.xml" -N -B -ntp \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
    "-Dartifact=com.tdunning:t-digest:$TDIGEST_VERSION:jar" "-DoutputDirectory=$TDIGEST_WORK"
  TDIGEST_JAR="$TDIGEST_WORK/t-digest-$TDIGEST_VERSION.jar"
  javac -cp "$TDIGEST_JAR" -d "$TDIGEST_WORK" \
    "$TDIGEST_ROOT/compatibility-verifier/tdigest-compatibility/LegacyReader.java"
  java -ea -Xmx128m -cp "$TDIGEST_WORK:$TDIGEST_JAR" LegacyReader "$TDIGEST_FIXTURES" "$TDIGEST_VERSION"
done
# Keep the checked-in accuracy oracle executable and independent from the current Pinot implementation.
bash "$TDIGEST_ROOT/compatibility-verifier/tdigest-compatibility/generate-rank-errors.sh"
