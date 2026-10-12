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

set -euo pipefail
# Match the unit-test workflow's Maven Central retry and connection settings, including standalone runs.
export MAVEN_OPTS="${MAVEN_OPTS:-} -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 \
-Dmaven.wagon.http.retryHandler.count=30 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false"
TDIGEST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TDIGEST_FIXTURES="$TDIGEST_ROOT/pinot-segment-local/target/tdigest-compat-fixtures"
TDIGEST_WORK="$TDIGEST_ROOT/pinot-segment-local/target/tdigest-legacy-reader"
TDIGEST_RUNTIME_CLASSPATH="$TDIGEST_FIXTURES/pinot-reader-classpath.txt"
if [[ ! -s "$TDIGEST_FIXTURES/manifest.tsv" || ! -s "$TDIGEST_RUNTIME_CLASSPATH" ]]; then
  # Explicit selection keeps the gate independent of unit-test shard filters and quarantine lists.
  "$TDIGEST_ROOT/mvnw" -f "$TDIGEST_ROOT/pom.xml" -B -ntp -pl pinot-segment-local -am \
    -Dtest=LegacyTDigestCompatibilityTest -Dsurefire.failIfNoSpecifiedTests=false -DskipTests=false test
fi
if [[ ! -s "$TDIGEST_FIXTURES/manifest.tsv" || ! -s "$TDIGEST_RUNTIME_CLASSPATH" ]]; then
  printf 'LegacyTDigestCompatibilityTest did not generate the required fixtures in %s.\n' "$TDIGEST_FIXTURES" >&2
  exit 1
fi
TDIGEST_PINOT_CLASSPATH="$TDIGEST_ROOT/pinot-segment-local/target/classes:$(cat "$TDIGEST_RUNTIME_CLASSPATH")"
mkdir -p "$TDIGEST_WORK"
javac -cp "$TDIGEST_PINOT_CLASSPATH" -d "$TDIGEST_WORK" \
  "$TDIGEST_ROOT/compatibility-verifier/tdigest-compatibility/PinotReader.java"
for TDIGEST_VERSION in 3.2 3.3; do
  TDIGEST_JAR="$TDIGEST_WORK/t-digest-$TDIGEST_VERSION.jar"
  if [[ ! -s "$TDIGEST_JAR" ]]; then
    "$TDIGEST_ROOT/mvnw" -f "$TDIGEST_ROOT/pom.xml" -N -B -ntp \
      org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
      "-Dartifact=com.tdunning:t-digest:$TDIGEST_VERSION:jar" "-DoutputDirectory=$TDIGEST_WORK"
  fi
  javac -cp "$TDIGEST_JAR" -d "$TDIGEST_WORK" \
    "$TDIGEST_ROOT/compatibility-verifier/tdigest-compatibility/LegacyReader.java"
  java -ea:LegacyReader -Xmx128m -cp "$TDIGEST_WORK:$TDIGEST_JAR" LegacyReader "$TDIGEST_FIXTURES" "$TDIGEST_VERSION"
  # The Pinot JVM contains no tdunning classes; it consumes bytes and expectations from the legacy JVM.
  java -ea -Xmx128m -cp "$TDIGEST_WORK:$TDIGEST_PINOT_CLASSPATH" \
    PinotReader "$TDIGEST_FIXTURES/legacy-$TDIGEST_VERSION" "$TDIGEST_VERSION"
done
# Keep the checked-in accuracy oracle executable and independent from the current Pinot implementation.
bash "$TDIGEST_ROOT/compatibility-verifier/tdigest-compatibility/generate-rank-errors.sh"
