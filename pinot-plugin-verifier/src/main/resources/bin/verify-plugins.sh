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

DIST_DIR="${1:-${PINOT_DIST_DIR:-./build}}"
shift || true

if [ ! -d "$DIST_DIR" ]; then
  echo "verify-plugins.sh: distribution directory not found: $DIST_DIR" >&2
  echo "Run 'mvn -Pbin-dist install -DskipTests' first, then point this script at" >&2
  echo "the apache-pinot-VERSION-bin directory (or set PINOT_DIST_DIR)." >&2
  exit 2
fi

LIB_DIR="$DIST_DIR/lib"
PLUGINS_DIR="$DIST_DIR/plugins"
if [ ! -d "$LIB_DIR" ] || [ ! -d "$PLUGINS_DIR" ]; then
  echo "verify-plugins.sh: $DIST_DIR is not a Pinot distribution layout (missing lib/ or plugins/)" >&2
  exit 2
fi

# Locate the verifier jar. Two layouts:
#   - sitting inside the distribution (preferred — the assembly copies it next to pinot-all.jar)
#   - found relative to this script in dev (target/pinot-plugin-verifier-*.jar)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERIFIER_JAR="${VERIFIER_JAR:-}"
if [ -z "$VERIFIER_JAR" ]; then
  for candidate in \
      "$DIST_DIR"/lib/pinot-plugin-verifier-*.jar \
      "$SCRIPT_DIR"/../../../target/pinot-plugin-verifier-*.jar \
      "$SCRIPT_DIR"/../target/pinot-plugin-verifier-*.jar; do
    if compgen -G "$candidate" > /dev/null 2>&1; then
      VERIFIER_JAR="$(ls -1 $candidate | head -1)"
      break
    fi
  done
fi
if [ -z "$VERIFIER_JAR" ] || [ ! -f "$VERIFIER_JAR" ]; then
  echo "verify-plugins.sh: could not locate pinot-plugin-verifier jar." >&2
  echo "Set VERIFIER_JAR explicitly, or build with 'mvn -pl pinot-plugin-verifier package'." >&2
  exit 2
fi

# Build the classpath from lib/* — same layout as a real Pinot service launch. Plugin jars
# under plugins/ are intentionally NOT on this classpath: they should be loaded by
# PluginManager via -Dplugins.dir, not via the system classloader.
CLASSPATH="$LIB_DIR/*:$VERIFIER_JAR"

JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
if ! command -v "${JAVA_BIN%/}" > /dev/null 2>&1 && ! command -v java > /dev/null 2>&1; then
  echo "verify-plugins.sh: java not found on PATH and JAVA_HOME not set." >&2
  exit 2
fi
JAVA_BIN="${JAVA_BIN%/}"
if [ -z "$JAVA_BIN" ] || ! command -v "$JAVA_BIN" > /dev/null 2>&1; then
  JAVA_BIN="java"
fi

echo "verify-plugins.sh: distribution = $DIST_DIR"
echo "verify-plugins.sh: verifier     = $VERIFIER_JAR"
echo

exec "$JAVA_BIN" \
    -cp "$CLASSPATH" \
    -Dplugins.dir="$PLUGINS_DIR" \
    org.apache.pinot.verifier.PluginVerifier \
    "$@"
