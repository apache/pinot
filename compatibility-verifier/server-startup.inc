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

COMPAT_SERVER_STARTUP_SOURCE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/CompatibilityServerStarter.java"

# Compile without Pinot dependencies so the bootstrap can run with each tested distribution's own classes and JDK.
function setupCompatibilityServerStartup() {
  local compiler=javac
  if [ -n "${JAVA_HOME:-}" ]; then
    compiler="$JAVA_HOME/bin/javac"
  fi
  mkdir -p "$1/server-startup" || return 1
  COMPAT_SERVER_STARTUP_CLASSES="$(cd "$1/server-startup" && pwd)" || return 1
  "$compiler" --release 11 -d "$COMPAT_SERVER_STARTUP_CLASSES" "$COMPAT_SERVER_STARTUP_SOURCE"
}

# Keep the generated script beside the original: appassembler resolves its libraries, plugins, and configuration
# relative to the script. Change only the main class, preserving all launcher JVM options and argument handling.
function prepareCompatibilityServerLauncher() {
  local launcher=$1
  local compatLauncher="${launcher%.sh}-compat.sh"
  local temporaryLauncher
  temporaryLauncher=$(mktemp "${compatLauncher}.XXXXXX") || return 1
  if ! awk '
    $1 == "org.apache.pinot.tools.admin.PinotAdministrator" {
      sub(/org\.apache\.pinot\.tools\.admin\.PinotAdministrator/, "org.apache.pinot.compat.CompatibilityServerStarter")
      replaced++
    }
    { print }
    END { if (replaced != 1) exit 1 }
  ' "$launcher" > "$temporaryLauncher"; then
    echo "Cannot locate exactly one PinotAdministrator main class in $launcher" >&2
    rm -f "$temporaryLauncher"
    return 1
  fi
  if ! chmod +x "$temporaryLauncher" || ! mv "$temporaryLauncher" "$compatLauncher"; then
    rm -f "$temporaryLauncher"
    return 1
  fi
  echo "$compatLauncher"
}
