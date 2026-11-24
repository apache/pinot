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
# Build with JDK 25 and verify the clients on Java 17 and the standalone SPI artifacts on Java 11.
# JAVA11_HOME and JAVA17_HOME may be set explicitly. GitHub-hosted runner variables are fallbacks.
set -e

VERIFIER_MODULE="pinot-java11-client-verifier"
VERIFIER_CLASSES="${VERIFIER_MODULE}/target/classes"
MVN="${MVN:-./mvnw}"
SPI_MODULES=(pinot-spi pinot-segment-spi pinot-timeseries/pinot-timeseries-spi)
JAVA11_HOME="${JAVA11_HOME:-${JAVA_HOME_11_X64:-${JAVA_HOME_11_ARM64:-}}}"
JAVA17_HOME="${JAVA17_HOME:-${JAVA_HOME_17_X64:-${JAVA_HOME_17_ARM64:-}}}"

# Build JDK, for the record.
java -version
for jdk_home in "${JAVA11_HOME}" "${JAVA17_HOME}"; do
  if [ -z "${jdk_home}" ] || [ ! -x "${jdk_home}/bin/java" ]; then
    echo "Set JAVA11_HOME and JAVA17_HOME to the SPI and client verification runtimes."
    exit 1
  fi
  "${jdk_home}/bin/java" -version
done

# Build the verifier and the client and SPI artifacts. The extra goal resolves each module's
# runtime classpath independently using reactor artifacts. Linting runs in a separate job.
#
# -Dshade.phase.prop=none matters: pinot-java-client and pinot-jdbc-client each produce a ~150 MB
# shaded jar, and shadedArtifactAttached=true means those jars never even appear on the runtime
# classpath this job verifies. The standard client jar and its dependency classpath suffice here.
# -DskipShade=true only deactivates the pinot-jdbc-client profile, so both require the phase override.
"${MVN}" clean package dependency:build-classpath -B -ntp -T1C -pl "${VERIFIER_MODULE}" -am \
  -DincludeScope=runtime -Dmdep.outputFile=target/spi-runtime-classpath.txt -Dmdep.regenerateFile=true \
  -DskipTests \
  -Dshade.phase.prop=none \
  -Dmaven.javadoc.skip=true \
  -Dlicense.skip=true \
  -Dcheckstyle.skip=true \
  -Dspotless.check.skip=true

classpath_from_file() {
  if [ ! -s "$1" ]; then
    echo "Expected Maven to write the runtime classpath to $1" >&2
    return 1
  fi
  cat "$1"
}

# Tracing off for the runs: echoing hundreds of jar paths would bury the verifier's report.
set +x
CLIENT_CLASSPATH="${VERIFIER_CLASSES}:$(classpath_from_file "${VERIFIER_MODULE}/target/runtime-classpath.txt")"
echo "Verifying the Java and JDBC clients on Java 17"
# Arrow's direct buffer access needs java.nio opened for applications using the classpath.
if ! "${JAVA17_HOME}/bin/java" --add-opens=java.base/java.nio=ALL-UNNAMED -cp "${CLIENT_CLASSPATH}" \
  org.apache.pinot.java11.ClientCompatibilityVerifier 17; then
  echo "Client verification failed. The runtime classpath was:"
  tr ':' '\n' <<< "${CLIENT_CLASSPATH}"
  exit 1
fi

for module in "${SPI_MODULES[@]}"; do
  artifact_id="${module##*/}"
  jars=()
  for jar in "${module}/target/${artifact_id}-"*.jar; do
    case "${jar}" in
      *-sources.jar|*-javadoc.jar|*-tests.jar|*-shaded.jar) continue ;;
    esac
    if [ -f "${jar}" ]; then
      jars+=("${jar}")
    fi
  done
  if [ "${#jars[@]}" -ne 1 ]; then
    echo "Expected exactly one main jar for ${artifact_id}, found ${#jars[@]}"
    exit 1
  fi
  SPI_CLASSPATH="${VERIFIER_CLASSES}:${jars[0]}:$(classpath_from_file "${module}/target/spi-runtime-classpath.txt")"
  echo "Verifying ${artifact_id} on Java 11"
  if ! "${JAVA11_HOME}/bin/java" -cp "${SPI_CLASSPATH}" org.apache.pinot.java11.SpiCompatibilityVerifier "${artifact_id}"; then
    echo "SPI verification failed. The runtime classpath was:"
    tr ':' '\n' <<< "${SPI_CLASSPATH}"
    exit 1
  fi
done
