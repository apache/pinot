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
# Verifies that Pinot's Java-11-pinned client and SPI artifacts work on a Java 11 JVM.
#
# The build cannot run on Java 11 -- the root pom enforces requireJavaVersion [25,) and Pinot's
# services have a genuine Java 25 floor. So this is necessarily a two-JDK job: build the artifacts
# with the build JDK, then run the verifier under Java 11.
#
# Environment:
#   JAVA11_HOME  Optional. Home of the JVM to verify against. Falls back to the
#                JAVA_HOME_11_<arch> variables that GitHub-hosted runners export.
#   MVN          Optional. Maven command, defaults to "mvn".

TARGET_JAVA_VERSION=11
VERIFIER_MODULE="pinot-java11-client-verifier"
VERIFIER_MAIN_CLASS="org.apache.pinot.java11.Java11CompatibilityVerifier"
MVN="${MVN:-mvn}"

# Build JDK, for the record.
java -version

# GitHub-hosted runners export JAVA_HOME_<version>_<arch> for every JDK that setup-java installed.
# The arch suffix differs between x64 and arm64 runners, so try both rather than assuming.
if [ -z "${JAVA11_HOME}" ]; then
  JAVA11_HOME="${JAVA_HOME_11_X64:-${JAVA_HOME_11_ARM64:-}}"
fi
if [ -z "${JAVA11_HOME}" ]; then
  echo "No Java ${TARGET_JAVA_VERSION} JVM found. Set JAVA11_HOME, or install one with actions/setup-java" \
       "and pass its path through."
  exit 1
fi

JAVA11_BIN="${JAVA11_HOME}/bin/java"
if [ ! -x "${JAVA11_BIN}" ]; then
  echo "Not an executable JVM launcher: ${JAVA11_BIN}"
  exit 1
fi
"${JAVA11_BIN}" -version || exit 1

# Build the verifier and everything it depends on, which is exactly the six Java-11-pinned modules
# and the third-party closure underneath them. Linting is covered by the linter job, so skip it here.
#
# -Dshade.phase.prop=none matters: pinot-java-client and pinot-jdbc-client each produce a ~150 MB
# shaded jar, and shadedArtifactAttached=true means those jars never even appear on the runtime
# classpath this job verifies. Without the flag the job spends minutes building them and then pushes
# 300 MB into ~/.m2, which actions/cache uploads under a key the other Maven jobs share. Note that
# -DskipShade=true is not enough: it only deactivates the pinot-jdbc-client profile, while
# pinot-java-client sets shade.phase.prop=package unconditionally.
${MVN} clean install -B -ntp -T1C -pl "${VERIFIER_MODULE}" -am \
  -DskipTests \
  -Dshade.phase.prop=none \
  -Dmaven.javadoc.skip=true \
  -Dlicense.skip=true \
  -Dcheckstyle.skip=true \
  -Dspotless.check.skip=true || exit 1

CLASSPATH_FILE="${VERIFIER_MODULE}/target/runtime-classpath.txt"
if [ ! -s "${CLASSPATH_FILE}" ]; then
  echo "Expected the build to write the resolved runtime closure to ${CLASSPATH_FILE}"
  exit 1
fi

VERIFIER_CLASSPATH="${VERIFIER_MODULE}/target/classes:$(cat "${CLASSPATH_FILE}")"

# Tracing off for the run itself: echoing a few hundred jar paths buries the verifier's own output.
set +x
echo "Running ${VERIFIER_MAIN_CLASS} on Java ${TARGET_JAVA_VERSION} against a closure of" \
     "$(tr ':' '\n' <<< "${VERIFIER_CLASSPATH}" | wc -l | tr -d ' ') classpath entries"

# No --add-opens or -Dio.netty.tryReflectionSetAccessible here on purpose. Those flags exist in
# Pinot's own launch scripts for JDK 17+, and adding them would paper over exactly the kind of
# runtime breakage this job is meant to catch. The verifier asserts it is really running on Java
# ${TARGET_JAVA_VERSION}, so a mis-wired JDK fails the job instead of passing it vacuously.
if ! "${JAVA11_BIN}" -cp "${VERIFIER_CLASSPATH}" "${VERIFIER_MAIN_CLASS}" "${TARGET_JAVA_VERSION}"; then
  # Hosted runners discard the workspace, so dump the closure that was verified while we still can.
  echo
  echo "Verification failed. The runtime closure that was verified:"
  tr ':' '\n' <<< "${VERIFIER_CLASSPATH}"
  exit 1
fi
