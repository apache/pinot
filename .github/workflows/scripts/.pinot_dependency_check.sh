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

# Navigate to the project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../" && pwd)"
cd "$REPO_ROOT"

echo "Running Maven build for pinot-dependency-verifier..."
mvn -pl pinot-dependency-verifier clean package -DskipTests

CHANGED_POMS=$(git diff --name-only origin/main | grep pom.xml)

# No changed POMs
if [ -z "$CHANGED_POMS" ]; then
  echo "No changed POM files detected. Skipping dependency verification."
  exit 0
fi

java -cp "pinot-dependency-verifier/target/classes" \
  org.apache.pinot.verifier.DepVerifier $CHANGED_POMS


#echo "Checking for dependency management violations..."
#
## Check hardcoded version in root POM
#hardcoded=$(awk '
#  /<dependencyManagement>/ { inDepMgmt = 1 }
#  /<\/dependencyManagement>/ { inDepMgmt = 0 }
#  inDepMgmt {
#    if ($0 ~ /<artifactId>/) {
#      sub(/.*<artifactId>/, "")
#      sub(/<\/artifactId>.*/, "")
#      artifactId = $0
#    }
#    if ($0 ~ /<version>/ && $0 !~ /\$\{/) {
#      sub(/.*<version>/, "")
#      sub(/<\/version>.*/, "")
#      version = $0
#      print artifactId ": " version
#    }
#  }
#' pom.xml)
#
#if [ -n "$hardcoded" ]; then
#  echo "Found hardcoded dependency versions:"
#  echo "$hardcoded"
#  exit 1
#fi
#
## Check if <version>s are defined in subdirectories
## If it does, it should start with ${...} and the variable should be defined in root POM
## If it's a number, add to list with its group ID
#find . -name "pom.xml" \
#  ! -path "./pinot-plugins/assembly-descriptor/*" \
#  ! -path "./contrib/pinot-druid-benchmark/*" \
#  ! -path "./pom.xml"
#
#echo "All dependency management guidelines are followed."
#exit 0