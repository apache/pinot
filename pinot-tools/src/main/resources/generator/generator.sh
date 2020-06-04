#!/bin/bash
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


#ADMIN_PATH="/opt/pinot/bin/pinot-admin.sh"
ADMIN_PATH="./pinot-tools/target/pinot-tools-pkg/bin/pinot-admin.sh"
#PATTERN_BASEDIR="/opt/pinot/examples/docker/generators"
PATTERN_BASEDIR="./pinot-tools/src/main/resources/generator"
TEMP_DIR="/tmp/pinotGenerator"

if [ -z "$1" ]; then
  echo "No PATTERN name specified. Aborting."
  exit 1
fi

PATTERN_NAME="$1"
DATA_DIR="${TEMP_DIR:?}/${PATTERN_NAME}"
SEGMENT_DIR="${TEMP_DIR:?}/${PATTERN_NAME}Segment"

echo "Preparing temp directory for ${PATTERN_NAME}"
rm -rf "${DATA_DIR}"
rm -rf "${SEGMENT_DIR}"
mkdir -p "${TEMP_DIR}"

echo "Generating data for ${PATTERN_NAME} in ${DATA_DIR}"
${ADMIN_PATH} GenerateData \
-numFiles 1 -numRecords 354780  -format csv \
-schemaFile "${PATTERN_BASEDIR}/${PATTERN_NAME}_schema.json" \
-schemaAnnotationFile "${PATTERN_BASEDIR}/${PATTERN_NAME}_generator.json" \
-outDir "$DATA_DIR"

if [ ! -d "${DATA_DIR}" ]; then
  echo "Data generation failed. Aborting."
  exit 1
fi

echo "Creating segment for ${PATTERN_NAME} in ${SEGMENT_DIR}"
${ADMIN_PATH} CreateSegment \
-tableName "${PATTERN_NAME}" -segmentName "${PATTERN_NAME}" -format CSV -overwrite \
-schemaFile "${PATTERN_BASEDIR}/${PATTERN_NAME}_schema.json" \
-dataDir "${DATA_DIR}" \
-outDir "${SEGMENT_DIR}" || exit 1

if [ ! -d "${SEGMENT_DIR}" ]; then
  echo "Data generation failed. Aborting."
  exit 1
fi

echo "Adding table ${PATTERN_NAME}"
${ADMIN_PATH} AddTable -exec \
-tableConfigFile "${PATTERN_BASEDIR}/${PATTERN_NAME}_config.json" \
-schemaFile "${PATTERN_BASEDIR}/${PATTERN_NAME}_schema.json" || exit 1

echo "Uploading segment for ${PATTERN_NAME}"
${ADMIN_PATH} UploadSegment \
-tableName "${PATTERN_NAME}" \
-segmentDir "${SEGMENT_DIR}" || exit 1

echo "Succesfully applied PATTERN ${PATTERN_NAME}"
