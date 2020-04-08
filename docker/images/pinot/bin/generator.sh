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

JAR_PATH="/opt/pinot/lib/pinot-all-0.3.0-SNAPSHOT-jar-with-dependencies.jar"
ADMIN_PATH="/opt/pinot/bin/pinot-admin.sh"
TEMPLATE_BASEDIR="/tmp/pinotGenerator/generator"
TEMP_DIR="/tmp/pinotGenerator"

if [ -z "$1" ]; then
  echo "No template name specified. Aborting."
  exit 1
fi

TEMPLATE_NAME="$1"
DATA_DIR="${TEMP_DIR:?}/${TEMPLATE_NAME}"
SEGMENT_DIR="${TEMP_DIR:?}/${TEMPLATE_NAME}Segment"

echo "Preparing temp directory for ${TEMPLATE_NAME}"
rm -rf "${DATA_DIR}"
rm -rf "${SEGMENT_DIR}"
mkdir -p "${TEMP_DIR}"

echo "Extracting template files"
/bin/sh -c "cd \"${TEMP_DIR}\" && jar -xf \"${JAR_PATH}\" \"generator/${TEMPLATE_NAME}_schema.json\" \"generator/${TEMPLATE_NAME}_config.json\" \"generator/${TEMPLATE_NAME}_generator.json\""

echo "Generating data for ${TEMPLATE_NAME} in ${DATA_DIR}"
${ADMIN_PATH} GenerateData \
-numFiles 1 -numRecords 354780  -format csv \
-schemaFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json" \
-schemaAnnotationFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_generator.json" \
-outDir "$DATA_DIR"

if [ ! -d "${DATA_DIR}" ]; then
  echo "Data generation failed. Aborting."
  exit 1
fi

echo "Creating segment for ${TEMPLATE_NAME} in ${SEGMENT_DIR}"
${ADMIN_PATH} CreateSegment \
-tableName "${TEMPLATE_NAME}" -segmentName "${TEMPLATE_NAME}" -format CSV -overwrite \
-schemaFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json" \
-dataDir "${DATA_DIR}" \
-outDir "${SEGMENT_DIR}" || exit 1

if [ ! -d "${SEGMENT_DIR}" ]; then
  echo "Data generation failed. Aborting."
  exit 1
fi

echo "Adding table ${TEMPLATE_NAME}"
${ADMIN_PATH} AddTable -exec \
-tableConfigFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json" \
-schemaFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json" || exit 1

echo "Uploading segment for ${TEMPLATE_NAME}"
${ADMIN_PATH} UploadSegment \
-tableName "${TEMPLATE_NAME}" \
-segmentDir "${SEGMENT_DIR}" || exit 1

echo "Succesfully applied template ${TEMPLATE_NAME}"
