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

JAR_PATH="$(find /opt/pinot/lib/pinot-all-*-jar-with-dependencies.jar)"
ADMIN_PATH="/opt/pinot/bin/pinot-admin.sh"
TEMP_DIR=$(mktemp -d -t pinotGenerator-XXXXXXXX)
TEMPLATE_BASEDIR="$TEMP_DIR/generator"

TEMPLATE_NAME="$1"
if [ -z "$TEMPLATE_NAME" ]; then
  echo "No template name specified. Aborting."
  exit 1
fi

TABLE_NAME="$2"
if [ -z "$TABLE_NAME" ]; then
  echo "No table name specified. Defaulting to '$TEMPLATE_NAME'"
  TABLE_NAME=$TEMPLATE_NAME
fi

DATA_DIR="${TEMP_DIR:?}/${TEMPLATE_NAME}"
SEGMENT_DIR="${TEMP_DIR:?}/${TEMPLATE_NAME}Segment"

echo "Extracting template files to '${TEMP_DIR}'"
/bin/sh -c "cd \"${TEMP_DIR}\" && jar -xf \"${JAR_PATH}\" \"generator/${TEMPLATE_NAME}_schema.json\" \"generator/${TEMPLATE_NAME}_config.json\" \"generator/${TEMPLATE_NAME}_generator.json\""

echo "Setting table name and schema name to $TABLE_NAME"
sed -i -e "s/\"tableName\": \"$TEMPLATE_NAME\"/\"tableName\": \"$TABLE_NAME\"/g" "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json"
sed -i -e "s/\"schemaName\": \"$TEMPLATE_NAME\"/\"schemaName\": \"$TABLE_NAME\"/g" "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json"
sed -i -e "s/\"schemaName\": \"$TEMPLATE_NAME\"/\"schemaName\": \"$TABLE_NAME\"/g" "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json"

echo "Generating data for ${TEMPLATE_NAME} in ${DATA_DIR}"
JAVA_OPTS="" ${ADMIN_PATH} GenerateData \
-numFiles 1 -numRecords 631152  -format csv \
-schemaFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json" \
-schemaAnnotationFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_generator.json" \
-outDir "$DATA_DIR"

if [ ! -d "${DATA_DIR}" ]; then
  echo "Data generation failed. Aborting."
  rm -rf "$TEMP_DIR"
  exit 1
fi

echo "Creating segment for ${TEMPLATE_NAME} in ${SEGMENT_DIR}"
JAVA_OPTS="" ${ADMIN_PATH} CreateSegment \
-format csv \
-tableConfigFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json" \
-schemaFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json" \
-dataDir "${DATA_DIR}" \
-outDir "${SEGMENT_DIR}"

if [ ! -d "${SEGMENT_DIR}" ]; then
  echo "Data generation failed. Aborting."
  rm -rf "$TEMP_DIR"
  exit 1
fi

echo "Adding table ${TABLE_NAME} from template ${TEMPLATE_NAME}"
JAVA_OPTS="" ${ADMIN_PATH} AddTable -exec \
-tableConfigFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json" \
-schemaFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json" || exit 1

echo "Uploading segment for ${TEMPLATE_NAME}"
JAVA_OPTS="" ${ADMIN_PATH} UploadSegment \
-tableName "${TABLE_NAME}" \
-segmentDir "${SEGMENT_DIR}"

echo "Deleting temp directory"
rm -rf "$TEMP_DIR"

echo "Succesfully created table ${TABLE_NAME} from template ${TEMPLATE_NAME}"
