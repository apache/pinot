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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

JAR_PATH="$(find $DIR/../lib/pinot-all-*-jar-with-dependencies.jar 2>/dev/null)"
ADMIN_PATH="$DIR/pinot-admin.sh"
TEMP_DIR=$(mktemp -d -t pinotGenerator-XXXXXXXX)
TEMPLATE_BASEDIR="$TEMP_DIR/generator"
CONTROLLER_HOST="localhost"
CONTROLLER_PORT="9000"
CONTROLLER_SCHEME="http"

USAGE="$(basename "$0") [-h] [-a PATH] [-c HOST:PORT] [-s SCHEME] [-j PATH] [-n ROWS] TEMPLATE_NAME [TABLE_NAME]

  where:
      -h  show this help text
      -a  set pinot-admin path for segement creation
      -c  set controller host and port (default: 'localhost:9000')
      -j  set jar path for resource extraction
      -n  set number of rows to generate (optional)
      -s  set connection scheme (default: 'http')"

while getopts ':ha:c:j:n:s:' OPTION; do
  case "$OPTION" in
    h) echo "$USAGE"
       exit
       ;;
    a) ADMIN_PATH="$OPTARG"
       ;;
    c) case $OPTARG in
         (*:*) CONTROLLER_HOST=${OPTARG%:*} CONTROLLER_PORT=${OPTARG##*:};;
         (*)   CONTROLLER_HOST=$OPTARG      CONTROLLER_PORT=9000;;
       esac
       ;;
    j) JAR_PATH="$OPTARG"
       ;;
    n) NUM_RECORDS="$OPTARG"
       ;;
    s) CONTROLLER_SCHEME="$OPTARG"
       ;;
    :) printf "missing argument for -%s\n" "$OPTARG" >&2
       echo "$USAGE" >&2
       exit 1
       ;;
   \?) printf "illegal option: -%s\n" "$OPTARG" >&2
       echo "$USAGE" >&2
       exit 1
       ;;
  esac
done
shift $((OPTIND - 1))

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

if [ -z "$NUM_RECORDS" ]; then
  if [ "$TEMPLATE_NAME" = "simpleWebsite" ]; then
    NUM_RECORDS=354780
  fi
  if [ "$TEMPLATE_NAME" = "complexWebsite" ]; then
    NUM_RECORDS=631152
  fi

  if [ -z "$NUM_RECORDS" ]; then
    echo "No row count specified and no defaults available. (Does the template exist?) Aborting."
    exit 1
  fi
fi

DATA_DIR="${TEMP_DIR:?}/${TEMPLATE_NAME}"
SEGMENT_DIR="${TEMP_DIR:?}/${TEMPLATE_NAME}Segment"

echo "Extracting template files from '${JAR_PATH}' to '${TEMP_DIR}'"

/bin/sh -c "cd \"${TEMP_DIR}\" && jar -xf \"${JAR_PATH}\" \"generator/${TEMPLATE_NAME}_schema.json\" \"generator/${TEMPLATE_NAME}_config.json\" \"generator/${TEMPLATE_NAME}_generator.json\""

if [ $? != 0 ]; then
  echo "Extracting template failed. Aborting."
  rm -rf "$TEMP_DIR"
  exit 1
fi

if [ ! -e "${TEMP_DIR}/generator/${TEMPLATE_NAME}_schema.json" ] || [ ! -e "${TEMP_DIR}/generator/${TEMPLATE_NAME}_config.json" ] || [ ! -e "${TEMP_DIR}/generator/${TEMPLATE_NAME}_generator.json" ]; then
  echo "Could not find template '$TEMPLATE_NAME'. Aborting."
  rm -rf "$TEMP_DIR"
  exit 1
fi

echo "Setting table name and schema name to $TABLE_NAME"
sed -i -e "s/\"tableName\": \"$TEMPLATE_NAME\"/\"tableName\": \"$TABLE_NAME\"/g" "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json"
sed -i -e "s/\"schemaName\": \"$TEMPLATE_NAME\"/\"schemaName\": \"$TABLE_NAME\"/g" "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json"
sed -i -e "s/\"schemaName\": \"$TEMPLATE_NAME\"/\"schemaName\": \"$TABLE_NAME\"/g" "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json"

NUM_RECORDS=354780
if [ "$TEMPLATE_NAME" = "complexWebsite" ]; then
  NUM_RECORDS=631152
fi

echo "Generating data for ${TEMPLATE_NAME} in ${DATA_DIR}"
JAVA_OPTS="" ${ADMIN_PATH} GenerateData \
-numFiles 1 -numRecords $NUM_RECORDS -format csv \
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
  echo "Segment creation failed. Aborting."
  rm -rf "$TEMP_DIR"
  exit 1
fi

echo "Adding table ${TABLE_NAME} from template ${TEMPLATE_NAME}"
JAVA_OPTS="" ${ADMIN_PATH} AddTable -exec \
-tableConfigFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_config.json" \
-schemaFile "${TEMPLATE_BASEDIR}/${TEMPLATE_NAME}_schema.json" \
-controllerHost "${CONTROLLER_HOST}" \
-controllerPort "${CONTROLLER_PORT}" \
-controllerProtocol "${CONTROLLER_SCHEME}"

if [ $? != 0 ]; then
  echo "Adding table failed. Aborting."
  rm -rf "$TEMP_DIR"
  exit 1
fi

echo "Uploading segment for ${TEMPLATE_NAME}"
JAVA_OPTS="" ${ADMIN_PATH} UploadSegment \
-tableName "${TABLE_NAME}" \
-segmentDir "${SEGMENT_DIR}" \
-controllerHost "${CONTROLLER_HOST}" \
-controllerPort "${CONTROLLER_PORT}" \
-controllerProtocol "${CONTROLLER_SCHEME}"

if [ $? != 0 ]; then
  echo "Segment upload failed. Aborting."
  rm -rf "$TEMP_DIR"
  exit 1
fi

echo "Deleting temp directory"
rm -rf "$TEMP_DIR"

echo "Successfully created table ${TABLE_NAME} from template ${TEMPLATE_NAME}"
