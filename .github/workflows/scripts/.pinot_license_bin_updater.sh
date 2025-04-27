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

# Java Version
java -version

# Check Network
ifconfig
netstat -i

SETTINGS_FILE="../settings.xml"
# PINOT_DISTRIBUTION_FILE="../../../pinot-distribution/pom.xml"
PINOT_DISTRIBUTION_FILE="./pinot-distribution/pom.xml"
PATH_OF_DEP_FILE="./pinot-distribution/target/reports/dependencies.html"

# Build project
mvn clean install -DskipTests -Ppresto-driver

sed -i '/<\/dependencies>/i \
<!-- pinot-plugins/pinot-batch-ingestion -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-batch-ingestion-common</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-batch-ingestion-hadoop</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-batch-ingestion-spark-2.4</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-batch-ingestion-spark-3</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-batch-ingestion-standalone</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-environment -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-azure</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-file-system -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-adls</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-gcs</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-hdfs</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-s3</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-input-format -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-avro-base</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-avro</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-clp-log</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-confluent-avro</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-csv</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-json</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-orc</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-parquet</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-protobuf</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-thrift</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-metrics -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-dropwizard</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-yammer</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-minion-tasks -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-minion-builtin-tasks</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-segment-uploader -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-segment-uploader-default</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-segment-writer -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-segment-writer-file-based</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-plugins/pinot-stream-ingestion -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-kafka-2.0</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-kafka-base</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-kinesis</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-pulsar</artifactId>\
  <version>${project.version}</version>\
</dependency>\
\
<!-- pinot-connectors -->\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-flink-connector</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-spark-2-connector</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-spark-3-connector</artifactId>\
  <version>${project.version}</version>\
</dependency>\
<dependency>\
  <groupId>org.apache.pinot</groupId>\
  <artifactId>pinot-spark-common</artifactId>\
  <version>${project.version}</version>\
</dependency>' $PINOT_DISTRIBUTION_FILE

cat $PINOT_DISTRIBUTION_FILE
read -p "ok?"

# After adding dependecies, write:
mvn -Ddependency.locations.enabled=false project-info-reports:dependencies -Ppresto-driver -pl :pinot-distribution

SOURCE_HTML=$(curl -s "$PATH_OF_DEP_FILE")
TMPFILE=$(mktemp)
echo "$HTML" > "$TMPFILE"

# Get compile time dependencies
COMPILE_1_XPATH="/html/body/div/main/section[1]/section/table"
COMPILE_2_XPATH="/html/body/div/main/section[2]/section[1]/table"
ROWS_1=$(xmllint --html --xpath "$COMPILE_1_XPATH//tr" "$TMPFILE" 2>/dev/null)
ROWS_2=$(xmllint --html --xpath "$COMPILE_2_XPATH//tr" "$TMPFILE" 2>/dev/null)

echo $ROWS_1 >> "~/test.txt"
echo $ROWS_2 >> "~/test2.txt"

# Get runtime dependencies
