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

echo "Compiling the benchmark driver..."
mvn clean install > /dev/null
rm -rf temp results
mkdir temp results

echo "Untaring Pinot Segments Without Extra Index..."
tar -zxf pinot_non_startree.tar.gz -C temp
echo "Untaring Pinot Segments With Inverted Index..."
tar -zxf pinot_non_startree_inverted_index.tar.gz -C temp
#echo "Untaring Pinot Segments With Default Startree Index..."
#tar -zxf pinot_default_startree.tar.gz -C temp
#echo "Untaring Pinot Segments With Optimal Startree Index..."
#tar -zxf pinot_optimal_startree.tar.gz -C temp
echo "Untaring Druid Segments..."
tar -zxf druid_segment_cache.tar.gz -C temp

cd temp
echo "Downloading Druid..."
curl -O http://static.druid.io/artifacts/releases/druid-0.9.2-bin.tar.gz
tar -zxf druid-0.9.2-bin.tar.gz
rm druid-0.9.2-bin.tar.gz
echo "Downloading ZooKeeper..."
curl -O http://apache.claz.org/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
tar -zxf zookeeper-3.4.6.tar.gz
rm zookeeper-3.4.6.tar.gz
cd ..

echo "Benchmarking Pinot without Extra Index..."
java -jar pinot-tool-launcher-jar-with-dependencies.jar PerfBenchmarkRunner -mode startAll -dataDir temp/non_startree_small_yearly -tableNames tpch_lineitem_OFFLINE > /dev/null 2>&1 &
PINOT_PROCESS_ID=$!
echo ${PINOT_PROCESS_ID}
echo "Wait 30 seconds so that cluster is ready for processing queries..."
sleep 30
echo "Starting response time benchmark..."
./target/appassembler/bin/pinot-response-time.sh src/main/resources/pinot_queries http://localhost:8099/query 20 20 results/pinot_non_startree | tee results/pinot_non_startree_response_time.txt
echo "Starting throughput benchmark..."
./target/appassembler/bin/pinot-throughput.sh src/main/resources/pinot_queries http://localhost:8099/query 5 60 | tee results/pinot_non_startree_throughput.txt
kill -9 ${PINOT_PROCESS_ID}

echo "Benchmarking Pinot with Inverted Index..."
java -jar pinot-tool-launcher-jar-with-dependencies.jar PerfBenchmarkRunner -mode startAll -dataDir temp/non_startree_small_yearly_inverted_index -tableNames tpch_lineitem_OFFLINE -invertedIndexColumns l_receiptdate,l_shipmode > /dev/null 2>&1 &
PINOT_PROCESS_ID=$!
echo "Wait 30 seconds so that cluster is ready for processing queries..."
sleep 30
echo "Starting response time benchmark..."
./target/appassembler/bin/pinot-response-time.sh src/main/resources/pinot_queries http://localhost:8099/query 20 20 results/pinot_non_startree_inverted_index | tee results/pinot_non_startree_inverted_index_response_time.txt
echo "Starting throughput benchmark..."
./target/appassembler/bin/pinot-throughput.sh src/main/resources/pinot_queries http://localhost:8099/query 5 60 | tee results/pinot_non_startree_inverted_index_throughput.txt
kill -9 ${PINOT_PROCESS_ID}

#echo "Benchmarking Pinot with Default Startree Index..."
#java -jar pinot-tool-launcher-jar-with-dependencies.jar PerfBenchmarkRunner -mode startAll -dataDir temp/default_startree_small_yearly -tableNames tpch_lineitem_OFFLINE > /dev/null 2>&1 &
#PINOT_PROCESS_ID=$!
#echo "Wait 30 seconds so that cluster is ready for processing queries..."
#sleep 30
#echo "Starting response time benchmark..."
#./target/appassembler/bin/pinot-response-time.sh src/main/resources/pinot_queries http://localhost:8099/query 20 20 results/pinot_default_startree | tee results/pinot_default_startree_response_time.txt
#echo "Starting throughput benchmark..."
#./target/appassembler/bin/pinot-throughput.sh src/main/resources/pinot_queries http://localhost:8099/query 5 60 | tee results/pinot_default_startree_throughput.txt
#kill -9 ${PINOT_PROCESS_ID}
#
#echo "Benchmarking Pinot with Optimal Startree Index..."
#java -jar pinot-tool-launcher-jar-with-dependencies.jar PerfBenchmarkRunner -mode startAll -dataDir temp/optimal_startree_small_yearly -tableNames tpch_lineitem_OFFLINE > /dev/null 2>&1 &
#PINOT_PROCESS_ID=$!
#echo "Wait 30 seconds so that cluster is ready for processing queries..."
#sleep 30
#echo "Starting response time benchmark..."
#./target/appassembler/bin/pinot-response-time.sh src/main/resources/pinot_queries http://localhost:8099/query 20 20 results/pinot_optimal_startree | tee results/pinot_optimal_startree_response_time.txt
#echo "Starting throughput benchmark..."
#./target/appassembler/bin/pinot-throughput.sh src/main/resources/pinot_queries http://localhost:8099/query 5 60 | tee results/pinot_optimal_startree_throughput.txt
#kill -9 ${PINOT_PROCESS_ID}

echo "Benchmarking Druid with Inverted Index (Default Setting)..."
cd temp/druid-0.9.2
./bin/init
rm -rf var/druid/segment-cache
mv ../segment-cache var/druid/segment-cache
#Start ZooKeeper
../zookeeper-3.4.6/bin/zkServer.sh start ../zookeeper-3.4.6/conf/zoo_sample.cfg > /dev/null 2>&1
#Replace Druid JVM config and broker runtime properties
cp ../../src/main/resources/config/druid_jvm.config conf/druid/broker/jvm.config
cp ../../src/main/resources/config/druid_jvm.config conf/druid/historical/jvm.config
cp ../../src/main/resources/config/druid_broker_runtime.properties conf/druid/broker/runtime.properties
#Start Druid cluster
java `cat conf/druid/broker/jvm.config | xargs` -cp conf-quickstart/druid/_common:conf/druid/broker:lib/* io.druid.cli.Main server broker > /dev/null 2>&1 &
DRUID_BROKER_PROCESS_ID=$!
java `cat conf/druid/historical/jvm.config | xargs` -cp conf-quickstart/druid/_common:conf/druid/historical:lib/* io.druid.cli.Main server historical > /dev/null 2>&1 &
DRUID_SERVER_PROCESS_ID=$!
#Run benchmark
cd ../..
echo "Wait 30 seconds so that cluster is ready for processing queries..."
sleep 30
echo "Starting response time benchmark..."
./target/appassembler/bin/druid-response-time.sh src/main/resources/druid_queries http://localhost:8082/druid/v2/?pretty 20 20 results/druid | tee results/druid_response_time.txt
echo "Starting throughput benchmark..."
./target/appassembler/bin/druid-throughput.sh src/main/resources/druid_queries http://localhost:8082/druid/v2/?pretty 5 60 | tee results/druid_throughput.txt
kill -9 ${DRUID_BROKER_PROCESS_ID}
kill -9 ${DRUID_SERVER_PROCESS_ID}
temp/zookeeper-3.4.6/bin/zkServer.sh stop temp/zookeeper-3.4.6/conf/zoo_sample.cfg > /dev/null 2>&1

echo "********************************************************************"
echo "* Benchmark finished. Results can be found in 'results' directory. *"
echo "********************************************************************"

exit 0
