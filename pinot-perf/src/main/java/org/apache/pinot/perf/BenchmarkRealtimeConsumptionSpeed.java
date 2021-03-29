/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;


/**
 * Benchmark that writes a configurable amount of rows in Kafka and checks how much time it takes to consume all of
 * them.
 */
public class BenchmarkRealtimeConsumptionSpeed extends BaseClusterIntegrationTest {
  private static final int ROW_COUNT = 100_000;
  private static final long TIMEOUT_MILLIS = 20 * 60 * 1000L; // Twenty minutes

  public static void main(String[] args) {
    try {
      new BenchmarkRealtimeConsumptionSpeed().runBenchmark();
    } catch (Exception e) {
      System.exit(-1);
    }
    System.exit(0);
  }

  private void runBenchmark()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();

    // Start Kafka
    startKafka();

    startController();
    startBroker();
    startServer();


    // Unpack the Avro files
    File avroFile = unpackAvroData(_tempDir).get(0);

    // Create and upload the schema and table config
    addSchema(createSchema());
    addTableConfig(createRealtimeTableConfig(avroFile));

    // Generate ROW_COUNT rows and write them into Kafka
    new Thread(() -> {
      try {
        ClusterIntegrationTestUtils
            .pushRandomAvroIntoKafka(avroFile, KafkaStarterUtils.DEFAULT_KAFKA_BROKER, getKafkaTopic(), ROW_COUNT,
                getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn());
      } catch (Exception e) {
        // Ignored
      }
    }).start();

    // Count how many seconds it takes for select count(*) to match with ROW_COUNT
    long startTime = System.currentTimeMillis();

    int pinotRecordCount = -1;
    long timeAfterTimeout = System.currentTimeMillis() + TIMEOUT_MILLIS;
    do {
      Thread.sleep(500L);

      // Run the query
      try {
        JsonNode response = postQuery("select count(*) from mytable");
        pinotRecordCount = response.get("aggregationResults").get(0).get("value").asInt();
      } catch (Exception e) {
        // Ignore
        continue;
      }

      System.out.println("Pinot record count: " + pinotRecordCount);
      if (timeAfterTimeout < System.currentTimeMillis()) {
        throw new RuntimeException("Timeout exceeded!");
      }
    } while (ROW_COUNT != pinotRecordCount);

    long endTime = System.currentTimeMillis();

    System.out.println("Consumed " + ROW_COUNT + " rows in " + (endTime - startTime) / 1000.0 + " seconds");
    FileUtils.deleteDirectory(_tempDir);
  }
}
