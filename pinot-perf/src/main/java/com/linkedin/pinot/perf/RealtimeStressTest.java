/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.integration.tests.ClusterIntegrationTestUtils;
import com.linkedin.pinot.integration.tests.OfflineClusterIntegrationTest;
import com.linkedin.pinot.integration.tests.RealtimeClusterIntegrationTest;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaServerStartable;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Stress test that writes an infinite amount of data in Kafka for a given duration until Pinot breaks.
 */
public class RealtimeStressTest extends RealtimeClusterIntegrationTest {
  private static final int ROW_COUNT = 100_000;
  private static final int MIN_ROW_COUNT = 100_000;
  private static final int ROW_COUNT_FOR_SEGMENT_FLUSH = 10_000;
  private static final long TIMEOUT_MILLIS = 20 * 60 * 1000L; // Twenty minutes
  private final File _tmpDir = new File("/tmp/" + getHelixClusterName());
  private static final int SEGMENT_COUNT = 1;
  private static final Random RANDOM = new Random(123456L);
  private static long rowsWritten = 0L;

  public static void main(String[] args) {
    try {
      new RealtimeStressTest().runBenchmark();
    } catch (Exception e) {
      System.exit(-1);
    }
  }

  private void runBenchmark() throws Exception {
    // Start ZK and Kafka
    startZk();
    KafkaServerStartable kafkaStarter = KafkaStarterUtils
        .startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    // Create Kafka topic
    KafkaStarterUtils.createTopic(getKafkaTopic(), KafkaStarterUtils.DEFAULT_ZK_STR, 10);

    // Unpack data (needed to get the Avro schema)
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(RealtimeClusterIntegrationTest.class.getClassLoader()
            .getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz"))), _tmpDir);

    _tmpDir.mkdirs();
    final List<File> avroFiles = new ArrayList<File>(SEGMENT_COUNT);
    for (int segmentNumber = 1; segmentNumber <= SEGMENT_COUNT; ++segmentNumber) {
      avroFiles.add(new File(_tmpDir.getPath() + "/On_Time_On_Time_Performance_2014_" + segmentNumber + ".avro"));
    }

    File schemaFile =
        new File(OfflineClusterIntegrationTest.class.getClassLoader()
            .getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema").getFile());

    // Start the Pinot cluster
    startController();
    startBroker();
    startServer();

    // Create realtime table
    setUpTable(avroFiles.get(0));

    // Wait a couple of seconds for all Helix state transitions to happen
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

    // Generate ROW_COUNT rows and write them into Kafka
    ClusterIntegrationTestUtils.pushRandomAvroIntoKafka(avroFiles.get(0), KafkaStarterUtils.DEFAULT_KAFKA_BROKER,
        getKafkaTopic(), ROW_COUNT, getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn());
    rowsWritten += ROW_COUNT;

    // Run forever until something breaks or the timeout completes
    long pinotRecordCount = -1L;
    long timeAfterTimeout = System.currentTimeMillis() + TIMEOUT_MILLIS;
    do {
      Thread.sleep(500L);

      // Run the query
      try {
        JSONObject response = postQuery("select count(*) from mytable");
        JSONArray aggregationResultsArray = response.getJSONArray("aggregationResults");
        JSONObject firstAggregationResult = aggregationResultsArray.getJSONObject(0);
        String pinotValue = firstAggregationResult.get("value").toString();
        pinotRecordCount = Long.parseLong(pinotValue);
      } catch (Exception e) {
        // Ignore
        continue;
      }

      // Write more rows if needed
      if (rowsWritten - pinotRecordCount < MIN_ROW_COUNT) {
        ClusterIntegrationTestUtils.pushRandomAvroIntoKafka(avroFiles.get(0), KafkaStarterUtils.DEFAULT_KAFKA_BROKER,
            getKafkaTopic(), ROW_COUNT, getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn());
        rowsWritten += ROW_COUNT;
      }

      System.out.println("Pinot record count: " + pinotRecordCount);
      if (timeAfterTimeout < System.currentTimeMillis()) {
        throw new RuntimeException("Timeout exceeded!");
      }
    } while (true);
  }
}
