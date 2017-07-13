/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.integration.tests;

import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Hybrid cluster integration test that uploads 8 months of data as offline and 6 months of data as realtime (with a
 * two month overlap).
 */
public class HybridClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String TENANT_NAME = "TestTenant";
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;

  private KafkaServerStartable _kafkaStarter;

  protected boolean useLlc() {
    return false;
  }

  protected int getNumOfflineSegments() {
    return NUM_OFFLINE_SEGMENTS;
  }

  protected int getNumRealtimeSegments() {
    return NUM_REALTIME_SEGMENTS;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    // Unpack the Avro files
    // NOTE: Avro files has to be ordered as time series data
    int numSegments = unpackAvroData(_tempDir).size();
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_tempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles);

    ExecutorService executor = Executors.newCachedThreadPool();

    // Create segments from Avro data
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, 0, _segmentDir, _tarDir, getTableName(), false,
        getRawIndexColumns(), null, executor);

    // Push data into the Kafka topic
    pushAvroIntoKafka(realtimeAvroFiles, getKafkaTopic(), executor);

    // Load data into H2
    setUpH2Connection(avroFiles, executor);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create Pinot table
    setUpTable("DaysSinceEpoch", "DAYS", avroFiles.get(0));

    // Upload all segments
    for (String segmentName : _tarDir.list()) {
      File segmentFile = new File(_tarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, segmentFile, segmentFile.length());
    }

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
    Thread.sleep(12312312L);
  }

  protected void startHybridCluster()
      throws Exception {
    // Start Zk and Kafka
    startZk();
    _kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    // Create Kafka topic
    KafkaStarterUtils.createTopic(getKafkaTopic(), KafkaStarterUtils.DEFAULT_ZK_STR, getNumKafkaPartitions());

    // Start the Pinot cluster
    ControllerConf config = ControllerTestUtils.getDefaultControllerConfiguration();
    config.setTenantIsolationEnabled(false);
    startController(config);
    startBroker();
    startServers(2);

    // Create tenants
    createBrokerTenant(TENANT_NAME, 1);
    createServerTenant(TENANT_NAME, 1, 1);
  }

  protected void setUpTable(String timeColumnName, String timeColumnType, File avroFile)
      throws Exception {
    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    String schemaName = schema.getSchemaName();
    addSchema(schemaFile, schemaName);
    addHybridTable(getTableName(), timeColumnName, timeColumnType, KafkaStarterUtils.DEFAULT_ZK_STR, getKafkaTopic(),
        schemaName, TENANT_NAME, TENANT_NAME, avroFile, getSortedColumn(), getInvertedIndexColumns(), getLoadMode(),
        useLlc(), getRawIndexColumns(), getTaskConfig());
  }

  protected List<File> getOfflineAvroFiles(List<File> avroFiles) {
    int numOfflineSegments = getNumOfflineSegments();
    List<File> offlineAvroFiles = new ArrayList<>(numOfflineSegments);
    for (int i = 0; i < numOfflineSegments; i++) {
      offlineAvroFiles.add(avroFiles.get(i));
    }
    return offlineAvroFiles;
  }

  protected List<File> getRealtimeAvroFiles(List<File> avroFiles) {
    int numSegments = avroFiles.size();
    int numRealtimeSegments = getNumRealtimeSegments();
    List<File> realtimeAvroFiles = new ArrayList<>(numRealtimeSegments);
    for (int i = numSegments - numRealtimeSegments; i < numSegments; i++) {
      realtimeAvroFiles.add(avroFiles.get(i));
    }
    return realtimeAvroFiles;
  }

  @Test
  public void testBrokerDebugOutput()
      throws Exception {
    String tableName = getTableName();
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary"));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
  }

  @Test
  @Override
  public void testQueriesFromQueryFile()
      throws Exception {
    super.testQueriesFromQueryFile();
  }

  @Test
  @Override
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    super.testGeneratedQueriesWithMultiValues();
  }

  @Test
  @Override
  public void testInstanceShutdown()
      throws Exception {
    super.testInstanceShutdown();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Try deleting the tables and check that they have no routing table
    final String tableName = getTableName();
    dropOfflineTable(tableName);
    dropRealtimeTable(tableName);

    // Routing table should not have any entries (length = 0) after deleting all tables
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          JSONArray routingTableSnapshot =
              getDebugInfo("debug/routingTable/" + tableName).getJSONArray("routingTableSnapshot");
          return routingTableSnapshot.length() == 0;
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, "Routing table is not empty after dropping all tables");

    stopServer();
    stopBroker();
    stopController();
    KafkaStarterUtils.stopServer(_kafkaStarter);
    stopZk();
    cleanup();
  }

  /**
   * Can be overridden to preserve segments.
   *
   * @throws Exception
   */
  protected void cleanup()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }
}
