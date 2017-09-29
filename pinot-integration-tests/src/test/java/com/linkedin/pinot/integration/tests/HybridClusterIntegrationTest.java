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
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.controller.ControllerConf;
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
import org.json.JSONObject;
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
  private Schema _schema;

  protected int getNumOfflineSegments() {
    return NUM_OFFLINE_SEGMENTS;
  }

  protected int getNumRealtimeSegments() {
    return NUM_REALTIME_SEGMENTS;
  }

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    List<File> avroFiles = getAllAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles);

    ExecutorService executor = Executors.newCachedThreadPool();

    // Create segments from Avro data
    File schemaFile = getSchemaFile();
    _schema = Schema.fromFile(schemaFile);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, 0, _segmentDir, _tarDir, getTableName(), false,
        getRawIndexColumns(), _schema, executor);

    // Push data into the Kafka topic
    pushAvroIntoKafka(realtimeAvroFiles, getKafkaTopic(), executor);

    // Load data into H2
    setUpH2Connection(avroFiles, executor);

    // Initialize query generator
    setUpQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create Pinot table
    setUpTable(avroFiles.get(0));

    // Upload all segments
    uploadSegments(_tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void startHybridCluster() throws Exception {
    // Start Zk and Kafka
    startZk();
    _kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    // Create Kafka topic
    KafkaStarterUtils.createTopic(getKafkaTopic(), KafkaStarterUtils.DEFAULT_ZK_STR, getNumKafkaPartitions());

    // Start the Pinot cluster
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTenantIsolationEnabled(false);
    startController(config);
    startBroker();
    startServers(2);

    // Create tenants
    createBrokerTenant(TENANT_NAME, 1);
    createServerTenant(TENANT_NAME, 1, 1);
  }

  protected void setUpTable(File avroFile) throws Exception {
    String schemaName = _schema.getSchemaName();
    addSchema(getSchemaFile(), schemaName);

    String timeColumnName = _schema.getTimeColumnName();
    Assert.assertNotNull(timeColumnName);
    TimeUnit outgoingTimeUnit = _schema.getOutgoingTimeUnit();
    Assert.assertNotNull(outgoingTimeUnit);
    String timeType = outgoingTimeUnit.toString();

    addHybridTable(getTableName(), useLlc(), KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KafkaStarterUtils.DEFAULT_ZK_STR,
        getKafkaTopic(), getRealtimeSegmentFlushSize(), avroFile, timeColumnName, timeType, schemaName, TENANT_NAME,
        TENANT_NAME, getLoadMode(), getSortedColumn(), getInvertedIndexColumns(), getRawIndexColumns(),
        getTaskConfig());
  }

  protected List<File> getAllAvroFiles() throws Exception {
    // Unpack the Avro files
    int numSegments = unpackAvroData(_tempDir).size();

    // Avro files has to be ordered as time series data
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_tempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }

    return avroFiles;
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
  public void testSegmentListApi() throws Exception {
    {
      String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
          forSegmentListAPIWithTableType(getTableName(), CommonConstants.Helix.TableType.OFFLINE.toString()));
      JSONArray array = new JSONArray(jsonOutputStr);
      // There should be one element in the array
      JSONObject element = (JSONObject) array.get(0);
      JSONArray segments = (JSONArray) element.get("OFFLINE");
      Assert.assertEquals(segments.length(), 8);
    }
    {
      String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
          forSegmentListAPIWithTableType(getTableName(), CommonConstants.Helix.TableType.REALTIME.toString()));
      JSONArray array = new JSONArray(jsonOutputStr);
      // There should be one element in the array
      JSONObject element = (JSONObject) array.get(0);
      JSONArray segments = (JSONArray) element.get("REALTIME");
      Assert.assertEquals(segments.length(), 3);
    }
    {
      String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder. forSegmentListAPI(getTableName()));
      JSONArray array = new JSONArray(jsonOutputStr);
      // there should be 2 elements in the array now.
      int realtimeIndex = 0;
      int offlineIndex = 1;
      JSONObject element = (JSONObject) array.get(realtimeIndex);
      if (!element.has("REALTIME")) {
        realtimeIndex = 1;
        offlineIndex = 0;
      }
      JSONObject offlineElement = (JSONObject)array.get(offlineIndex);
      JSONObject realtimeElement = (JSONObject)array.get(realtimeIndex);

      JSONArray realtimeSegments = (JSONArray) realtimeElement.get("REALTIME");
      Assert.assertEquals(realtimeSegments.length(), 3);

      JSONArray offlineSegments = (JSONArray) offlineElement.get("OFFLINE");
      Assert.assertEquals(offlineSegments.length(), 8);
    }
  }

  @Test
  public void testBrokerDebugOutput() throws Exception {
    String tableName = getTableName();
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
  }

  @Test
  @Override
  public void testQueriesFromQueryFile() throws Exception {
    super.testQueriesFromQueryFile();
  }

  @Test
  @Override
  public void testGeneratedQueriesWithMultiValues() throws Exception {
    super.testGeneratedQueriesWithMultiValues();
  }

  @Test
  @Override
  public void testQueryExceptions() throws Exception {
    super.testQueryExceptions();
  }

  @Test
  @Override
  public void testInstanceShutdown() throws Exception {
    super.testInstanceShutdown();
  }

  @AfterClass
  public void tearDown() throws Exception {
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
  protected void cleanup() throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }
}
