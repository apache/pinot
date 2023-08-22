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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
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

  @Override
  protected String getBrokerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected String getServerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, false);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startHybridCluster();

    List<File> avroFiles = getAllAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_REALTIME_SEGMENTS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir,
        _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void startHybridCluster()
      throws Exception {
    // Start Zk and Kafka
    startZk();
    startKafka();

    // Start the Pinot cluster
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);

    startController(properties);

    startBroker();
    startServers(2);

    // Create tenants
    createBrokerTenant(TENANT_NAME, 1);
    createServerTenant(TENANT_NAME, 1, 1);
  }

  @Test
  public void testSegmentMetadataApi()
      throws Exception {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName()));
    JsonNode tableSegmentsMetadata = JsonUtils.stringToJsonNode(jsonOutputStr);
    Assert.assertEquals(tableSegmentsMetadata.size(), 8);

    JsonNode segmentMetadataFromAllEndpoint = tableSegmentsMetadata.elements().next();
    String segmentName = segmentMetadataFromAllEndpoint.get("segmentName").asText();
    jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(getTableName(), segmentName));
    JsonNode segmentMetadataFromDirectEndpoint = JsonUtils.stringToJsonNode(jsonOutputStr);
    Assert.assertEquals(segmentMetadataFromAllEndpoint.get("totalDocs"),
        segmentMetadataFromDirectEndpoint.get("segment.total.docs"));
  }

  @Test
  public void testSegmentListApi()
      throws Exception {
    {
      String jsonOutputStr =
          sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName(), TableType.OFFLINE.toString()));
      JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
      // There should be one element in the array
      JsonNode element = array.get(0);
      JsonNode segments = element.get("OFFLINE");
      Assert.assertEquals(segments.size(), 8);
    }
    {
      String jsonOutputStr =
          sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName(), TableType.REALTIME.toString()));
      JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
      // There should be one element in the array
      JsonNode element = array.get(0);
      JsonNode segments = element.get("REALTIME");
      Assert.assertEquals(segments.size(), 24);
    }
    {
      String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName()));
      JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
      JsonNode offlineSegments = array.get(0).get("OFFLINE");
      Assert.assertEquals(offlineSegments.size(), 8);
      JsonNode realtimeSegments = array.get(1).get("REALTIME");
      Assert.assertEquals(realtimeSegments.size(), 24);
    }
  }

  // NOTE: Reload consuming segment will force commit it, so run this test after segment list api test
  @Test(dependsOnMethods = "testSegmentListApi")
  public void testReload()
      throws Exception {
    super.testReload(true);
  }

  @Test
  public void testBrokerDebugOutput()
      throws Exception {
    String tableName = getTableName();
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/timeBoundary/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + tableName));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
    Assert.assertNotNull(getDebugInfo("debug/routingTable/" + TableNameBuilder.REALTIME.tableNameWithType(tableName)));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testBrokerDebugRoutingTableSQL(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String encodedSQL;
    encodedSQL = URLEncoder.encode("select * from " + realtimeTableName, "UTF-8");
    Assert.assertNotNull(getDebugInfo("debug/routingTable/sql?query=" + encodedSQL));
    encodedSQL = URLEncoder.encode("select * from " + offlineTableName, "UTF-8");
    Assert.assertNotNull(getDebugInfo("debug/routingTable/sql?query=" + encodedSQL));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryTracing(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode jsonNode = postQuery("SET trace = true; SELECT COUNT(*) FROM " + getTableName());
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), getCountStarResult());
    Assert.assertTrue(jsonNode.get("exceptions").isEmpty());
    JsonNode traceInfo = jsonNode.get("traceInfo");
    Assert.assertEquals(traceInfo.size(), 2);
    Assert.assertTrue(traceInfo.has("localhost_O"));
    Assert.assertTrue(traceInfo.has("localhost_R"));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryTracingWithLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode jsonNode =
        postQuery("SET trace = true; SELECT 1, \'test\', ArrDelay FROM " + getTableName() + " LIMIT 10");
    long countStarResult = 10;
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    for (int rowId = 0; rowId < 10; rowId++) {
      Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(rowId).get(0).asLong(), 1);
      Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(rowId).get(1).asText(), "test");
    }
    Assert.assertTrue(jsonNode.get("exceptions").isEmpty());
    JsonNode traceInfo = jsonNode.get("traceInfo");
    Assert.assertEquals(traceInfo.size(), 2);
    Assert.assertTrue(traceInfo.has("localhost_O"));
    Assert.assertTrue(traceInfo.has("localhost_R"));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDropResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    final String query = String.format("SELECT * FROM %s limit 10", getTableName());
    final String resultTag = "resultTable";

    // dropResults=true - resultTable must not be in the response
    Assert.assertFalse(postQueryWithOptions(query, "dropResults=true").has(resultTag));

    // dropResults=TrUE (case insensitive match) - resultTable must not be in the response
    Assert.assertFalse(postQueryWithOptions(query, "dropResults=TrUE").has(resultTag));

    // dropResults=truee - (anything other than true, is taken as false) - resultTable must be in the response
    Assert.assertTrue(postQueryWithOptions(query, "dropResults=truee").has(resultTag));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardcodedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testHardcodedQueries();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueriesFromQueryFile(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testQueriesFromQueryFile();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGeneratedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testGeneratedQueries();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryExceptions(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testQueryExceptions();
  }

  @Test
  @Override
  public void testInstanceShutdown()
      throws Exception {
    super.testInstanceShutdown();
  }

  @Test
  @Override
  public void testBrokerResponseMetadata()
      throws Exception {
    super.testBrokerResponseMetadata();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testVirtualColumnQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    super.testVirtualColumnQueries();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Try deleting the tables and check that they have no routing table
    String tableName = getTableName();
    dropOfflineTable(tableName);
    dropRealtimeTable(tableName);

    // Routing should be removed after deleting all tables
    TestUtils.waitForCondition(aVoid -> {
      try {
        getDebugInfo("debug/routingTable/" + tableName);
        return false;
      } catch (Exception e) {
        // only return true if 404 not found error is thrown.
        return e.getMessage().contains("Got error status code: 404");
      }
    }, 60_000L, "Routing table is not empty after dropping all tables");

    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    cleanupHybridCluster();
  }

  /**
   * Can be overridden to preserve segments.
   *
   * @throws Exception
   */
  protected void cleanupHybridCluster()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }
}
