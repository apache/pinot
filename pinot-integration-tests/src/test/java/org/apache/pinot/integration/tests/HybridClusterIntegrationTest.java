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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;


public class HybridClusterIntegrationTest extends BaseHybridClusterIntegrationTest {
  private static final int NUM_SHARED_OFFLINE_SEGMENTS = 8;
  private static final int NUM_SHARED_REALTIME_SEGMENTS = 6;

  private static final String SHARED_TABLE_NAME_PREFIX = "hybrid_cluster";
  private static final String SHARED_KAFKA_TOPIC_PREFIX = "hybrid_cluster";
  private static final String SHARED_TENANT_NAME_PREFIX = "HybridClusterTenant";

  private final String _sharedResourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);
  private final Map<String, String> _originalClusterConfigValues = new LinkedHashMap<>();

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;
  private boolean _clusterConfigOverrideApplied;

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX + "_" + _sharedResourceSuffix
        : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC_PREFIX + "_" + _sharedResourceSuffix
        : super.getKafkaTopic();
  }

  @Override
  protected String getBrokerTenant() {
    return isSharedRichClusterEnabled() ? SHARED_TENANT_NAME_PREFIX : super.getBrokerTenant();
  }

  @Override
  protected String getServerTenant() {
    return isSharedRichClusterEnabled() ? SHARED_TENANT_NAME_PREFIX : super.getServerTenant();
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_INSTANCE_TAGS,
        TagNameUtils.getBrokerTagForTenant(getBrokerTenant()));
  }

  @BeforeClass(alwaysRun = true)
  @Override
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = getClassSegmentDir();
    _classTarDir = getClassTarDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startHybridCluster();
    cleanTableAndSchema();
    createServerTenant(getServerTenant(), NUM_SERVERS_OFFLINE, NUM_SERVERS_REALTIME);
    resetKafkaTopic();

    List<File> avroFiles = getClassAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_SHARED_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_SHARED_REALTIME_SEGMENTS);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0,
        _classSegmentDir, _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    pushAvroIntoKafka(realtimeAvroFiles);
    setUpH2Connection(avroFiles);
    setUpQueryGenerator(avroFiles);
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void startHybridCluster()
      throws Exception {
    startZk();
    startController();
    applyClusterConfigOverrides();
    startBroker();
    startServers(NUM_SERVERS);
    startKafkaWithoutTopic();
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreClusterConfigOverrides);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    if (!isSharedRichClusterEnabled()) {
      exception = runCleanup(exception, this::stopServer);
      exception = runCleanup(exception, this::stopBroker);
      exception = runCleanup(exception, this::stopController);
      exception = runCleanup(exception, this::stopKafka);
      exception = runCleanup(exception, this::stopZk);
    }
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected void testQuery(@Language("sql") String query)
      throws Exception {
    super.testQuery(rewriteDefaultTableName(query));
  }

  @Override
  protected void testQuery(@Language("sql") String pinotQuery, @Language("sql") String h2Query)
      throws Exception {
    super.testQuery(rewriteDefaultTableName(pinotQuery), rewriteDefaultTableName(h2Query));
  }

  private String rewriteDefaultTableName(String query) {
    return isSharedRichClusterEnabled() ? query.replace(DEFAULT_TABLE_NAME, getTableName()) : query;
  }

  @Test
  public void testUpdateBrokerResource()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      throw new SkipException("Skipping broker add/drop coverage in shared rich cluster mode");
    }

    // Add a new broker to the cluster
    BaseBrokerStarter brokerStarter = startOneBroker(1);

    // Check if broker is added to all the tables in broker resource
    String clusterName = getHelixClusterName();
    String brokerId = brokerStarter.getInstanceId();
    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertEquals(brokerAssignment.get(brokerId), CommonConstants.Helix.StateModel.BrokerResourceStateModel.ONLINE);
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (!brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to find broker in broker resource ExternalView");

    // Stop the broker
    brokerStarter.stop();
    _brokerPorts.remove(_brokerPorts.size() - 1);

    // Dropping the broker should fail because it is still in the broker resource
    try {
      getOrCreateAdminClient().getInstanceClient().dropInstance(brokerId);
      fail("Dropping instance should fail because it is still in the broker resource");
    } catch (Exception e) {
      // Expected
    }

    // Untag the broker and update the broker resource so that it is removed from the broker resource
    getOrCreateAdminClient().getInstanceClient().updateInstanceTags(brokerId, Collections.emptyList(), true);

    // Check if broker is removed from all the tables in broker resource
    brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertFalse(brokerAssignment.containsKey(brokerId));
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to remove broker from broker resource ExternalView");

    // Dropping the broker should success now
    getOrCreateAdminClient().getInstanceClient().dropInstance(brokerId);

    // Check if broker is dropped from the cluster
    assertFalse(_helixAdmin.getInstancesInCluster(clusterName).contains(brokerId));
  }

  @Test
  public void testSegmentMetadataApi()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    {
      String jsonOutputStr = getOrCreateAdminClient().getSegmentClient()
          .getSegmentsMetadata(getTableName(), null, null, TableType.OFFLINE.toString());
      JsonNode tableSegmentsMetadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(tableSegmentsMetadata.size(), 8);

      JsonNode segmentMetadataFromAllEndpoint = tableSegmentsMetadata.elements().next();
      String segmentName = segmentMetadataFromAllEndpoint.get("segmentName").asText();
      Map<String, Object> segmentMetadataFromDirectEndpoint = getOrCreateAdminClient().getSegmentClient()
          .getSegmentMetadata(offlineTableName, segmentName, null);
      Assert.assertEquals(segmentMetadataFromAllEndpoint.get("totalDocs"),
          JsonUtils.objectToJsonNode(segmentMetadataFromDirectEndpoint).get("segment.total.docs"));
    }
    // get list of segment names to pass in query params for following tests
    List<String> segments = getSegmentNames(getTableName(), TableType.OFFLINE.toString());
    // with null column params
    {
      String jsonOutputStr = getOrCreateAdminClient().getSegmentClient()
          .getSegmentsMetadata(getTableName(), null, segments, TableType.OFFLINE.toString());
      JsonNode tableSegmentsMetadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(tableSegmentsMetadata.size(), segments.size());
      JsonNode segmentMetadataFromAllEndpoint = tableSegmentsMetadata.elements().next();
      String segmentName = segmentMetadataFromAllEndpoint.get("segmentName").asText();
      Map<String, Object> segmentMetadataFromDirectEndpoint = getOrCreateAdminClient().getSegmentClient()
          .getSegmentMetadata(offlineTableName, segmentName, null);
      Assert.assertEquals(segmentMetadataFromAllEndpoint.get("totalDocs"),
          JsonUtils.objectToJsonNode(segmentMetadataFromDirectEndpoint).get("segment.total.docs"));
      Assert.assertEquals(tableSegmentsMetadata.get(segments.get(0)).get("columns").size(), 0);
    }
    // with * column param
    {
      String jsonOutputStr = getOrCreateAdminClient().getSegmentClient()
          .getSegmentsMetadata(getTableName(), List.of("*"), segments, TableType.OFFLINE.toString());
      JsonNode tableSegmentsMetadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(tableSegmentsMetadata.size(), segments.size());
      JsonNode segmentMetadataFromAllEndpoint = tableSegmentsMetadata.elements().next();
      String segmentName = segmentMetadataFromAllEndpoint.get("segmentName").asText();
      Map<String, Object> segmentMetadataFromDirectEndpoint = getOrCreateAdminClient().getSegmentClient()
          .getSegmentMetadata(offlineTableName, segmentName, null);
      Assert.assertEquals(segmentMetadataFromAllEndpoint.get("totalDocs"),
          JsonUtils.objectToJsonNode(segmentMetadataFromDirectEndpoint).get("segment.total.docs"));
      Assert.assertEquals(tableSegmentsMetadata.get(segments.get(0)).get("columns").size(), 79);
    }
    // with specified column params
    {
      List<String> columns = List.of("Carrier", "FlightNum", "TailNum");
      String jsonOutputStr = getOrCreateAdminClient().getSegmentClient()
          .getSegmentsMetadata(getTableName(), columns, segments, TableType.OFFLINE.toString());
      JsonNode tableSegmentsMetadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      Assert.assertEquals(tableSegmentsMetadata.size(), segments.size());
      JsonNode segmentMetadataFromAllEndpoint = tableSegmentsMetadata.elements().next();
      String segmentName = segmentMetadataFromAllEndpoint.get("segmentName").asText();
      Map<String, Object> segmentMetadataFromDirectEndpoint = getOrCreateAdminClient().getSegmentClient()
          .getSegmentMetadata(offlineTableName, segmentName, null);
      Assert.assertEquals(segmentMetadataFromAllEndpoint.get("totalDocs"),
          JsonUtils.objectToJsonNode(segmentMetadataFromDirectEndpoint).get("segment.total.docs"));
      Assert.assertEquals(tableSegmentsMetadata.get(segments.get(0)).get("columns").size(), columns.size());
    }
  }

  @Test
  public void testSegmentListApi()
      throws Exception {
    List<String> offlineSegments =
        getOrCreateAdminClient().getSegmentClient().listSegments(getTableName(), TableType.OFFLINE.toString(), false);
    Assert.assertEquals(offlineSegments.size(), 8);

    List<String> realtimeSegments =
        getOrCreateAdminClient().getSegmentClient().listSegments(getTableName(), TableType.REALTIME.toString(), false);
    Assert.assertEquals(realtimeSegments.size(), 24);

    Assert.assertEquals(offlineSegments.size(), 8);
    Assert.assertEquals(realtimeSegments.size(), 24);
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
    encodedSQL = URIUtils.encode("select * from " + realtimeTableName);
    Assert.assertNotNull(getDebugInfo("debug/routingTable/sql?query=" + encodedSQL));
    encodedSQL = URIUtils.encode("select * from " + offlineTableName);
    Assert.assertNotNull(getDebugInfo("debug/routingTable/sql?query=" + encodedSQL));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryTracing(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Tracing is a v1 only concept and the v2 query engine has separate multi-stage stats that are enabled by default
    notSupportedInV2();
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
    // Tracing is a v1 only concept and the v2 query engine has separate multi-stage stats that are enabled by default
    notSupportedInV2();
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
    JsonNode jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertFalse(jsonNode.has(resultTag));

    // dropResults=TrUE (case insensitive match) - resultTable must not be in the response
    Assert.assertFalse(postQueryWithOptions(query, "dropResults=TrUE").has(resultTag));

    // dropResults=truee - (anything other than true, is taken as false) - resultTable must be in the response
    Assert.assertTrue(postQueryWithOptions(query, "dropResults=truee").has(resultTag));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainDropResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String resultTag = "resultTable";
    String query = String.format("EXPLAIN PLAN FOR SELECT * FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    JsonNode jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));
    query = String.format("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR SELECT * FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));

    query = String.format("EXPLAIN IMPLEMENTATION PLAN FOR SELECT * FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));

    query = String.format("EXPLAIN PLAN FOR SELECT 1 + 1 FROM %s limit 10", getTableName());

    // dropResults=true - resultTable must be in the response (it is a query plan)
    jsonNode = postQueryWithOptions(query, "dropResults=true");
    Assert.assertTrue(jsonNode.has(resultTag));
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
    // Some of the hardcoded queries in the query file need to be adapted for v2 (for instance, using the arrayToMV
    // with multi-value columns in filters / aggregations)
    notSupportedInV2();
    super.testQueriesFromQueryFile();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGeneratedQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testGeneratedQueries(true, useMultiStageQueryEngine);
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
    if (isSharedRichClusterEnabled()) {
      ResultSetGroup resultSetGroup = getPinotConnection().execute("select * from " + getTableName());
      for (int i = 0; i < resultSetGroup.getResultSet(0).getColumnCount(); i++) {
        assertFalse(resultSetGroup.getResultSet(0).getColumnName(i).startsWith("$"));
      }
      getPinotConnection()
          .execute("select $docId, $segmentName, $hostName, $partitionId from " + getTableName());
      getPinotConnection().execute(
          "select $docId, $segmentName, $hostName, $partitionId from " + getTableName()
              + " where $docId < 5 limit 50");
      getPinotConnection().execute(
          "select $docId, $segmentName, $hostName, $partitionId from " + getTableName()
              + " where $docId = 5 limit 50");
      getPinotConnection().execute(
          "select $docId, $segmentName, $hostName, $partitionId from " + getTableName()
              + " where $docId > 19998 limit 50");
      return;
    }
    super.testVirtualColumnQueries();
  }

  @Test(dataProvider = "useBothQueryEngines")
  void testControllerQuerySubmit(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Hybrid Table
    @Language("sql")
    String query = "SELECT count(*) FROM " + getTableName();
    JsonNode response = postQueryToController(query);
    assertNoError(response);

    // Offline table
    String tableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    query = "SELECT count(*) FROM " + tableName;
    response = postQueryToController(query);
    assertNoError(response);

    tableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    query = "SELECT count(*) FROM " + tableName;
    response = postQueryToController(query);
    assertNoError(response);

    query = "SELECT count(*) FROM unknown";
    response = postQueryToController(query);
    if (useMultiStageQueryEngine) {
      QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
          .containsMessage("TableDoesNotExistError");
    } else {
      QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.BROKER_RESOURCE_MISSING)
          .containsMessage("BrokerResourceMissingError");
    }
  }

  @Test
  void testControllerJoinQuerySubmit()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // Hybrid Table
    @Language("sql")
    String query = "SELECT count(*) FROM unknown JOIN " + getTableName()
        + " ON unknown.FlightNum = " + getTableName() + ".FlightNum";
    JsonNode response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");

    query = "SELECT count(*) FROM unknown_1 JOIN unknown_2  ON "
        + "unknown_1.FlightNum = unknown_2.FlightNum";
    response = postQueryToController(query);
    QueryAssert.assertThat(response).firstException().hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST)
        .containsMessage("TableDoesNotExistError");
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _sharedResourceSuffix)
        : _tempDir;
  }

  private File getClassSegmentDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "segmentDir") : _segmentDir;
  }

  private File getClassTarDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "tarDir") : _tarDir;
  }

  private List<File> getClassAvroFiles()
      throws Exception {
    int numSegments = unpackAvroData(_classTempDir).size();
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_classTempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    return avroFiles;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(offlineTableName) != null
        || _helixResourceManager.hasOfflineTable(tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(tableName)) {
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }

    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  private void applyClusterConfigOverrides() {
    HelixConfigScope scope = getClusterConfigScope();
    Map<String, String> configOverrides = new LinkedHashMap<>();
    configOverrides.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(10));
    configOverrides.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        Integer.toString(6));
    configOverrides.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM, Integer.toString(12));

    try {
      _clusterConfigOverrideApplied = true;
      configOverrides.forEach((key, value) -> {
        _originalClusterConfigValues.put(key, _helixManager.getConfigAccessor().get(scope, key));
        _helixManager.getConfigAccessor().set(scope, key, value);
      });
    } catch (RuntimeException e) {
      restoreClusterConfigOverrides();
      throw e;
    }
  }

  private void restoreClusterConfigOverrides() {
    if ((!_clusterConfigOverrideApplied && _originalClusterConfigValues.isEmpty()) || _helixManager == null) {
      return;
    }

    HelixConfigScope scope = getClusterConfigScope();
    _originalClusterConfigValues.forEach((key, originalValue) -> {
      if (originalValue == null) {
        _helixManager.getConfigAccessor().remove(scope, key);
      } else {
        _helixManager.getConfigAccessor().set(scope, key, originalValue);
      }
    });
    _originalClusterConfigValues.clear();
    _clusterConfigOverrideApplied = false;
  }

  private HelixConfigScope getClusterConfigScope() {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
        .build();
  }

  private void closeH2Connection()
      throws Exception {
    if (_h2Connection != null) {
      _h2Connection.close();
      _h2Connection = null;
    }
    _queryGenerator = null;
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private void deleteClassTempDir()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
