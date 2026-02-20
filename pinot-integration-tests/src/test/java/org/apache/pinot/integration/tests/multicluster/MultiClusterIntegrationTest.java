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
package org.apache.pinot.integration.tests.multicluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.broker.helix.MultiClusterHelixBrokerStarter;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.ClusterTest;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MultiClusterIntegrationTest extends ClusterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterIntegrationTest.class);

  protected static final String SCHEMA_FILE = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  protected static final String TIME_COLUMN = "DaysSinceEpoch";
  // TODO: N clusters instead of 2 in future iterations.
  protected static final String CLUSTER_1_NAME = "DualIsolatedCluster1";
  protected static final String CLUSTER_2_NAME = "DualIsolatedCluster2";
  protected static final ClusterConfig CLUSTER_1_CONFIG = new ClusterConfig(CLUSTER_1_NAME, 30000);
  protected static final ClusterConfig CLUSTER_2_CONFIG = new ClusterConfig(CLUSTER_2_NAME, 40000);
  protected static final String DEFAULT_TENANT = "DefaultTenant";
  protected static final String LOGICAL_TABLE_NAME = "logical_table";
  protected static final String LOGICAL_TABLE_NAME_2 = "logical_table_2";
  protected static final String LOGICAL_FEDERATION_CLUSTER_1_TABLE = "logical_federation_table_cluster1";
  protected static final String LOGICAL_FEDERATION_CLUSTER_2_TABLE = "logical_federation_table_cluster2";
  protected static final String LOGICAL_FEDERATION_CLUSTER_1_TABLE_2 = "logical_federation_table2_cluster1";
  protected static final String LOGICAL_FEDERATION_CLUSTER_2_TABLE_2 = "logical_federation_table2_cluster2";
  protected static final int TABLE_SIZE_CLUSTER_1 = 1500;
  protected static final int TABLE_SIZE_CLUSTER_2 = 1000;
  protected static final int SEGMENTS_PER_CLUSTER = 3;
  protected static final String JOIN_COLUMN = "OriginCityName";
  protected static final String UNAVAILABLE_CLUSTER_NAME = "UnavailableCluster";
  protected static final String UNAVAILABLE_ZK_ADDRESS = "localhost:29999";

  protected ClusterComponents _cluster1;
  protected ClusterComponents _cluster2;
  protected ClusterComponents _brokerWithUnavailableCluster;
  protected List<File> _cluster1AvroFiles;
  protected List<File> _cluster2AvroFiles;

  @BeforeClass
  public void setUp() throws Exception {
    LOGGER.info("Setting up MultiClusterIntegrationTest");

    // Initialize cluster components
    _cluster1 = new ClusterComponents();
    _cluster2 = new ClusterComponents();

    // Setup directories
    setupDirectories();

    // Start ZooKeeper instances for both clusters
    startZookeeper(_cluster1);
    startZookeeper(_cluster2);

    // Start controllers for both clusters
    startControllerInit(_cluster1, CLUSTER_1_CONFIG);
    startControllerInit(_cluster2, CLUSTER_2_CONFIG);

    // Start brokers and servers for both clusters
    // Note: Each cluster's broker is configured to know about the other cluster as remote
    startCluster(_cluster1, _cluster2, CLUSTER_1_CONFIG);
    startCluster(_cluster2, _cluster1, CLUSTER_2_CONFIG);

    // Start an alternate broker with one valid and one unavailable remote cluster
    startBrokerWithUnavailableCluster();

    LOGGER.info("MultiClusterIntegrationTest setup complete");
  }

  /**
   * Starts a broker configured with cluster2 (valid) and an unavailable cluster (invalid ZK).
   */
  private void startBrokerWithUnavailableCluster() throws Exception {
    _brokerWithUnavailableCluster = new ClusterComponents();
    _brokerWithUnavailableCluster._brokerPort = findAvailablePort(55000);

    PinotConfiguration brokerConfig = new PinotConfiguration();
    brokerConfig.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, _cluster1._zkUrl);
    brokerConfig.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, CLUSTER_1_NAME);
    brokerConfig.setProperty(Broker.CONFIG_OF_BROKER_HOSTNAME, ControllerTest.LOCAL_HOST);
    brokerConfig.setProperty(Helix.KEY_OF_BROKER_QUERY_PORT, _brokerWithUnavailableCluster._brokerPort);
    brokerConfig.setProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    brokerConfig.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    brokerConfig.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    brokerConfig.setProperty(Helix.CONFIG_OF_REMOTE_CLUSTER_NAMES,
        CLUSTER_2_NAME + "," + UNAVAILABLE_CLUSTER_NAME);
    brokerConfig.setProperty(String.format(Helix.CONFIG_OF_REMOTE_ZOOKEEPER_SERVERS, CLUSTER_2_NAME),
        _cluster2._zkUrl);
    brokerConfig.setProperty(String.format(Helix.CONFIG_OF_REMOTE_ZOOKEEPER_SERVERS, UNAVAILABLE_CLUSTER_NAME),
        UNAVAILABLE_ZK_ADDRESS);

    _brokerWithUnavailableCluster._brokerStarter = createBrokerStarter();
    _brokerWithUnavailableCluster._brokerStarter.init(brokerConfig);
    _brokerWithUnavailableCluster._brokerStarter.start();
    LOGGER.info("Started broker with unavailable cluster on port {}", _brokerWithUnavailableCluster._brokerPort);
  }

  // TODO: Add more tests for cross-cluster queries in subsequent iterations.
  @Test
  public void testMultiClusterBrokerStartsAndIsQueryable() throws Exception {
    LOGGER.info("Testing that multi-cluster broker starts successfully and is queryable");

    // Verify both clusters' brokers are running (MultiClusterHelixBrokerStarter)
    assertNotNull(_cluster1._brokerStarter, "Cluster 1 broker should be started");
    assertNotNull(_cluster2._brokerStarter, "Cluster 2 broker should be started");
    assertTrue(_cluster1._brokerStarter instanceof MultiClusterHelixBrokerStarter,
        "Cluster 1 broker should be MultiClusterHelixBrokerStarter");
    assertTrue(_cluster2._brokerStarter instanceof MultiClusterHelixBrokerStarter,
        "Cluster 2 broker should be MultiClusterHelixBrokerStarter");

    // Setup a test table on both clusters
    String testTableName = "multicluster_test_table";
    createSchemaAndTableOnBothClusters(testTableName);

    // Create and load test data into both clusters
    _cluster1AvroFiles = createAvroData(TABLE_SIZE_CLUSTER_1, 1);
    _cluster2AvroFiles = createAvroData(TABLE_SIZE_CLUSTER_2, 2);

    loadDataIntoCluster(_cluster1AvroFiles, testTableName, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, testTableName, _cluster2);

    // Verify cluster 1 is queryable
    String query = "SELECT COUNT(*) FROM " + testTableName;
    String result1 = executeQuery(query, _cluster1);
    assertNotNull(result1, "Query result from cluster 1 should not be null");
    long count1 = parseCountResult(result1);
    assertEquals(count1, TABLE_SIZE_CLUSTER_1);

    // Verify cluster 2 is queryable
    String result2 = executeQuery(query, _cluster2);
    assertNotNull(result2, "Query result from cluster 2 should not be null");
    long count2 = parseCountResult(result2);
    assertEquals(count2, TABLE_SIZE_CLUSTER_2);

    LOGGER.info("Multi-cluster broker test passed: both clusters started and queryable");
  }

  @BeforeGroups("query")
  public void setupTablesForQueryTests() throws Exception {
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster2._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster2._controllerBaseApiUrl);
    setupFirstLogicalFederatedTable();
    setupSecondLogicalFederatedTable();
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME,
      LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE);
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME_2,
      LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2);
    cleanSegmentDirs();
    loadDataIntoCluster(createAvroData(TABLE_SIZE_CLUSTER_1, 1), LOGICAL_FEDERATION_CLUSTER_1_TABLE, _cluster1);
    loadDataIntoCluster(createAvroData(TABLE_SIZE_CLUSTER_2, 2), LOGICAL_FEDERATION_CLUSTER_2_TABLE, _cluster2);
    loadDataIntoCluster(createAvroDataMultipleSegments(TABLE_SIZE_CLUSTER_1, 1, SEGMENTS_PER_CLUSTER),
      LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, _cluster1);
    loadDataIntoCluster(createAvroDataMultipleSegments(TABLE_SIZE_CLUSTER_2, 2, SEGMENTS_PER_CLUSTER),
      LOGICAL_FEDERATION_CLUSTER_2_TABLE_2, _cluster2);
  }

  @Test(dataProvider = "queryModes", groups = "query")
  public void testLogicalFederationQueries(String testName, String queryOptions, boolean isJoinQuery,
      int brokerPort, boolean expectUnavailableException)
      throws Exception {
    LOGGER.info("Running {} on broker port {} (expectUnavailableException={})",
        testName, brokerPort, expectUnavailableException);
    long expectedTotal = TABLE_SIZE_CLUSTER_1 + TABLE_SIZE_CLUSTER_2;

    if (isJoinQuery) {
      // Join query test
      String joinQuery = queryOptions
          + "SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count FROM " + LOGICAL_TABLE_NAME + " t1 "
          + "JOIN " + LOGICAL_TABLE_NAME_2 + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
          + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";
      String result = executeQueryOnBrokerPort(joinQuery, brokerPort);
      assertNotNull(result);
      assertTrue(result.contains("resultTable"), "Expected resultTable in response: " + result);
      assertResultRows(result);
      verifyUnavailableClusterException(result, expectUnavailableException);
    }

    // Count query test (all modes)
    String countQuery = queryOptions + "SELECT COUNT(*) as count FROM " + LOGICAL_TABLE_NAME;
    String countResult = executeQueryOnBrokerPort(countQuery, brokerPort);
    assertEquals(parseCountResult(countResult), expectedTotal);
    verifyUnavailableClusterException(countResult, expectUnavailableException);
  }

  /**
   * Data provider for all query mode combinations: broker mode x query engine/options.
   * Each test case has: testName, queryOptions, isJoinQuery, brokerPort, expectUnavailableException
   */
  @DataProvider(name = "queryModes")
  public Object[][] queryModes() {
    int normalBroker = _cluster1._brokerPort;
    int unavailableBroker = _brokerWithUnavailableCluster._brokerPort;

    String sseOpts = "SET enableMultiClusterRouting=true; ";
    String mseOpts = sseOpts + "SET useMultistageEngine=true; ";
    String physOptOpts = mseOpts + "SET usePhysicalOptimizer=true; ";
    String mseLiteOpts = physOptOpts + "SET runInBroker=true; ";

    return new Object[][]{
        // SSE tests (count only)
        {"SSE-NormalBroker", sseOpts, false, normalBroker, false},
        {"SSE-UnavailableBroker", sseOpts, false, unavailableBroker, true},
        // MSE tests (join + count)
        {"MSE-NormalBroker", mseOpts, true, normalBroker, false},
        {"MSE-UnavailableBroker", mseOpts, true, unavailableBroker, true},
        // Physical optimizer tests (join + count)
        {"PhysicalOptimizer-NormalBroker", physOptOpts, true, normalBroker, false},
        {"PhysicalOptimizer-UnavailableBroker", physOptOpts, true, unavailableBroker, true},
        // MSELiteMode tests (join + count)
        {"MSELiteMode-NormalBroker", mseLiteOpts, true, normalBroker, false},
        {"MSELiteMode-UnavailableBroker", mseLiteOpts, true, unavailableBroker, true},
    };
  }

  @Override
  protected BaseBrokerStarter createBrokerStarter() {
    return new MultiClusterHelixBrokerStarter();
  }

  protected static class ClusterConfig {
    final String _name;
    final int _basePort;

    ClusterConfig(String name, int basePort) {
      _name = name;
      _basePort = basePort;
    }
  }

  protected static class ClusterComponents {
    ZkStarter.ZookeeperInstance _zkInstance;
    BaseControllerStarter _controllerStarter;
    BaseBrokerStarter _brokerStarter;
    BaseServerStarter _serverStarter;
    int _controllerPort;
    int _brokerPort;
    int _serverPort;
    String _zkUrl;
    String _controllerBaseApiUrl;
    File _tempDir;
    File _segmentDir;
    File _tarDir;
  }

  protected void setupDirectories() throws Exception {
    setupClusterDirectories(_cluster1, "cluster1");
    setupClusterDirectories(_cluster2, "cluster2");
  }

  private void setupClusterDirectories(ClusterComponents cluster, String clusterPrefix) throws Exception {
    cluster._tempDir = new File(FileUtils.getTempDirectory(), clusterPrefix + "_" + getClass().getSimpleName());
    cluster._segmentDir = new File(cluster._tempDir, "segmentDir");
    cluster._tarDir = new File(cluster._tempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(cluster._tempDir, cluster._segmentDir, cluster._tarDir);
  }

  protected void startZookeeper(ClusterComponents cluster) throws Exception {
    cluster._zkInstance = ZkStarter.startLocalZkServer();
    cluster._zkUrl = cluster._zkInstance.getZkUrl();
  }

  protected void startControllerInit(ClusterComponents cluster, ClusterConfig config) throws Exception {
    cluster._controllerPort = findAvailablePort(config._basePort);
    startController(cluster, config);
  }

  protected void startCluster(ClusterComponents cluster, ClusterComponents remoteCluster,
      ClusterConfig config) throws Exception {
    cluster._brokerPort = findAvailablePort(cluster._controllerPort + 1000);
    startBroker(cluster, remoteCluster, config);
    cluster._serverPort = findAvailablePort(cluster._brokerPort + 1000);
    startServerWithMSE(cluster, config);
  }

  protected void startController(ClusterComponents cluster, ClusterConfig config) throws Exception {
    Map<String, Object> controllerConfig = new HashMap<>();
    controllerConfig.put(ControllerConf.ZK_STR, cluster._zkUrl);
    controllerConfig.put(ControllerConf.HELIX_CLUSTER_NAME, config._name);
    controllerConfig.put(ControllerConf.CONTROLLER_HOST, ControllerTest.LOCAL_HOST);
    controllerConfig.put(ControllerConf.CONTROLLER_PORT, cluster._controllerPort);
    controllerConfig.put(ControllerConf.DATA_DIR, cluster._tempDir.getAbsolutePath());
    controllerConfig.put(ControllerConf.LOCAL_TEMP_DIR, cluster._tempDir.getAbsolutePath());
    controllerConfig.put(ControllerConf.DISABLE_GROOVY, false);
    controllerConfig.put(ControllerConf.CONSOLE_SWAGGER_ENABLE, false);
    controllerConfig.put(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");

    cluster._controllerStarter = createControllerStarter();
    cluster._controllerStarter.init(new PinotConfiguration(controllerConfig));
    cluster._controllerStarter.start();
    cluster._controllerBaseApiUrl = "http://localhost:" + cluster._controllerPort;
  }

  protected void startBroker(ClusterComponents cluster, ClusterComponents remoteCluster,
      ClusterConfig config) throws Exception {
    PinotConfiguration brokerConfig = new PinotConfiguration();
    brokerConfig.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, cluster._zkUrl);
    String remoteClusterName = CLUSTER_1_NAME.equalsIgnoreCase(config._name) ? CLUSTER_2_NAME : CLUSTER_1_NAME;
    brokerConfig.setProperty(Helix.CONFIG_OF_REMOTE_CLUSTER_NAMES, remoteClusterName);
    brokerConfig.setProperty(String.format(Helix.CONFIG_OF_REMOTE_ZOOKEEPER_SERVERS, remoteClusterName),
        remoteCluster._zkUrl);
    brokerConfig.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, config._name);
    brokerConfig.setProperty(Broker.CONFIG_OF_BROKER_HOSTNAME, ControllerTest.LOCAL_HOST);
    brokerConfig.setProperty(Helix.KEY_OF_BROKER_QUERY_PORT, cluster._brokerPort);
    brokerConfig.setProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    brokerConfig.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    brokerConfig.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    cluster._brokerStarter = createBrokerStarter();
    cluster._brokerStarter.init(brokerConfig);
    cluster._brokerStarter.start();
  }

  protected void startServerWithMSE(ClusterComponents cluster, ClusterConfig config) throws Exception {
    PinotConfiguration serverConfig = new PinotConfiguration();
    serverConfig.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, cluster._zkUrl);
    serverConfig.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, config._name);
    serverConfig.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, ControllerTest.LOCAL_HOST);
    serverConfig.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR, cluster._tempDir + "/dataDir");
    serverConfig.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, cluster._tempDir + "/segmentTar");
    serverConfig.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    serverConfig.setProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);
    serverConfig.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, findAvailablePort(cluster._serverPort));
    serverConfig.setProperty(Helix.KEY_OF_SERVER_NETTY_PORT, findAvailablePort(cluster._serverPort + 1));
    serverConfig.setProperty(Server.CONFIG_OF_GRPC_PORT, findAvailablePort(cluster._serverPort + 2));
    serverConfig.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConfig.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    serverConfig.setProperty(Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, true);

    cluster._serverStarter = createServerStarter();
    cluster._serverStarter.init(serverConfig);
    cluster._serverStarter.start();
  }

  protected int findAvailablePort(int basePort) {
    try {
      return NetUtils.findOpenPort(basePort);
    } catch (Exception e) {
      throw new RuntimeException("Failed to find available port starting from " + basePort, e);
    }
  }

  protected List<File> createAvroData(int dataSize, int clusterId) throws Exception {
    return createAvroDataMultipleSegments(dataSize, clusterId, 1);
  }

  protected List<File> createAvroDataMultipleSegments(int totalDataSize, int clusterId, int numSegments)
      throws Exception {
    Schema schema = createSchema(SCHEMA_FILE);
    org.apache.avro.Schema avroSchema = createAvroSchema(schema);
    File tempDir = (clusterId == 1) ? _cluster1._tempDir : _cluster2._tempDir;
    List<File> avroFiles = new ArrayList<>();

    for (int segment = 0; segment < numSegments; segment++) {
      File avroFile = new File(tempDir, "cluster" + clusterId + "_data_segment" + segment + ".avro");
      try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        writer.create(avroSchema, avroFile);
        int start = segment * (totalDataSize / numSegments);
        int end = (segment == numSegments - 1) ? totalDataSize : (segment + 1) * (totalDataSize / numSegments);
        for (int i = start; i < end; i++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
            record.put(fieldSpec.getName(), generateFieldValue(fieldSpec.getName(), i, clusterId,
                fieldSpec.getDataType()));
          }
          writer.append(record);
        }
      }
      avroFiles.add(avroFile);
    }
    return avroFiles;
  }

  private org.apache.avro.Schema createAvroSchema(Schema schema) {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    List<org.apache.avro.Schema.Field> fields = new ArrayList<>();

    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      org.apache.avro.Schema.Type avroType = getAvroType(fieldSpec.getDataType());
      fields.add(new org.apache.avro.Schema.Field(fieldSpec.getName(),
          org.apache.avro.Schema.create(avroType), null, null));
    }
    avroSchema.setFields(fields);
    return avroSchema;
  }

  private org.apache.avro.Schema.Type getAvroType(FieldSpec.DataType type) {
    switch (type) {
      case INT: return org.apache.avro.Schema.Type.INT;
      case LONG: return org.apache.avro.Schema.Type.LONG;
      case FLOAT: return org.apache.avro.Schema.Type.FLOAT;
      case DOUBLE: return org.apache.avro.Schema.Type.DOUBLE;
      case BOOLEAN: return org.apache.avro.Schema.Type.BOOLEAN;
      default: return org.apache.avro.Schema.Type.STRING;
    }
  }

  private Object generateFieldValue(String fieldName, int index, int clusterId, FieldSpec.DataType dataType) {
    int baseValue = index + (clusterId * 10000);
    switch (dataType) {
      case INT: return index + 10000;
      case LONG: return (long) baseValue;
      case FLOAT: return (float) (baseValue + 0.1);
      case DOUBLE: return (double) (baseValue + 0.1);
      case BOOLEAN: return (baseValue % 2) == 0;
      default: return "cluster_" + fieldName + "_" + index;
    }
  }

  protected void loadDataIntoCluster(List<File> avroFiles, String tableName, ClusterComponents cluster)
      throws Exception {
    cleanDirectories(cluster._segmentDir, cluster._tarDir);
    Schema schema = createSchema(SCHEMA_FILE);
    schema.setSchemaName(tableName);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0,
        cluster._segmentDir, cluster._tarDir);
    uploadSegmentsToCluster(tableName, cluster._tarDir, cluster._controllerBaseApiUrl);
    Thread.sleep(2000);
  }

  private void cleanDirectories(File... dirs) {
    for (File dir : dirs) {
      try {
        FileUtils.cleanDirectory(dir);
      } catch (IOException e) {
        // Ignore cleanup errors
      }
    }
  }

  protected void uploadSegmentsToCluster(String tableName, File tarDir, String controllerBaseApiUrl) throws Exception {
    File[] segmentTarFiles = tarDir.listFiles();
    assertNotNull(segmentTarFiles);
    assertTrue(segmentTarFiles.length > 0);

    URI uploadSegmentHttpURI = URI.create(controllerBaseApiUrl + "/segments");

    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      for (File segmentTarFile : segmentTarFiles) {
        int status = fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI,
                segmentTarFile.getName(), segmentTarFile, List.of(), tableName, TableType.OFFLINE)
            .getStatusCode();
        assertEquals(status, HttpStatus.SC_OK);
      }
    }

    Thread.sleep(3000);
  }


  protected void createSchemaAndTableForCluster(String tableName, String controllerBaseApiUrl) throws IOException {
    Schema schema = createSchema(SCHEMA_FILE);
    schema.setSchemaName(tableName);
    addSchemaToCluster(schema, controllerBaseApiUrl);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    addTableConfigToCluster(tableConfig, controllerBaseApiUrl);
  }

  protected void createSchemaAndTableOnBothClusters(String tableName) throws Exception {
    dropTableAndSchemaIfExists(tableName, _cluster1._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(tableName, _cluster2._controllerBaseApiUrl);
    createSchemaAndTableForCluster(tableName, _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(tableName, _cluster2._controllerBaseApiUrl);
  }

  protected void dropTableAndSchemaIfExists(String tableName, String controllerBaseApiUrl) {
    dropResource(controllerBaseApiUrl + "/tables/" + tableName);
    dropResource(controllerBaseApiUrl + "/schemas/" + tableName);
  }

  private void dropResource(String url) {
    try {
      ControllerTest.sendDeleteRequest(url);
    } catch (Exception e) {
      // Ignore
    }
  }

  protected void addSchemaToCluster(Schema schema, String controllerBaseApiUrl) throws IOException {
    String url = controllerBaseApiUrl + "/schemas";
    String schemaJson = schema.toPrettyJsonString();
    String response = ControllerTest.sendPostRequest(url, schemaJson);
    assertNotNull(response);
  }

  protected void addTableConfigToCluster(TableConfig tableConfig, String controllerBaseApiUrl) throws IOException {
    String url = controllerBaseApiUrl + "/tables";
    String tableConfigJson = JsonUtils.objectToPrettyString(tableConfig);
    String response = ControllerTest.sendPostRequest(url, tableConfigJson);
    assertNotNull(response);
  }

  protected String executeQuery(String query, ClusterComponents cluster) throws Exception {
    return executeQueryOnBrokerPort(query, cluster._brokerPort);
  }

  protected String executeQueryOnBrokerPort(String query, int brokerPort) throws Exception {
    Map<String, Object> payload = Map.of("sql", query);
    String url = "http://localhost:" + brokerPort + "/query/sql";
    return ControllerTest.sendPostRequest(url, JsonUtils.objectToPrettyString(payload));
  }

  protected void verifyUnavailableClusterException(String result, boolean expectException) throws Exception {
    if (expectException) {
      assertTrue(result.contains(UNAVAILABLE_CLUSTER_NAME),
          "Response should mention unavailable cluster: " + UNAVAILABLE_CLUSTER_NAME);
      JsonNode resultJson = JsonMapper.builder().build().readTree(result);
      JsonNode exceptions = resultJson.get("exceptions");
      assertNotNull(exceptions, "Exceptions array should exist");
      boolean found = false;
      for (JsonNode ex : exceptions) {
        if (ex.get("errorCode").asInt() == 510
            && ex.get("message").asText().contains(UNAVAILABLE_CLUSTER_NAME)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Should find REMOTE_CLUSTER_UNAVAILABLE (510) exception");
    }
  }

  protected long parseCountResult(String result) {
    try {
      JsonNode rows = JsonMapper.builder().build().readTree(result).path("resultTable").path("rows");
      if (rows.isArray() && rows.size() > 0) {
        JsonNode firstRow = rows.get(0);
        if (firstRow.isArray() && firstRow.size() > 0) {
          return Long.parseLong(firstRow.get(0).asText());
        }
      }
    } catch (Exception e) {
      // Ignore
    }
    return 0;
  }

  protected Schema createSchema(String schemaFileName) throws IOException {
    InputStream schemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaFileName);
    assertNotNull(schemaInputStream, "Schema file not found: " + schemaFileName);
    return Schema.fromInputStream(schemaInputStream);
  }

  @AfterClass
  public void tearDown() throws Exception {
    // Stop the alternate broker with unavailable cluster
    if (_brokerWithUnavailableCluster != null && _brokerWithUnavailableCluster._brokerStarter != null) {
      try {
        _brokerWithUnavailableCluster._brokerStarter.stop();
      } catch (Exception e) {
        LOGGER.warn("Error stopping broker with unavailable cluster", e);
      }
    }
    stopCluster(_cluster1);
    stopCluster(_cluster2);
  }

  private void stopCluster(ClusterComponents cluster) {
    if (cluster == null) {
      return;
    }
    try {
      if (cluster._serverStarter != null) {
        cluster._serverStarter.stop();
      }
      if (cluster._brokerStarter != null) {
        cluster._brokerStarter.stop();
      }
      if (cluster._controllerStarter != null) {
        cluster._controllerStarter.stop();
      }
      if (cluster._zkInstance != null) {
        ZkStarter.stopLocalZkServer(cluster._zkInstance);
      }
      FileUtils.deleteQuietly(cluster._tempDir);
    } catch (Exception e) {
      LOGGER.warn("Error stopping cluster", e);
    }
  }

  protected void cleanSegmentDirs() {
    cleanDirectories(_cluster1._segmentDir, _cluster1._tarDir, _cluster2._segmentDir, _cluster2._tarDir);
  }

  protected long getCount(String tableName, ClusterComponents cluster, boolean enableMultiClusterRouting)
      throws Exception {
    String query = "SET enableMultiClusterRouting=" + enableMultiClusterRouting + "; SELECT COUNT(*) as count FROM "
      + tableName;
    return parseCountResult(executeQuery(query, cluster));
  }

  /*
  Logical table helper methods
  */
  protected void createLogicalTable(String schemaFile,
      Map<String, PhysicalTableConfig> physicalTableConfigMap, String brokerTenant, String controllerBaseApiUrl,
      String logicalTable, String refOfflineTable, String refRealtimeTable) throws IOException {
    ControllerRequestURLBuilder urlBuilder = ControllerRequestURLBuilder.baseUrl(controllerBaseApiUrl);
    ControllerRequestClient client = new ControllerRequestClient(urlBuilder, getHttpClient(),
        getControllerRequestClientHeaders());
    Schema schema = createSchema(schemaFile);
    schema.setSchemaName(logicalTable);
    client.addSchema(schema);
    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName(logicalTable)
        .setBrokerTenant(brokerTenant)
        .setRefOfflineTableName(refOfflineTable)
        .setRefRealtimeTableName(refRealtimeTable)
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .build();
    String response = ControllerTest.sendPostRequest(urlBuilder.forLogicalTableCreate(),
        config.toSingleLineJsonString(), Map.of());
    assertEquals(response, "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTable
        + " logical table successfully added.\"}");
  }

  protected void createLogicalTableOnBothClusters(String logicalTableName,
      String cluster1PhysicalTable, String cluster2PhysicalTable) throws IOException {
    // For cluster 1: cluster1's table is local (isMultiCluster=false), cluster2's table is remote (isMultiCluster=true)
    Map<String, PhysicalTableConfig> cluster1PhysicalTableConfigMap = Map.of(
        cluster1PhysicalTable + "_OFFLINE", new PhysicalTableConfig(false),
        cluster2PhysicalTable + "_OFFLINE", new PhysicalTableConfig(true)
    );

    // For cluster 2: cluster2's table is local (isMultiCluster=false), cluster1's table is remote (isMultiCluster=true)
    Map<String, PhysicalTableConfig> cluster2PhysicalTableConfigMap = Map.of(
        cluster1PhysicalTable + "_OFFLINE", new PhysicalTableConfig(true),
        cluster2PhysicalTable + "_OFFLINE", new PhysicalTableConfig(false)
    );

    createLogicalTable(SCHEMA_FILE, cluster1PhysicalTableConfigMap, DEFAULT_TENANT,
        _cluster1._controllerBaseApiUrl, logicalTableName, cluster1PhysicalTable + "_OFFLINE", null);
    createLogicalTable(SCHEMA_FILE, cluster2PhysicalTableConfigMap, DEFAULT_TENANT,
        _cluster2._controllerBaseApiUrl, logicalTableName, cluster2PhysicalTable + "_OFFLINE", null);
  }

  protected void dropLogicalTableIfExists(String logicalTableName, String controllerBaseApiUrl) {
    dropResource(controllerBaseApiUrl + "/logicalTables/" + logicalTableName);
  }

  protected void setupFirstLogicalFederatedTable() throws Exception {
    setupLogicalFederatedTable(LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE);
  }

  protected void setupSecondLogicalFederatedTable() throws Exception {
    setupLogicalFederatedTable(LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2);
  }

  protected void setupLogicalFederatedTable(String cluster1TableName, String cluster2TableName) throws Exception {
    dropTableAndSchemaIfExists(cluster1TableName, _cluster1._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(cluster2TableName, _cluster2._controllerBaseApiUrl);
    createSchemaAndTableForCluster(cluster1TableName, _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(cluster2TableName, _cluster2._controllerBaseApiUrl);
  }

  protected void assertResultRows(String resultJson) throws Exception {
    JsonNode rows = JsonMapper.builder().build().readTree(resultJson).get("resultTable").get("rows");
    assertNotNull(rows);
    for (JsonNode row : rows) {
      int number = Integer.parseInt(row.get(0).asText().split("_")[2]);
      // Depending on the number of records with the same join key in each cluster, the expected count varies.
      // If the number is less than the size of the smaller cluster, it should appear in both clusters,
      // resulting in 4 records (2 from each cluster).
      // Otherwise, it should appear only in one cluster, resulting in 1 record.
      int expectedCount = number < Math.min(TABLE_SIZE_CLUSTER_1, TABLE_SIZE_CLUSTER_2) ? 4 : 1;
      assertEquals(row.get(1).asInt(), expectedCount);
    }
  }
}
