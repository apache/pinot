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
package org.apache.pinot.integration.tests.federation;

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
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientTransport;
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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Base class for dual isolated cluster integration tests.
 * Contains all common code, helper methods, and utilities.
 * Does NOT contain any @Test methods.
 */
public abstract class BaseDualIsolatedClusterIntegrationTest extends ClusterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDualIsolatedClusterIntegrationTest.class);

  // Constants
  protected static final String SCHEMA_FILE = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  protected static final String TIME_COLUMN = "DaysSinceEpoch";
  protected static final String FEDERATION_TABLE = "federation_test_table";
  protected static final String FEDERATION_TABLE_2 = "federation_test_table_2";
  protected static final String JOIN_COLUMN = "OriginCityName";
  protected static final int CLUSTER_1_SIZE = 1500;
  protected static final int CLUSTER_2_SIZE = 1000;
  protected static final int SEGMENTS_PER_CLUSTER = 3;
  protected static final String DEFAULT_TENANT = "DefaultTenant";

  protected static final String CLUSTER_1_NAME = "DualIsolatedCluster1";
  protected static final String CLUSTER_2_NAME = "DualIsolatedCluster2";

  protected static final String LOGICAL_FEDERATION_CLUSTER_1_TABLE = "logical_federation_table_cluster1";
  protected static final String LOGICAL_FEDERATION_CLUSTER_2_TABLE = "logical_federation_table_cluster2";

  protected static final String LOGICAL_FEDERATION_CLUSTER_1_TABLE_2 = "logical_federation_table2_cluster1";
  protected static final String LOGICAL_FEDERATION_CLUSTER_2_TABLE_2 = "logical_federation_table2_cluster2";

  // Cluster configurations
  protected static final ClusterConfig CLUSTER_1_CONFIG = new ClusterConfig(CLUSTER_1_NAME, 30000);
  protected static final ClusterConfig CLUSTER_2_CONFIG = new ClusterConfig(CLUSTER_2_NAME, 40000);
  protected static final String LOGICAL_TABLE_NAME = "logical_table";
  protected static final String LOGICAL_TABLE_NAME_2 = "logical_table_2";

  // Cluster components
  protected ClusterComponents _cluster1;
  protected ClusterComponents _cluster2;

  // Test data
  protected List<File> _cluster1AvroFiles;
  protected List<File> _cluster2AvroFiles;
  protected List<File> _cluster1AvroFiles2;
  protected List<File> _cluster2AvroFiles2;

  /**
   * Cluster configuration helper class
   */
  protected static class ClusterConfig {
    final String _name;
    final int _basePort;

    ClusterConfig(String name, int basePort) {
      _name = name;
      _basePort = basePort;
    }
  }

  /**
   * Cluster components container
   */
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
    Connection _pinotConnection;
    File _tempDir;
    File _segmentDir;
    File _tarDir;
  }

  protected void setupDirectories() throws Exception {
    _cluster1._tempDir = new File(FileUtils.getTempDirectory(), "cluster1_" + getClass().getSimpleName());
    _cluster1._segmentDir = new File(_cluster1._tempDir, "segmentDir");
    _cluster1._tarDir = new File(_cluster1._tempDir, "tarDir");

    _cluster2._tempDir = new File(FileUtils.getTempDirectory(), "cluster2_" + getClass().getSimpleName());
    _cluster2._segmentDir = new File(_cluster2._tempDir, "segmentDir");
    _cluster2._tarDir = new File(_cluster2._tempDir, "tarDir");

    TestUtils.ensureDirectoriesExistAndEmpty(_cluster1._tempDir, _cluster1._segmentDir, _cluster1._tarDir);
    TestUtils.ensureDirectoriesExistAndEmpty(_cluster2._tempDir, _cluster2._segmentDir, _cluster2._tarDir);
  }

  protected void startZookeeper(ClusterComponents cluster) throws Exception {
    LOGGER.info("Starting Zookeeper for cluster: {}", cluster._tempDir.getName());
    cluster._zkInstance = ZkStarter.startLocalZkServer();
    cluster._zkUrl = cluster._zkInstance.getZkUrl();
  }

  protected void startControllerInit(ClusterComponents cluster, ClusterConfig config) throws Exception {
    cluster._controllerPort = findAvailablePort(config._basePort);
    startController(cluster, config);
  }

  protected void startCluster(ClusterComponents cluster, ClusterComponents secondaryCluster,
      ClusterConfig config) throws Exception {
    LOGGER.info("Starting cluster: {}", config._name);

    // Start Broker
    cluster._brokerPort = findAvailablePort(cluster._controllerPort + 1000);
    startBroker(cluster, secondaryCluster, config);

    // Start Server with MSE enabled
    cluster._serverPort = findAvailablePort(cluster._brokerPort + 1000);
    startServerWithMSE(cluster, config);

    LOGGER.info("Cluster {} started successfully", config._name);
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

  protected void startBroker(ClusterComponents cluster, ClusterComponents secondaryCluster,
      ClusterConfig config) throws Exception {
    PinotConfiguration brokerConfig = new PinotConfiguration();
    brokerConfig.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, cluster._zkUrl);

    String secondaryClusterName = getSecondaryClusterName(config._name);
    brokerConfig.setProperty(String.format(Helix.CONFIG_OF_SECONDARY_ZOOKEEPER_SERVER, secondaryClusterName),
        secondaryCluster._zkUrl);
    brokerConfig.setProperty(Helix.CONFIG_OF_SECONDARY_CLUSTER_NAME, secondaryClusterName);

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

  protected String getSecondaryClusterName(String primaryClusterName) {
    return CLUSTER_1_NAME.equalsIgnoreCase(primaryClusterName) ? CLUSTER_2_NAME : CLUSTER_1_NAME;
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

  protected void setupPinotConnections() throws Exception {
    PinotClientTransport transport1 = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
    _cluster1._pinotConnection = ConnectionFactory.fromZookeeper(
        _cluster1._zkUrl + "/" + CLUSTER_1_CONFIG._name, transport1);

    PinotClientTransport transport2 = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
    _cluster2._pinotConnection = ConnectionFactory.fromZookeeper(
        _cluster2._zkUrl + "/" + CLUSTER_2_CONFIG._name, transport2);
  }

  protected void assertResultRows(String resultJson) throws Exception {
    JsonNode root =
        JsonMapper.builder().build().readTree(resultJson);
    JsonNode rows = root.get("resultTable").get("rows");
    assertNotNull(rows);
    for (JsonNode row : rows) {
      String cityName = row.get(0).asText();
      int count = row.get(1).asInt();
      String[] parts = cityName.split("_");
      int number = Integer.parseInt(parts[2]);
      if (number < 1000) {
        assertEquals(count, 4, "Expected count 4 for " + cityName);
      } else {
        assertEquals(count, 1, "Expected count 1 for " + cityName);
      }
    }
  }

  protected List<File> createAvroData(int dataSize, int clusterId) throws Exception {
    return createAvroDataMultipleSegments(dataSize, clusterId, 1);
  }

  protected List<File> createAvroDataMultipleSegments(int totalDataSize, int clusterId,
      int numSegments) throws Exception {
    Schema schema = createSchema(SCHEMA_FILE);
    org.apache.avro.Schema avroSchema = createAvroSchema(schema);

    File tempDir = (clusterId == 1) ? _cluster1._tempDir : _cluster2._tempDir;
    List<File> avroFiles = new ArrayList<>();
    int dataPerSegment = totalDataSize / numSegments;

    for (int segment = 0; segment < numSegments; segment++) {
      File avroFile = new File(tempDir, "cluster" + clusterId + "_data_segment" + segment + ".avro");

      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        int startIndex = segment * dataPerSegment;
        int endIndex = (segment == numSegments - 1) ? totalDataSize : (segment + 1) * dataPerSegment;

        for (int i = startIndex; i < endIndex; i++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
            String fieldName = fieldSpec.getName();
            Object value = generateFieldValue(fieldName, i, clusterId, fieldSpec.getDataType());
            record.put(fieldName, value);
          }
          fileWriter.append(record);
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

  private org.apache.avro.Schema.Type getAvroType(FieldSpec.DataType pinotType) {
    switch (pinotType) {
      case INT: return org.apache.avro.Schema.Type.INT;
      case LONG: return org.apache.avro.Schema.Type.LONG;
      case FLOAT: return org.apache.avro.Schema.Type.FLOAT;
      case DOUBLE: return org.apache.avro.Schema.Type.DOUBLE;
      case STRING: return org.apache.avro.Schema.Type.STRING;
      case BOOLEAN: return org.apache.avro.Schema.Type.BOOLEAN;
      default: return org.apache.avro.Schema.Type.STRING;
    }
  }

  private Object generateFieldValue(String fieldName, int index, int clusterId, FieldSpec.DataType dataType) {
    int baseValue = index + (clusterId * 10000);

    switch (dataType) {
      case INT:
        return index + 10000;
      case LONG:
        return (long) baseValue;
      case FLOAT:
        return (float) (baseValue + 0.1);
      case DOUBLE:
        return (double) (baseValue + 0.1);
      case BOOLEAN:
        return (baseValue % 2) == 0;
      case STRING:
      default:
        return "cluster_" + fieldName + "_" + index;
    }
  }

  protected void loadDataIntoCluster(List<File> avroFiles, String tableName, ClusterComponents cluster)
      throws Exception {
    FileUtils.cleanDirectory(cluster._segmentDir);
    FileUtils.cleanDirectory(cluster._tarDir);

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

  protected void setupFederationTable() throws Exception {
    dropTableAndSchemaIfExists(FEDERATION_TABLE, _cluster1._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(FEDERATION_TABLE, _cluster2._controllerBaseApiUrl);

    Schema schema = createSchema(SCHEMA_FILE);
    schema.setSchemaName(FEDERATION_TABLE);
    addSchemaToCluster(schema, _cluster1._controllerBaseApiUrl);
    addSchemaToCluster(schema, _cluster2._controllerBaseApiUrl);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(FEDERATION_TABLE)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    addTableConfigToCluster(tableConfig, _cluster1._controllerBaseApiUrl);
    addTableConfigToCluster(tableConfig, _cluster2._controllerBaseApiUrl);
  }

  protected void setupLogicalFederatedTable(String cluster1TableName, String cluster2TableName) throws Exception {
    dropTableAndSchemaIfExists(cluster1TableName, _cluster1._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(cluster2TableName, _cluster2._controllerBaseApiUrl);

    createSchemaAndTableForCluster(cluster1TableName, _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(cluster2TableName, _cluster2._controllerBaseApiUrl);
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

  protected void setupFirstLogicalFederatedTable() throws Exception {
    setupLogicalFederatedTable(LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE);
  }

  protected void setupSecondLogicalFederatedTable() throws Exception {
    setupLogicalFederatedTable(LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2);
  }

  protected void createLogicalTableOnBothClusters(String logicalTableName,
      String cluster1PhysicalTable, String cluster2PhysicalTable) throws IOException {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = Map.of(
        cluster1PhysicalTable + "_OFFLINE", new PhysicalTableConfig(true),
        cluster2PhysicalTable + "_OFFLINE", new PhysicalTableConfig(true)
    );

    createLogicalTable(CLUSTER_1_NAME, SCHEMA_FILE, physicalTableConfigMap, DEFAULT_TENANT,
        _cluster1._controllerBaseApiUrl, logicalTableName, cluster1PhysicalTable + "_OFFLINE", null);
    createLogicalTable(CLUSTER_2_NAME, SCHEMA_FILE, physicalTableConfigMap, DEFAULT_TENANT,
        _cluster2._controllerBaseApiUrl, logicalTableName, cluster2PhysicalTable + "_OFFLINE", null);
  }

  protected void cleanSegmentDirs() throws Exception {
    try {
      FileUtils.cleanDirectory(_cluster1._segmentDir);
    } catch (IOException e) {
      LOGGER.warn("Failed to clean cluster 1 segment dir", e);
    }
    try {
      FileUtils.cleanDirectory(_cluster1._tarDir);
    } catch (IOException e) {
      LOGGER.warn("Failed to clean cluster 1 tar dir", e);
    }
    try {
      FileUtils.cleanDirectory(_cluster2._segmentDir);
    } catch (IOException e) {
      LOGGER.warn("Failed to clean cluster 2 segment dir", e);
    }
    try {
      FileUtils.cleanDirectory(_cluster2._tarDir);
    } catch (IOException e) {
      LOGGER.warn("Failed to clean cluster 2 tar dir", e);
    }
  }

  protected void dropTableAndSchemaIfExists(String tableName, String controllerBaseApiUrl) throws Exception {
    dropResourceIfExists(controllerBaseApiUrl + "/tables/" + tableName);
    dropResourceIfExists(controllerBaseApiUrl + "/schemas/" + tableName);
  }

  protected void dropLogicalTableIfExists(String logicalTableName, String controllerBaseApiUrl) throws Exception {
    dropResourceIfExists(controllerBaseApiUrl + "/logicalTables/" + logicalTableName);
  }

  protected void dropResourceIfExists(String url) {
    try {
      ControllerTest.sendDeleteRequest(url);
    } catch (Exception e) {
      // Ignore if not exists
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

  protected long getCount(String tableName, ClusterComponents cluster) throws Exception {
    String query = "SELECT COUNT(*) as count FROM " + tableName;
    String result = executeQuery(query, cluster);
    return parseCountResult(result);
  }

  protected String executeQuery(String query, ClusterComponents cluster) throws Exception {
    String queryUrl = "http://localhost:" + cluster._brokerPort + "/query/sql";
    Map<String, Object> requestPayload = new HashMap<>();
    requestPayload.put("sql", query);
    String jsonPayload = JsonUtils.objectToPrettyString(requestPayload);
    return ControllerTest.sendPostRequest(queryUrl, jsonPayload);
  }

  protected long parseCountResult(String result) {
    try {
      JsonNode root = JsonMapper.builder().build().readTree(result);
      JsonNode rows = root.path("resultTable").path("rows");
      if (rows.isArray() && rows.size() > 0) {
        JsonNode firstRow = rows.get(0);
        if (firstRow.isArray() && firstRow.size() > 0) {
          return Long.parseLong(firstRow.get(0).asText());
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error parsing count result: {}", result, e);
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
    LOGGER.info("Tearing down dual isolated Pinot clusters");

    // Stop Cluster 1 components
    if (_cluster1 != null) {
      if (_cluster1._serverStarter != null) {
        _cluster1._serverStarter.stop();
      }
      if (_cluster1._brokerStarter != null) {
        _cluster1._brokerStarter.stop();
      }
      if (_cluster1._controllerStarter != null) {
        _cluster1._controllerStarter.stop();
      }
      if (_cluster1._zkInstance != null) {
        ZkStarter.stopLocalZkServer(_cluster1._zkInstance);
      }
      // Clean up test directories
      FileUtils.deleteQuietly(_cluster1._tempDir);
    }

    // Stop Cluster 2 components
    if (_cluster2 != null) {
      if (_cluster2._serverStarter != null) {
        _cluster2._serverStarter.stop();
      }
      if (_cluster2._brokerStarter != null) {
        _cluster2._brokerStarter.stop();
      }
      if (_cluster2._controllerStarter != null) {
        _cluster2._controllerStarter.stop();
      }
      if (_cluster2._zkInstance != null) {
        ZkStarter.stopLocalZkServer(_cluster2._zkInstance);
      }
      // Clean up test directories
      FileUtils.deleteQuietly(_cluster2._tempDir);
    }

    LOGGER.info("Dual isolated Pinot clusters torn down successfully");
  }

  /** Logical table code **/
  protected void createLogicalTable(String clusterName, String schemaFile,
      Map<String, PhysicalTableConfig> physicalTableConfigMap, String brokerTenant, String controllerBaseApiUrl,
      String logicalTable, String refOfflineTable, String refRealtimeTable)
      throws IOException {
    ControllerRequestURLBuilder controllerRequestURLBuilder =
        ControllerRequestURLBuilder.baseUrl(controllerBaseApiUrl);
    ControllerRequestClient controllerRequestClient =
        new ControllerRequestClient(ControllerRequestURLBuilder.baseUrl(controllerBaseApiUrl), getHttpClient(),
            getControllerRequestClientHeaders());
    String getBrokersUrl = controllerRequestURLBuilder.forBrokersGet(null);
    String brokerResponse = ControllerTest.sendGetRequest(getBrokersUrl);
    System.out.println("Broker response: " + brokerResponse + " for cluster: " + clusterName);

    String addLogicalTableUrl = controllerRequestURLBuilder.forLogicalTableCreate();
    Schema logicalTableSchema = createSchema(schemaFile);
    logicalTableSchema.setSchemaName(logicalTable);
    controllerRequestClient.addSchema(logicalTableSchema);
    LogicalTableConfig logicalTableConfig =
        getLogicalTableConfig(logicalTable, physicalTableConfigMap, brokerTenant, refOfflineTable, refRealtimeTable);
    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), Map.of());
    assertEquals(resp, "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTable
        + " logical table successfully added.\"}");
  }

  public LogicalTableConfig getLogicalTableConfig(String tableName,
      Map<String, PhysicalTableConfig> physicalTableConfigMap, String brokerTenant, String refOfflineTable, String refRealtimeTable) {
    LogicalTableConfigBuilder builder =
        new LogicalTableConfigBuilder().setTableName(tableName)
            .setBrokerTenant(brokerTenant)
            .setRefOfflineTableName(refOfflineTable)
            .setRefRealtimeTableName(refRealtimeTable)
            .setPhysicalTableConfigMap(physicalTableConfigMap);
    return builder.build();
  }
}
