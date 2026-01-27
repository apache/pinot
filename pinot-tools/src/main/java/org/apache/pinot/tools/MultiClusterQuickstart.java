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
package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.broker.helix.MultiClusterHelixBrokerStarter;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
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
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-Cluster Quickstart for demonstrating multi-cluster querying (federation).
 * Usage: ./pinot-admin.sh QuickStart -type MULTI_CLUSTER
 */
public class MultiClusterQuickstart extends QuickStartBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterQuickstart.class);
  private static final String DEFAULT_TENANT = "DefaultTenant";
  private static final String LOGICAL_TABLE_NAME = "unified_orders";
  private static final String PHYSICAL_TABLE_PREFIX = "orders_cluster";
  private static final String SCHEMA_RESOURCE_PATH = "/examples/logicalTables/orders_schema.json";
  private static final String DATA_RESOURCE_PATH = "/examples/batch/orders/ordersUS/rawdata/ordersUS_sample.csv";
  private static final int DEFAULT_NUM_CLUSTERS = 2;

  private final int _numClusters = DEFAULT_NUM_CLUSTERS;
  private final List<ClusterComponents> _clusters;
  private File _quickstartTmpDir;

  public MultiClusterQuickstart() {
    _clusters = new ArrayList<>();
  }

  @Override
  public List<String> types() {
    return Arrays.asList("MULTI_CLUSTER", "MULTICLUSTER");
  }

  @Override
  public void execute() throws Exception {
    _quickstartTmpDir = _setCustomDataDir ? _dataDir
        : new File(_dataDir, "multicluster_" + System.currentTimeMillis());
    Preconditions.checkState(_quickstartTmpDir.mkdirs() || _quickstartTmpDir.exists());

    printStatus(Quickstart.Color.CYAN,
        "***** Starting Multi-Cluster Quickstart with " + _numClusters + " clusters *****");

    try {
      startAllClusters();
      createPhysicalTablesOnAllClusters();
      loadDataIntoAllClusters();
      createLogicalTablesOnAllClusters();
      printStatus(Quickstart.Color.CYAN, "***** Waiting for tables to be fully loaded *****");
      Thread.sleep(5000);
      runSampleQueries();

      printStatus(Quickstart.Color.GREEN, "***** Multi-Cluster Quickstart Complete! *****");
      for (int i = 0; i < _clusters.size(); i++) {
        printStatus(Quickstart.Color.GREEN,
            "  Cluster " + (i + 1) + ": http://localhost:" + _clusters.get(i)._controllerPort);
      }

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          stopAllClusters();
          FileUtils.deleteDirectory(_quickstartTmpDir);
        } catch (Exception e) {
          LOGGER.error("Shutdown error", e);
        }
      }));
    } catch (Exception e) {
      LOGGER.error("Error in quickstart", e);
      stopAllClusters();
      throw e;
    }
  }

  private void startAllClusters() throws Exception {
    printStatus(Quickstart.Color.CYAN, "***** Starting " + _numClusters + " Pinot clusters *****");

    for (int i = 0; i < _numClusters; i++) {
      ClusterComponents cluster = new ClusterComponents();
      cluster._clusterName = "MultiCluster" + (i + 1);
      cluster._clusterIndex = i;
      cluster._tempDir = new File(_quickstartTmpDir, "cluster" + (i + 1));
      Preconditions.checkState(cluster._tempDir.mkdirs() || cluster._tempDir.exists());
      _clusters.add(cluster);
    }

    for (ClusterComponents cluster : _clusters) {
      startZookeeper(cluster);
    }
    for (ClusterComponents cluster : _clusters) {
      startController(cluster);
    }
    for (ClusterComponents cluster : _clusters) {
      startBrokerWithMultiClusterConfig(cluster);
    }
    for (ClusterComponents cluster : _clusters) {
      startServer(cluster);
    }

    printStatus(Quickstart.Color.GREEN, "***** All clusters started successfully *****");
  }

  private void startZookeeper(ClusterComponents cluster) throws Exception {
    cluster._zkInstance = ZkStarter.startLocalZkServer();
    cluster._zkUrl = cluster._zkInstance.getZkUrl();
    printStatus(Quickstart.Color.GREEN, "  ZK for " + cluster._clusterName + " at " + cluster._zkUrl);
  }

  private void startController(ClusterComponents cluster) throws Exception {
    int basePort = 9000 + (cluster._clusterIndex * 100);
    cluster._controllerPort = NetUtils.findOpenPort(basePort);

    Map<String, Object> config = new HashMap<>();
    config.put(ControllerConf.ZK_STR, cluster._zkUrl);
    config.put(ControllerConf.HELIX_CLUSTER_NAME, cluster._clusterName);
    config.put(ControllerConf.CONTROLLER_HOST, "localhost");
    config.put(ControllerConf.CONTROLLER_PORT, cluster._controllerPort);
    config.put(ControllerConf.DATA_DIR, new File(cluster._tempDir, "controllerData").getAbsolutePath());
    config.put(ControllerConf.LOCAL_TEMP_DIR, new File(cluster._tempDir, "controllerTmp").getAbsolutePath());
    config.put(ControllerConf.DISABLE_GROOVY, false);
    config.put(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");

    cluster._controllerStarter = new ControllerStarter();
    cluster._controllerStarter.init(new PinotConfiguration(config));
    cluster._controllerStarter.start();
    printStatus(Quickstart.Color.GREEN,
        "  Controller for " + cluster._clusterName + " on port " + cluster._controllerPort);
  }

  private void startBrokerWithMultiClusterConfig(ClusterComponents cluster) throws Exception {
    int basePort = 8000 + (cluster._clusterIndex * 100);
    cluster._brokerPort = NetUtils.findOpenPort(basePort);

    PinotConfiguration brokerConfig = new PinotConfiguration();
    brokerConfig.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, cluster._zkUrl);
    brokerConfig.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, cluster._clusterName);
    brokerConfig.setProperty(Broker.CONFIG_OF_BROKER_HOSTNAME, "localhost");
    brokerConfig.setProperty(Helix.KEY_OF_BROKER_QUERY_PORT, cluster._brokerPort);
    brokerConfig.setProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60000L);
    brokerConfig.setProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    brokerConfig.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");

    List<String> remoteClusterNames = new ArrayList<>();
    for (ClusterComponents remote : _clusters) {
      if (remote != cluster) {
        remoteClusterNames.add(remote._clusterName);
        brokerConfig.setProperty(
            String.format(Helix.CONFIG_OF_REMOTE_ZOOKEEPER_SERVERS, remote._clusterName), remote._zkUrl);
      }
    }
    if (!remoteClusterNames.isEmpty()) {
      brokerConfig.setProperty(Helix.CONFIG_OF_REMOTE_CLUSTER_NAMES, String.join(",", remoteClusterNames));
    }

    cluster._brokerStarter = new MultiClusterHelixBrokerStarter();
    cluster._brokerStarter.init(brokerConfig);
    cluster._brokerStarter.start();
    printStatus(Quickstart.Color.GREEN, "  Broker for " + cluster._clusterName + " on port " + cluster._brokerPort);
  }

  private void startServer(ClusterComponents cluster) throws Exception {
    int basePort = 7000 + (cluster._clusterIndex * 100);
    cluster._serverAdminPort = NetUtils.findOpenPort(basePort);
    cluster._serverNettyPort = NetUtils.findOpenPort(basePort + 10);
    cluster._serverGrpcPort = NetUtils.findOpenPort(basePort + 20);

    PinotConfiguration serverConfig = new PinotConfiguration();
    serverConfig.setProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER, cluster._zkUrl);
    serverConfig.setProperty(Helix.CONFIG_OF_CLUSTER_NAME, cluster._clusterName);
    serverConfig.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, "localhost");
    serverConfig.setProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR,
        new File(cluster._tempDir, "serverData").getAbsolutePath());
    serverConfig.setProperty(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
        new File(cluster._tempDir, "serverSegmentTar").getAbsolutePath());
    serverConfig.setProperty(Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, "v3");
    serverConfig.setProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);
    serverConfig.setProperty(Server.CONFIG_OF_ADMIN_API_PORT, cluster._serverAdminPort);
    serverConfig.setProperty(Helix.KEY_OF_SERVER_NETTY_PORT, cluster._serverNettyPort);
    serverConfig.setProperty(Server.CONFIG_OF_GRPC_PORT, cluster._serverGrpcPort);
    serverConfig.setProperty(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    serverConfig.setProperty(Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, true);

    cluster._serverStarter = new HelixServerStarter();
    cluster._serverStarter.init(serverConfig);
    cluster._serverStarter.start();
    printStatus(Quickstart.Color.GREEN,
        "  Server for " + cluster._clusterName + " on ports " + cluster._serverNettyPort);
  }

  private void createPhysicalTablesOnAllClusters() throws Exception {
    printStatus(Quickstart.Color.CYAN, "***** Creating physical tables *****");
    for (ClusterComponents cluster : _clusters) {
      String tableName = PHYSICAL_TABLE_PREFIX + (cluster._clusterIndex + 1);
      createPhysicalTable(cluster, tableName);
      cluster._physicalTableName = tableName;
    }
  }

  private void createPhysicalTable(ClusterComponents cluster, String tableName) throws Exception {
    Schema schema = loadSchema(tableName);
    uploadSchema(schema, "http://localhost:" + cluster._controllerPort + "/schemas?override=true&force=true");

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
    AbstractBaseAdminCommand.sendPostRequest(
        "http://localhost:" + cluster._controllerPort + "/tables", JsonUtils.objectToString(tableConfig));
    printStatus(Quickstart.Color.GREEN, "  Table " + tableName + " created on " + cluster._clusterName);
  }

  private Schema loadSchema(String schemaName) throws Exception {
    try (InputStream is = getClass().getResourceAsStream(SCHEMA_RESOURCE_PATH)) {
      Schema schema = Schema.fromInputStream(is);
      schema.setSchemaName(schemaName);
      return schema;
    }
  }

  private void uploadSchema(Schema schema, String url) throws Exception {
    File tempFile = File.createTempFile("schema_", ".json");
    tempFile.deleteOnExit();
    FileUtils.writeStringToFile(tempFile, schema.toPrettyJsonString(), StandardCharsets.UTF_8);

    HttpEntity entity = MultipartEntityBuilder.create()
        .addPart("schema", new FileBody(tempFile, ContentType.APPLICATION_JSON, schema.getSchemaName() + ".json"))
        .build();

    new HttpClient().sendPostRequest(URI.create(url), entity, null, null);
  }

  private void loadDataIntoAllClusters() throws Exception {
    printStatus(Quickstart.Color.CYAN, "***** Loading data *****");
    for (ClusterComponents cluster : _clusters) {
      loadDataIntoCluster(cluster);
    }
  }

  private void loadDataIntoCluster(ClusterComponents cluster) throws Exception {
    File inputDir = new File(cluster._tempDir, "inputData");
    inputDir.mkdirs();
    File inputFile = new File(inputDir, "data.csv");
    copySampleDataForCluster(cluster, inputFile);

    String url = "http://localhost:" + cluster._controllerPort
        + "/ingestFromFile?tableNameWithType=" + cluster._physicalTableName + "_OFFLINE"
        + "&batchConfigMapStr=" + java.net.URLEncoder.encode(
            "{\"inputFormat\":\"csv\",\"recordReader.prop.delimiter\":\",\"}", StandardCharsets.UTF_8);

    HttpEntity entity = MultipartEntityBuilder.create()
        .addPart("file", new FileBody(inputFile, ContentType.TEXT_PLAIN, inputFile.getName()))
        .build();

    SimpleHttpResponse resp = new HttpClient().sendPostRequest(URI.create(url), entity, null, null);
    printStatus(Quickstart.Color.GREEN,
        "  Data loaded into " + cluster._clusterName + " (status: " + resp.getStatusCode() + ")");
  }

  private void copySampleDataForCluster(ClusterComponents cluster, File outputFile) throws Exception {
    try (InputStream is = getClass().getResourceAsStream(DATA_RESOURCE_PATH)) {
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      String[] lines = content.split("\n");

      List<String> result = new ArrayList<>();
      result.add(lines[0]);

      String region = "cluster" + (cluster._clusterIndex + 1);
      for (int i = 1; i < lines.length; i++) {
        String[] parts = lines[i].split(",");
        if (parts.length >= 3) {
          parts[0] = region + "_" + parts[0];
          parts[2] = region;
          result.add(String.join(",", parts));
        }
      }
      FileUtils.writeLines(outputFile, result);
    }
  }

  private void createLogicalTablesOnAllClusters() throws Exception {
    printStatus(Quickstart.Color.CYAN, "***** Creating logical tables *****");
    for (ClusterComponents cluster : _clusters) {
      createLogicalTableOnCluster(cluster);
    }
  }

  private void createLogicalTableOnCluster(ClusterComponents cluster) throws Exception {
    Schema schema = loadSchema(LOGICAL_TABLE_NAME);
    uploadSchema(schema, "http://localhost:" + cluster._controllerPort + "/schemas?override=true&force=true");

    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    for (ClusterComponents c : _clusters) {
      boolean isRemote = (c != cluster);
      physicalTableConfigMap.put(c._physicalTableName + "_OFFLINE", new PhysicalTableConfig(isRemote));
    }

    LogicalTableConfig config = new LogicalTableConfig();
    config.setTableName(LOGICAL_TABLE_NAME);
    config.setBrokerTenant(DEFAULT_TENANT);
    config.setRefOfflineTableName(cluster._physicalTableName + "_OFFLINE");
    config.setPhysicalTableConfigMap(physicalTableConfigMap);

    try {
      AbstractBaseAdminCommand.sendPostRequest(
          "http://localhost:" + cluster._controllerPort + "/logicalTables", config.toSingleLineJsonString());
      printStatus(Quickstart.Color.GREEN, "  Logical table created on " + cluster._clusterName);
    } catch (Exception e) {
      printStatus(Quickstart.Color.YELLOW, "  Logical table creation failed: " + e.getMessage());
    }
  }

  private void runSampleQueries() throws Exception {
    printStatus(Quickstart.Color.YELLOW, "\n======== SAMPLE QUERIES ========\n");
    ClusterComponents primary = _clusters.get(0);

    for (ClusterComponents cluster : _clusters) {
      String query = "SELECT COUNT(*) FROM " + cluster._physicalTableName;
      JsonNode result = runQuery(cluster, query);
      printStatus(Quickstart.Color.GREEN, cluster._clusterName + ": " + extractCount(result) + " rows");
    }

    printStatus(Quickstart.Color.CYAN, "\n***** Multi-Cluster Query *****");
    String fedQuery = "SET enableMultiClusterRouting=true; SELECT COUNT(*) FROM " + LOGICAL_TABLE_NAME;
    JsonNode result = runQuery(primary, fedQuery);
    printStatus(Quickstart.Color.GREEN, "Federated count: " + extractCount(result) + " rows from ALL clusters");

    printStatus(Quickstart.Color.CYAN, "\n***** MSE Multi-Cluster Query *****");
    String mseQuery = "SET enableMultiClusterRouting=true; SET useMultistageEngine=true; "
        + "SELECT region, COUNT(*) as cnt FROM " + LOGICAL_TABLE_NAME + " GROUP BY region ORDER BY region";
    result = runQuery(primary, mseQuery);
    printQueryResult(result);

    printStatus(Quickstart.Color.GREEN, "\n======== DEMO COMPLETE ========\n");
    printStatus(Quickstart.Color.YELLOW, "Key: Use 'SET enableMultiClusterRouting=true' for cross-cluster queries");
  }

  private JsonNode runQuery(ClusterComponents cluster, String query) throws Exception {
    String url = "http://localhost:" + cluster._brokerPort + "/query/sql";
    String response = AbstractBaseAdminCommand.sendPostRequest(url, JsonUtils.objectToString(Map.of("sql", query)));
    return JsonUtils.stringToJsonNode(response);
  }

  private String extractCount(JsonNode result) {
    try {
      return result.path("resultTable").path("rows").get(0).get(0).asText();
    } catch (Exception e) {
      return "N/A";
    }
  }

  private void printQueryResult(JsonNode result) {
    try {
      JsonNode rows = result.path("resultTable").path("rows");
      for (JsonNode row : rows) {
        printStatus(Quickstart.Color.GREEN, "  " + row.get(0).asText() + ": " + row.get(1).asText() + " orders");
      }
    } catch (Exception e) {
      printStatus(Quickstart.Color.YELLOW, result.toPrettyString());
    }
  }

  private void stopAllClusters() {
    for (ClusterComponents cluster : _clusters) {
      stopCluster(cluster);
    }
  }

  private void stopCluster(ClusterComponents cluster) {
    if (cluster == null) {
      return;
    }
    try {
      if (cluster._serverStarter != null) {
        cluster._serverStarter.stop();
      }
    } catch (Exception e) { /* ignore */ }
    try {
      if (cluster._brokerStarter != null) {
        cluster._brokerStarter.stop();
      }
    } catch (Exception e) { /* ignore */ }
    try {
      if (cluster._controllerStarter != null) {
        cluster._controllerStarter.stop();
      }
    } catch (Exception e) { /* ignore */ }
    try {
      if (cluster._zkInstance != null) {
        ZkStarter.stopLocalZkServer(cluster._zkInstance);
      }
    } catch (Exception e) { /* ignore */ }
  }

  private static class ClusterComponents {
    String _clusterName;
    int _clusterIndex;
    File _tempDir;
    ZkStarter.ZookeeperInstance _zkInstance;
    String _zkUrl;
    BaseControllerStarter _controllerStarter;
    int _controllerPort;
    BaseBrokerStarter _brokerStarter;
    int _brokerPort;
    BaseServerStarter _serverStarter;
    int _serverAdminPort;
    int _serverNettyPort;
    int _serverGrpcPort;
    String _physicalTableName;
  }

  public static void main(String[] args) throws Exception {
    new MultiClusterQuickstart().execute();
  }
}
