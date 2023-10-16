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
package org.apache.pinot.tools.perf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class PerfBenchmarkDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerfBenchmarkDriver.class);
  private static final long BROKER_TIMEOUT_MS = 60_000L;
  private static final String BROKER_QUERY_PATH = "/query/sql";

  private final PerfBenchmarkDriverConf _conf;
  private final String _zkAddress;
  private final String _clusterName;
  private final String _tempDir;
  private final String _loadMode;
  private final String _segmentFormatVersion;
  private final boolean _verbose;

  private ControllerStarter _controllerStarter;
  private String _controllerHost;
  private int _controllerPort;
  private String _controllerAddress;
  private String _controllerDataDir;

  private String _queryUrl;

  private String _serverInstanceDataDir;
  private String _serverInstanceSegmentTarDir;
  private String _serverInstanceName;

  // TODO: read from configuration.
  private final int _numReplicas = 1;
  private final String _segmentAssignmentStrategy = "BalanceNumSegmentAssignmentStrategy";
  private final String _brokerTenantName = "DefaultTenant";
  private final String _serverTenantName = "DefaultTenant";

  // Used for creating tables and tenants, and uploading segments. Since uncompressed segments are already available
  // for PerfBenchmarkDriver, servers can directly load the segments. PinotHelixResourceManager.addNewSegment(), which
  // updates ZKSegmentMetadata only, is not exposed from controller API so we need to update segments directly via
  // PinotHelixResourceManager.
  private PinotHelixResourceManager _helixResourceManager;

  public PerfBenchmarkDriver(PerfBenchmarkDriverConf conf) {
    this(conf, "/tmp/", "HEAP", null, conf.isVerbose());
  }

  public PerfBenchmarkDriver(PerfBenchmarkDriverConf conf, String tempDir, String loadMode, String segmentFormatVersion,
      boolean verbose) {
    _conf = conf;
    _zkAddress = conf.getZkHost() + ":" + conf.getZkPort();
    _clusterName = conf.getClusterName();
    if (tempDir.endsWith("/")) {
      _tempDir = tempDir;
    } else {
      _tempDir = tempDir + '/';
    }
    _loadMode = loadMode;
    _segmentFormatVersion = segmentFormatVersion;
    _verbose = verbose;
    init();
  }

  private void init() {
    // Init controller.
    String controllerHost = _conf.getControllerHost();
    if (controllerHost != null) {
      _controllerHost = controllerHost;
    } else {
      _controllerHost = "localhost";
    }
    int controllerPort = _conf.getControllerPort();
    if (controllerPort > 0) {
      _controllerPort = controllerPort;
    } else {
      _controllerPort = 8300;
    }
    _controllerAddress = _controllerHost + ":" + _controllerPort;
    String controllerDataDir = _conf.getControllerDataDir();
    if (controllerDataDir != null) {
      _controllerDataDir = controllerDataDir;
    } else {
      _controllerDataDir = _tempDir + "controller/" + _controllerAddress + "/controller_data_dir";
    }

    // Init broker.
    _queryUrl = StringUtils.isNotBlank(_conf.getBrokerURL()) ? _conf.getBrokerURL() + BROKER_QUERY_PATH
        : "http://" + _conf.getBrokerHost() + ":" + _conf.getBrokerPort() + BROKER_QUERY_PATH;

    // Init server.
    String serverInstanceName = _conf.getServerInstanceName();
    if (serverInstanceName != null) {
      _serverInstanceName = serverInstanceName;
    } else {
      _serverInstanceName = "Server_localhost_" + CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    }
    String serverInstanceDataDir = _conf.getServerInstanceDataDir();
    if (serverInstanceDataDir != null) {
      _serverInstanceDataDir = serverInstanceDataDir;
    } else {
      _serverInstanceDataDir = _tempDir + "server/" + _serverInstanceName + "/index_data_dir";
    }
    String serverInstanceSegmentTarDir = _conf.getServerInstanceSegmentTarDir();
    if (serverInstanceSegmentTarDir != null) {
      _serverInstanceSegmentTarDir = serverInstanceSegmentTarDir;
    } else {
      _serverInstanceSegmentTarDir = _tempDir + "server/" + _serverInstanceName + "/segment_tar_dir";
    }
  }

  public void run()
      throws Exception {
    startZookeeper();
    startController();
    startBroker();
    startServer();
    startHelixResourceManager();
    configureResources();
    waitForExternalViewUpdate(_zkAddress, _clusterName, 60 * 1000L);
    postQueries();
  }

  private void startZookeeper()
      throws Exception {
    if (!_conf.isStartZookeeper()) {
      LOGGER.info("Skipping start zookeeper step. Assumes zookeeper is already started.");
      return;
    }
    int zkPort = _conf.getZkPort();
    ZkStarter.startLocalZkServer(zkPort);
  }

  private void startController()
      throws Exception {
    if (!_conf.shouldStartController()) {
      LOGGER.info("Skipping start controller step. Assumes controller is already started.");
      return;
    }

    LOGGER.info("Starting controller at {}", _controllerAddress);
    _controllerStarter = new ControllerStarter();
    _controllerStarter.init(new PinotConfiguration(getControllerProperties()));
    _controllerStarter.start();
  }

  private Map<String, Object> getControllerProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, _clusterName);
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, _zkAddress);
    properties.put(ControllerConf.CONTROLLER_HOST, _controllerHost);
    properties.put(ControllerConf.CONTROLLER_PORT, String.valueOf(_controllerPort));
    properties.put(ControllerConf.DATA_DIR, _controllerDataDir);
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    properties.put(ControllerConf.CONTROLLER_VIP_HOST, "localhost");
    properties.put(ControllerConf.CONTROLLER_VIP_PROTOCOL, CommonConstants.HTTP_PROTOCOL);
    return properties;
  }

  private ControllerConf getControllerConf() {
    ControllerConf conf = new ControllerConf();
    conf.setHelixClusterName(_clusterName);
    conf.setZkStr(_zkAddress);
    conf.setControllerHost(_controllerHost);
    conf.setControllerPort(String.valueOf(_controllerPort));
    conf.setDataDir(_controllerDataDir);
    conf.setTenantIsolationEnabled(false);
    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol(CommonConstants.HTTP_PROTOCOL);
    return conf;
  }

  private void startBroker()
      throws Exception {
    if (!_conf.shouldStartBroker()) {
      LOGGER.info("Skipping start broker step. Assumes broker is already started.");
      return;
    }
    String brokerInstanceName = "Broker_localhost_" + CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;

    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.CONFIG_OF_BROKER_ID, brokerInstanceName);
    properties.put(CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS, BROKER_TIMEOUT_MS);
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, _clusterName);
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, _zkAddress);

    LOGGER.info("Starting broker instance: {}", brokerInstanceName);

    HelixBrokerStarter helixBrokerStarter = new HelixBrokerStarter();
    helixBrokerStarter.init(new PinotConfiguration(properties));
    helixBrokerStarter.start();
  }

  private void startServer()
      throws Exception {
    if (!_conf.shouldStartServer()) {
      LOGGER.info("Skipping start server step. Assumes server is already started.");
      return;
    }

    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR, _serverInstanceDataDir);
    properties.put(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, _serverInstanceSegmentTarDir);
    properties.put(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, "localhost");
    properties.put(CommonConstants.Server.CONFIG_OF_INSTANCE_ID, _serverInstanceName);
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, _clusterName);
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, _zkAddress);
    if (_segmentFormatVersion != null) {
      properties.put(CommonConstants.Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, _segmentFormatVersion);
    }

    LOGGER.info("Starting server instance: {}", _serverInstanceName);

    HelixServerStarter helixServerStarter = new HelixServerStarter();
    helixServerStarter.init(new PinotConfiguration(properties));
    helixServerStarter.start();
  }

  private void startHelixResourceManager()
      throws Exception {
    if (_conf.shouldStartController()) {
      // helix resource manager is already available at this time if controller is started
      _helixResourceManager = _controllerStarter.getHelixResourceManager();
    } else {
      // When starting server only, we need to change the controller port to avoid registering controller helix
      // participant with the same host and port.
      ControllerConf controllerConf = getControllerConf();
      controllerConf.setControllerPort(Integer.toString(_conf.getControllerPort() + 1));
      _helixResourceManager = new PinotHelixResourceManager(controllerConf);
      String instanceId = controllerConf.getControllerHost() + "_" + controllerConf.getControllerPort();
      HelixManager helixManager = registerAndConnectAsHelixSpectator(instanceId);
      _helixResourceManager.start(helixManager, null);
    }

    // Create server tenants if required
    if (_conf.shouldStartServer()) {
      Tenant serverTenant = new Tenant(TenantRole.SERVER, _serverTenantName, 1, 1, 0);
      _helixResourceManager.createServerTenant(serverTenant);
    }

    // Create broker tenant if required
    if (_conf.shouldStartBroker()) {
      Tenant brokerTenant = new Tenant(TenantRole.BROKER, _brokerTenantName, 1, 0, 0);
      _helixResourceManager.createBrokerTenant(brokerTenant);
    }
  }

  /**
   * Register and connect to Helix cluster as Spectator role.
   */
  private HelixManager registerAndConnectAsHelixSpectator(String instanceId) {
    HelixManager helixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, instanceId, InstanceType.SPECTATOR, _zkAddress);

    try {
      helixManager.connect();
      return helixManager;
    } catch (Exception e) {
      String errorMsg =
          String.format("Exception when connecting the instance %s as Spectator role to Helix.", instanceId);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg);
    }
  }

  private void configureResources()
      throws Exception {
    if (!_conf.isConfigureResources()) {
      LOGGER.info("Skipping configure resources step.");
      return;
    }
    String tableName = _conf.getTableName();
    configureTable(tableName);
  }

  public void configureTable(String tableName)
      throws Exception {
    configureTable(tableName, null, null);
  }

  public void configureTable(String tableName, List<String> invertedIndexColumns, List<String> bloomFilterColumns)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
        .setSegmentAssignmentStrategy(_segmentAssignmentStrategy).setNumReplicas(_numReplicas)
        .setBrokerTenant(_brokerTenantName).setServerTenant(_serverTenantName).setLoadMode(_loadMode)
        .setSegmentVersion(_segmentFormatVersion).setInvertedIndexColumns(invertedIndexColumns)
        .setBloomFilterColumns(bloomFilterColumns).build();
    _helixResourceManager.addTable(tableConfig);
  }

  /**
   * Add segment while segment data is already in server data directory.
   *
   * @param segmentMetadata segment metadata.
   */
  public void addSegment(String tableNameWithType, SegmentMetadata segmentMetadata) {
    _helixResourceManager.addNewSegment(tableNameWithType, segmentMetadata,
        "http://" + _controllerAddress + "/" + segmentMetadata.getName());
  }

  public static void waitForExternalViewUpdate(String zkAddress, final String clusterName, long timeoutInMilliseconds) {
    final ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkAddress);

    List<String> allResourcesInCluster = helixAdmin.getResourcesInCluster(clusterName);
    Set<String> tableAndBrokerResources = new HashSet<>();
    for (String resourceName : allResourcesInCluster) {
      // Only check table resources and broker resource
      if (TableNameBuilder.isTableResource(resourceName) || resourceName.equals(
          CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
        tableAndBrokerResources.add(resourceName);
      }
    }

    StrictMatchExternalViewVerifier verifier =
        new StrictMatchExternalViewVerifier.Builder(clusterName).setZkAddr(zkAddress)
            .setResources(tableAndBrokerResources).build();

    boolean success = verifier.verify(timeoutInMilliseconds);
    if (success) {
      LOGGER.info("Cluster is ready to serve queries");
    }
  }

  private void postQueries()
      throws Exception {
    if (!_conf.isRunQueries()) {
      LOGGER.info("Skipping run queries step.");
      return;
    }
    String queriesDirectory = _conf.getQueriesDirectory();
    File[] queryFiles = new File(queriesDirectory).listFiles();
    Preconditions.checkNotNull(queryFiles);
    for (File queryFile : queryFiles) {
      if (!queryFile.getName().endsWith(".txt")) {
        continue;
      }
      LOGGER.info("Running queries from: {}", queryFile);
      try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
        String query;
        while ((query = reader.readLine()) != null) {
          postQuery(query);
        }
      }
    }
  }

  public JsonNode postQuery(String query)
      throws Exception {
    return postQuery(query, Collections.emptyMap());
  }

  public JsonNode postQuery(String query, Map<String, String> headers)
      throws Exception {
    ObjectNode requestJson = JsonUtils.newObjectNode();
    requestJson.put("sql", query);

    long start = System.currentTimeMillis();
    URLConnection conn = new URL(_queryUrl).openConnection();
    conn.setDoOutput(true);
    for (Map.Entry<String, String> header : headers.entrySet()) {
      conn.setRequestProperty(header.getKey(), header.getValue());
    }
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8))) {
      String requestString = requestJson.toString();
      writer.write(requestString);
      writer.flush();

      try {
        StringBuilder stringBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
          }
        }

        long totalTime = System.currentTimeMillis() - start;
        String responseString = stringBuilder.toString();
        ObjectNode responseJson = (ObjectNode) JsonUtils.stringToJsonNode(responseString);
        responseJson.put("totalTime", totalTime);

        if (_verbose) {
          if (!responseJson.has("exceptions") || responseJson.get("exceptions").size() <= 0) {
            LOGGER.info("requestString: {}", requestString);
            LOGGER.info("responseString: {}", responseString);
          } else {
            LOGGER.error("requestString: {}", requestString);
            LOGGER.error("responseString: {}", responseString);
          }
        }

        return responseJson;
      } catch (Exception e) {
        LOGGER.error("requestString: {}", requestString);
        throw e;
      }
    }
  }

  /**
   * Start cluster components with default configuration.
   *
   * @param isStartZookeeper whether to start zookeeper.
   * @param isStartController whether to start controller.
   * @param isStartBroker whether to start broker.
   * @param isStartServer whether to start server.
   * @return perf benchmark driver.
   * @throws Exception
   */
  public static PerfBenchmarkDriver startComponents(boolean isStartZookeeper, boolean isStartController,
      boolean isStartBroker, boolean isStartServer, @Nullable String serverDataDir)
      throws Exception {
    PerfBenchmarkDriverConf conf = new PerfBenchmarkDriverConf();
    conf.setStartZookeeper(isStartZookeeper);
    conf.setStartController(isStartController);
    conf.setStartBroker(isStartBroker);
    conf.setStartServer(isStartServer);
    conf.setServerInstanceDataDir(serverDataDir);
    PerfBenchmarkDriver driver = new PerfBenchmarkDriver(conf);
    driver.run();
    return driver;
  }

  public static void main(String[] args)
      throws Exception {
    PluginManager.get().init();
    PerfBenchmarkDriverConf conf = new Yaml().load(new FileInputStream(args[0]));
    PerfBenchmarkDriver perfBenchmarkDriver = new PerfBenchmarkDriver(conf);
    perfBenchmarkDriver.run();
  }
}
