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
package com.linkedin.pinot.tools.perf;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.Tenant.TenantBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


@SuppressWarnings("FieldCanBeLocal")
public class PerfBenchmarkDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerfBenchmarkDriver.class);
  private static final long BROKER_TIMEOUT_MS = 60_000L;

  private final PerfBenchmarkDriverConf _conf;
  private final String _zkAddress;
  private final String _clusterName;
  private final String _tempDir;
  private final String _loadMode;
  private final String _segmentFormatVersion;
  private final boolean _verbose;

  private String _controllerHost;
  private int _controllerPort;
  private String _controllerAddress;
  private String _controllerDataDir;

  private String _brokerBaseApiUrl;

  private String _serverInstanceDataDir;
  private String _serverInstanceSegmentTarDir;
  private String _serverInstanceName;

  // TODO: read from configuration.
  private final int _numReplicas = 1;
  private final String _segmentAssignmentStrategy = "BalanceNumSegmentAssignmentStrategy";
  private final String _brokerTenantName = "DefaultTenant";
  private final String _serverTenantName = "DefaultTenant";

  private PinotHelixResourceManager _helixResourceManager;

  public PerfBenchmarkDriver(PerfBenchmarkDriverConf conf) {
    this(conf, "/tmp/", "HEAP", null, false);
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
    _brokerBaseApiUrl = "http://" + _conf.getBrokerHost() + ":" + _conf.getBrokerPort();

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
    uploadIndexSegments();
    waitForExternalViewUpdate(_zkAddress, _clusterName, 60 * 1000L);
    postQueries();
  }

  private void startZookeeper()
      throws Exception {
    int zkPort = _conf.getZkPort();
    if (!_conf.isStartZookeeper()) {
      LOGGER.info("Skipping start zookeeper step. Assumes zookeeper is already started.");
      return;
    }
    ZookeeperLauncher launcher = new ZookeeperLauncher(_tempDir);
    launcher.start(zkPort);
  }

  private void startController() {
    if (!_conf.shouldStartController()) {
      LOGGER.info("Skipping start controller step. Assumes controller is already started.");
      return;
    }
    ControllerConf conf = getControllerConf();
    LOGGER.info("Starting controller at {}", _controllerAddress);
    new ControllerStarter(conf).start();
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
    conf.setControllerVipProtocol("http");
    return conf;
  }

  private void startBroker()
      throws Exception {
    if (!_conf.isStartBroker()) {
      LOGGER.info("Skipping start broker step. Assumes broker is already started.");
      return;
    }
    Configuration brokerConfiguration = new PropertiesConfiguration();
    String brokerInstanceName = "Broker_localhost_" + CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;
    brokerConfiguration.setProperty(CommonConstants.Helix.Instance.INSTANCE_ID_KEY, brokerInstanceName);
    brokerConfiguration.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS, BROKER_TIMEOUT_MS);
    LOGGER.info("Starting broker instance: {}", brokerInstanceName);
    new HelixBrokerStarter(_clusterName, _zkAddress, brokerConfiguration);
  }

  private void startServer()
      throws Exception {
    if (!_conf.shouldStartServer()) {
      LOGGER.info("Skipping start server step. Assumes server is already started.");
      return;
    }
    Configuration serverConfiguration = new PropertiesConfiguration();
    serverConfiguration.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR, _serverInstanceDataDir);
    serverConfiguration.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
        _serverInstanceSegmentTarDir);
    if (_segmentFormatVersion != null) {
      serverConfiguration.setProperty(CommonConstants.Server.CONFIG_OF_SEGMENT_FORMAT_VERSION, _segmentFormatVersion);
    }
    serverConfiguration.setProperty(CommonConstants.Helix.Instance.INSTANCE_ID_KEY, _serverInstanceName);
    LOGGER.info("Starting server instance: {}", _serverInstanceName);
    new HelixServerStarter(_clusterName, _zkAddress, serverConfiguration);
  }

  private void startHelixResourceManager()
      throws Exception {
    _helixResourceManager = new PinotHelixResourceManager(getControllerConf());
    _helixResourceManager.start();

    // Create broker tenant.
    Tenant brokerTenant = new TenantBuilder(_brokerTenantName).setRole(TenantRole.BROKER).setTotalInstances(1).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Create server tenant.
    Tenant serverTenant = new TenantBuilder(_serverTenantName).setRole(TenantRole.SERVER)
        .setTotalInstances(1)
        .setOfflineInstances(1)
        .build();
    _helixResourceManager.createServerTenant(serverTenant);
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
    configureTable(tableName, null);
  }

  public void configureTable(String tableName, List<String> invertedIndexColumns)
      throws Exception {
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName)
        .setSegmentAssignmentStrategy(_segmentAssignmentStrategy)
        .setNumReplicas(_numReplicas)
        .setBrokerTenant(_brokerTenantName)
        .setServerTenant(_serverTenantName)
        .setLoadMode(_loadMode)
        .setSegmentVersion(_segmentFormatVersion)
        .setInvertedIndexColumns(invertedIndexColumns)
        .build();
    _helixResourceManager.addTable(tableConfig);
  }

  private void uploadIndexSegments() throws Exception {
    if (!_conf.isUploadIndexes()) {
      LOGGER.info("Skipping upload index segments step.");
      return;
    }
    String indexDirectory = _conf.getIndexDirectory();
    File[] indexFiles = new File(indexDirectory).listFiles();
    Preconditions.checkNotNull(indexFiles);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentHttpURI(_controllerHost, _controllerPort);
      for (File indexFile : indexFiles) {
        LOGGER.info("Uploading index segment: {}", indexFile.getAbsolutePath());
        fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, indexFile.getName(), indexFile);
      }
    }
  }

  /**
   * Add segment while segment data is already in server data directory.
   *
   * @param segmentMetadata segment metadata.
   */
  public void addSegment(SegmentMetadata segmentMetadata) {
    _helixResourceManager.addNewSegment(segmentMetadata, "http://" + _controllerAddress + "/" + segmentMetadata.getName());
  }

  public static void waitForExternalViewUpdate(String zkAddress, final String clusterName, long timeoutInMilliseconds) {
    final ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkAddress);

    List<String> allResourcesInCluster = helixAdmin.getResourcesInCluster(clusterName);
    Set<String> tableAndBrokerResources = new HashSet<>();
    for (String resourceName : allResourcesInCluster) {
      // Only check table resources and broker resource
      if (!TableNameBuilder.isTableResource(resourceName) && !resourceName.equals(
          CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
        continue;
      }
      tableAndBrokerResources.add(resourceName);
    }

    StrictMatchExternalViewVerifier verifier = new StrictMatchExternalViewVerifier.Builder(clusterName)
        .setZkAddr(zkAddress)
        .setResources(tableAndBrokerResources)
        .build();

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

  public JSONObject postQuery(String query)
      throws Exception {
    return postQuery(query, null);
  }

  public JSONObject postQuery(String query, String optimizationFlags)
      throws Exception {
    JSONObject requestJson = new JSONObject();
    requestJson.put("pql", query);

    if (optimizationFlags != null && !optimizationFlags.isEmpty()) {
      requestJson.put("debugOptions", "optimizationFlags=" + optimizationFlags);
    }

    long start = System.currentTimeMillis();
    URLConnection conn = new URL(_brokerBaseApiUrl + "/query").openConnection();
    conn.setDoOutput(true);

    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"))) {
      String requestString = requestJson.toString();
      writer.write(requestString);
      writer.flush();

      StringBuilder stringBuilder = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"))) {
        String line;
        while ((line = reader.readLine()) != null) {
          stringBuilder.append(line);
        }
      }

      long totalTime = System.currentTimeMillis() - start;
      String responseString = stringBuilder.toString();
      JSONObject responseJson = new JSONObject(responseString);
      responseJson.put("totalTime", totalTime);

      if (_verbose && (responseJson.getLong("numDocsScanned") > 0)) {
        LOGGER.info("requestString: {}", requestString);
        LOGGER.info("responseString: {}", responseString);
      }
      return responseJson;
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
    PerfBenchmarkDriverConf conf = (PerfBenchmarkDriverConf) new Yaml().load(new FileInputStream(args[0]));
    PerfBenchmarkDriver perfBenchmarkDriver = new PerfBenchmarkDriver(conf);
    perfBenchmarkDriver.run();
  }
}
