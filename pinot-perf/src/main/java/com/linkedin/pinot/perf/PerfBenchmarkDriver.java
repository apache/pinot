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
package com.linkedin.pinot.perf;

import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.Tenant.TenantBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.Verifier;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class PerfBenchmarkDriver {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerfBenchmarkDriver.class);

  private PerfBenchmarkDriverConf conf;

  final String zkAddress;
  final String clusterName;

  private ControllerStarter controllerStarter;

  private String controllerHost;

  private int controllerPort;

  private String controllerDataDir;

  private String brokerBaseApiUrl;

  private String serverInstanceDataDir;

  private String serverInstanceSegmentTarDir;

  private String serverInstanceName;

  private PinotHelixResourceManager helixResourceManager;

  private boolean verbose = false;

  public PerfBenchmarkDriver(PerfBenchmarkDriverConf conf) {
    this.conf = conf;
    zkAddress = conf.getZkHost() + ":" + conf.getZkPort();
    clusterName = conf.getClusterName();
    init();
  }

  private void init() {
    controllerHost = "localhost";
    if (conf.getControllerHost() != null) {
      controllerHost = conf.getControllerHost();
    }
    controllerPort = 8300;
    if (conf.getControllerPort() > 0) {
      controllerPort = conf.getControllerPort();
    }
    String controllerInstanceName = controllerHost + ":" + controllerPort;
    controllerDataDir = "/tmp/controller/" + controllerInstanceName + "/controller_data_dir";
    if (conf.getControllerDataDir() != null) {
      controllerDataDir = conf.getControllerDataDir();
    }

    //broker init
    brokerBaseApiUrl = "http://" + conf.getBrokerHost() + ":" + conf.getBrokerPort();

    serverInstanceName = "Server_localhost_" + CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    if (conf.getServerInstanceName() != null) {
      serverInstanceName = conf.getServerInstanceName();
    }
    serverInstanceDataDir = "/tmp/server/" + serverInstanceName + "/index_data_dir";
    if (conf.getServerInstanceDataDir() != null) {
      serverInstanceDataDir = conf.getServerInstanceDataDir();
    }
    serverInstanceSegmentTarDir = "/tmp/pinot/server/" + serverInstanceName + "/segment_tar_dir";
    if (conf.getServerInstanceSegmentTarDir() != null) {
      serverInstanceSegmentTarDir = conf.getServerInstanceSegmentTarDir();
    }
  }

  public void run() throws Exception {

    startZookeeper();

    startController();

    startBroker();

    startServer();

    configureResources();

    uploadIndexSegments();

    final ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkAddress);
    Verifier customVerifier = new Verifier() {

      @Override
      public boolean verify() {
        List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(clusterName);
        LOGGER.info("Waiting for the cluster to be set up and indexes to be loaded on the servers"
            + new Timestamp(System.currentTimeMillis()));
        for (String resourceName : resourcesInCluster) {
          IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
          ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, resourceName);
          if (idealState == null || externalView == null) {
            return false;
          }
          Set<String> partitionSet = idealState.getPartitionSet();
          for (String partition : partitionSet) {
            Map<String, String> instanceStateMapIS = idealState.getInstanceStateMap(partition);
            Map<String, String> instanceStateMapEV = externalView.getStateMap(partition);
            if (instanceStateMapIS == null || instanceStateMapEV == null) {
              return false;
            }
            if (!instanceStateMapIS.equals(instanceStateMapEV)) {
              return false;
            }
          }
        }
        LOGGER.info("Cluster is ready to serve queries");
        return true;
      }
    };
    ClusterStateVerifier.verifyByPolling(customVerifier, 60 * 1000);

    postQueries();
  }

  public void startZookeeper() throws Exception {
    int zkPort = conf.getZkPort();
    // START ZOOKEEPER
    if (!conf.isStartZookeeper()) {
      LOGGER.info("Skipping start zookeeper step. Assumes zookeeper is already started");
      return;
    }
    ZookeeperLauncher launcher = new ZookeeperLauncher();
    launcher.start(zkPort);
  }

  public void createAndConfigureHelixCluster() {
    HelixSetupUtils.createHelixClusterIfNeeded(clusterName, zkAddress);
  }

  public void startBroker() throws Exception {

    if (!conf.isStartBroker()) {
      LOGGER.info("Skipping start broker step. Assumes broker is already started");
      return;
    }
    String brokerInstanceName = "Broker_localhost_" + CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;

    Configuration brokerConfiguration = new PropertiesConfiguration();
    brokerConfiguration.setProperty("instanceId", brokerInstanceName);
    HelixBrokerStarter helixBrokerStarter = new HelixBrokerStarter(clusterName, zkAddress, brokerConfiguration);
  }

  public void startServer() throws Exception {

    if (!conf.shouldStartServer()) {
      LOGGER.info("Skipping start server step. Assumes server is already started");
      return;
    }
    Configuration serverConfiguration = new PropertiesConfiguration();
    serverConfiguration.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR.toString(),
        serverInstanceDataDir);

    serverConfiguration.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR.toString(),
        serverInstanceSegmentTarDir);
    serverConfiguration.setProperty("instanceId", serverInstanceName);

    HelixServerStarter helixServerStarter = new HelixServerStarter(clusterName, zkAddress, serverConfiguration);

  }

  public void startController() {

    if (!conf.shouldStartController()) {
      LOGGER.info("Skipping start controller step. Assumes controller is already started");
      return;
    }
    ControllerConf conf = getControllerConf();
    controllerStarter = new ControllerStarter(conf);
    LOGGER.info("Starting controller at port: " + conf.getControllerPort());
    controllerStarter.start();
  }

  private ControllerConf getControllerConf() {
    ControllerConf conf = new ControllerConf();
    conf.setHelixClusterName(clusterName);
    conf.setZkStr(zkAddress);
    conf.setControllerHost(controllerHost);
    conf.setControllerPort(String.valueOf(controllerPort));
    conf.setDataDir(controllerDataDir);
    conf.setTenantIsolationEnabled(false);
    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol("http");
    return conf;
  }

  public void configureResources() throws Exception {
    if (!conf.isConfigureResources()) {
      LOGGER.info("Skipping configure resources step");
      return;
    }
    String tableName = conf.getTableName();
    configureTable(tableName);
  }

  public void configureTable(String tableName) throws Exception {
    configureTable(tableName, new ArrayList<String>());
  }

  public void configureTable(String tableName, List<String> invertedIndexColumns) throws Exception {

    //TODO:Get these from configuration
    int numInstances = 1;
    int numReplicas = 1;
    String segmentAssignmentStrategy = "BalanceNumSegmentAssignmentStrategy";
    String brokerTenantName = "DefaultTenant";
    String serverTenantName = "DefaultTenant";
    // create broker tenant
    Tenant brokerTenant =
        new TenantBuilder(brokerTenantName).setRole(TenantRole.BROKER).setTotalInstances(numInstances).build();
    helixResourceManager = new PinotHelixResourceManager(getControllerConf());
    helixResourceManager.start();
    helixResourceManager.createBrokerTenant(brokerTenant);
    // create server tenant
    Tenant serverTenant = new TenantBuilder(serverTenantName).setRole(TenantRole.SERVER).setTotalInstances(numInstances)
        .setOfflineInstances(numInstances).build();
    helixResourceManager.createServerTenant(serverTenant);
    // upload schema

    // create table
    String jsonString = ControllerRequestBuilderUtil.buildCreateOfflineTableJSON(tableName, serverTenantName,
        brokerTenantName, numReplicas, segmentAssignmentStrategy).toString();
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(jsonString);
    offlineTableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    offlineTableConfig.getValidationConfig().setRetentionTimeValue("");
    IndexingConfig indexingConfig = offlineTableConfig.getIndexingConfig();
    if (invertedIndexColumns != null && !invertedIndexColumns.isEmpty()) {
      indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
    }
    helixResourceManager.addTable(offlineTableConfig);
  }

  public void addSegment(SegmentMetadata metadata) {
    helixResourceManager.addSegment(metadata,
        "http://" + controllerHost + ":" + controllerPort + "/" + metadata.getName());
  }

  public void uploadIndexSegments() throws Exception {
    if (!conf.isUploadIndexes()) {
      LOGGER.info("Skipping upload Indexes step");
      return;
    }
    String indexDirectory = conf.getIndexDirectory();
    File file = new File(indexDirectory);
    File[] listFiles = file.listFiles();
    for (File indexFile : listFiles) {
      LOGGER.info("Uploading index segment " + indexFile.getAbsolutePath());
      FileUploadUtils.sendSegmentFile(controllerHost, "" + controllerPort, indexFile.getName(),
          new FileInputStream(indexFile), indexFile.length());
    }
  }

  public void postQueries() throws Exception {
    if (!conf.isRunQueries()) {
      LOGGER.info("Skipping run queries step");
      return;
    }
    String queriesDirectory = conf.getQueriesDirectory();
    File[] queryFiles = new File(queriesDirectory).listFiles();
    for (File file : queryFiles) {
      if (!file.getName().endsWith(".txt")) {
        continue;
      }
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String query;
      LOGGER.info("Running queries from " + file);
      while ((query = reader.readLine()) != null) {
        postQuery(query);
      }
      reader.close();
    }
  }

  public JSONObject postQuery(String query) throws Exception {
    return postQuery(query, null);
  }

  public JSONObject postQuery(String query, String optimizationFlags) throws Exception {
    final JSONObject json = new JSONObject();
    json.put("pql", query);

    if (optimizationFlags != null && !optimizationFlags.isEmpty()) {
      json.put("debugOptions", "optimizationFlags=" + optimizationFlags);
    }

    final long start = System.currentTimeMillis();
    final URLConnection conn = new URL(brokerBaseApiUrl + "/query").openConnection();
    conn.setDoOutput(true);
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    final String reqStr = json.toString();

    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    final String res = sb.toString();
    final JSONObject ret = new JSONObject(res);
    ret.put("totalTime", (stop - start));
    if ((ret.getLong("numDocsScanned") > 0) && verbose) {
      LOGGER.info("reqStr = " + reqStr);
      LOGGER.info(" Client side time in ms:" + (stop - start));
      LOGGER.info("numDocScanned : " + ret.getLong("numDocsScanned"));
      LOGGER.info("timeUsedMs : " + ret.getLong("timeUsedMs"));
      LOGGER.info("totalTime : " + ret.getLong("totalTime"));
      LOGGER.info("res = " + res);
    }
    return ret;
  }

  public static void main(String[] args) throws Exception {
    PerfBenchmarkDriverConf conf = (PerfBenchmarkDriverConf) new Yaml().load(new FileInputStream(args[0]));
    PerfBenchmarkDriver perfBenchmarkDriver = new PerfBenchmarkDriver(conf);
    perfBenchmarkDriver.run();
  }

}
