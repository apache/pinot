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
package org.apache.pinot.tools.admin.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.QuickstartTableRequest;
import org.apache.pinot.tools.BootstrapTableTool;
import org.apache.pinot.tools.utils.JarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class QuickstartRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickstartRunner.class.getName());
  private static final Random RANDOM = new Random();
  private static final String CLUSTER_NAME = "QuickStartCluster";

  private static final int ZK_PORT = 2123;
  private static final String ZK_ADDRESS = "localhost:" + ZK_PORT;

  private static final int DEFAULT_SERVER_NETTY_PORT = 7000;
  private static final int DEFAULT_SERVER_ADMIN_API_PORT = 7500;
  private static final int DEFAULT_BROKER_PORT = 8000;
  private static final int DEFAULT_CONTROLLER_PORT = 9000;
  private static final int DEFAULT_MINION_PORT = 6000;


  private static final String DEFAULT_ZK_DIR = "PinotZkDir";
  private static final String DEFAULT_CONTROLLER_DIR = "PinotControllerDir";
  private static final String DEFAULT_SERVER_DATA_DIR = "PinotServerDataDir";
  private static final String DEFAULT_SERVER_SEGMENT_DIR = "PinotServerSegmentDir";

  private final List<QuickstartTableRequest> _tableRequests;
  private final int _numServers;
  private final int _numBrokers;
  private final int _numControllers;
  private final int _numMinions;
  private final File _tempDir;
  private final boolean _enableTenantIsolation;

  private final List<Integer> _brokerPorts = new ArrayList<>();
  private final List<Integer> _controllerPorts = new ArrayList<>();
  private final List<String> _segmentDirs = new ArrayList<>();
  private boolean _isStopped = false;

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numServers, int numBrokers,
      int numControllers, File tempDir, boolean enableIsolation)
      throws Exception {
    this(tableRequests, numServers, numBrokers, numControllers, 1, tempDir, enableIsolation);
  }

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numServers, int numBrokers,
      int numControllers, int numMinions, File tempDir, boolean enableIsolation)
      throws Exception {
    _tableRequests = tableRequests;
    _numServers = numServers;
    _numBrokers = numBrokers;
    _numControllers = numControllers;
    _numMinions = numMinions;
    _tempDir = tempDir;
    _enableTenantIsolation = enableIsolation;
    clean();
  }

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numServers, int numBrokers,
      int numControllers, File tempDir)
      throws Exception {
    this(tableRequests, numServers, numBrokers, numControllers, tempDir, true);
  }

  private void startZookeeper()
      throws IOException {
    StartZookeeperCommand zkStarter = new StartZookeeperCommand();
    zkStarter.setPort(ZK_PORT);
    zkStarter.setDataDir(new File(_tempDir, DEFAULT_ZK_DIR).getAbsolutePath());
    zkStarter.execute();
  }

  private void startControllers()
      throws Exception {
    for (int i = 0; i < _numControllers; i++) {
      StartControllerCommand controllerStarter = new StartControllerCommand();
      controllerStarter.setControllerPort(String.valueOf(DEFAULT_CONTROLLER_PORT + i)).setZkAddress(ZK_ADDRESS)
          .setClusterName(CLUSTER_NAME).setTenantIsolation(_enableTenantIsolation)
          .setDataDir(new File(_tempDir, DEFAULT_CONTROLLER_DIR + i).getAbsolutePath());
      controllerStarter.execute();
      _controllerPorts.add(DEFAULT_CONTROLLER_PORT + i);
    }
  }

  private void startBrokers()
      throws Exception {
    for (int i = 0; i < _numBrokers; i++) {
      StartBrokerCommand brokerStarter = new StartBrokerCommand();
      brokerStarter.setPort(DEFAULT_BROKER_PORT + i).setZkAddress(ZK_ADDRESS).setClusterName(CLUSTER_NAME);
      brokerStarter.execute();
      _brokerPorts.add(DEFAULT_BROKER_PORT + i);
    }
  }

  private void startServers()
      throws Exception {
    for (int i = 0; i < _numServers; i++) {
      StartServerCommand serverStarter = new StartServerCommand();
      serverStarter.setPort(DEFAULT_SERVER_NETTY_PORT + i).setAdminPort(DEFAULT_SERVER_ADMIN_API_PORT + i)
          .setZkAddress(ZK_ADDRESS).setClusterName(CLUSTER_NAME)
          .setDataDir(new File(_tempDir, DEFAULT_SERVER_DATA_DIR + i).getAbsolutePath())
          .setSegmentDir(new File(_tempDir, DEFAULT_SERVER_SEGMENT_DIR + i).getAbsolutePath());
      serverStarter.execute();
    }
  }

  private void startMinions()
      throws Exception {
    for (int i = 0; i < _numMinions; i++) {
      StartMinionCommand minionStarter = new StartMinionCommand();
      minionStarter.setMinionPort(DEFAULT_MINION_PORT + i)
          .setZkAddress(ZK_ADDRESS).setClusterName(CLUSTER_NAME);
      minionStarter.execute();
    }
  }

  private void clean()
      throws Exception {
    FileUtils.cleanDirectory(_tempDir);
  }

  public void startAll()
      throws Exception {
    registerDefaultPinotFS();
    startZookeeper();
    startControllers();
    startBrokers();
    startServers();
    startMinions();
  }

  public void stop()
      throws Exception {
    if (_isStopped) {
      return;
    }

    StopProcessCommand stopper = new StopProcessCommand(false);
    stopper.stopController().stopBroker().stopServer().stopZookeeper();
    stopper.execute();
    clean();

    _isStopped = true;
  }

  public void createServerTenantWith(int numOffline, int numRealtime, String tenantName)
      throws Exception {
    new AddTenantCommand().setControllerUrl("http://localhost:" + _controllerPorts.get(0)).setName(tenantName)
        .setOffline(numOffline).setRealtime(numRealtime).setInstances(numOffline + numRealtime)
        .setRole(TenantRole.SERVER).setExecute(true).execute();
  }

  public void createBrokerTenantWith(int number, String tenantName)
      throws Exception {
    new AddTenantCommand().setControllerUrl("http://localhost:" + _controllerPorts.get(0)).setName(tenantName)
        .setInstances(number).setRole(TenantRole.BROKER).setExecute(true).execute();
  }

  public void bootstrapTable()
      throws Exception {
    for (QuickstartTableRequest request : _tableRequests) {
      // TODO get protocol from somewhere?
      String protocol = CommonConstants.HTTP_PROTOCOL;
      if (!new BootstrapTableTool(protocol, InetAddress.getLocalHost().getHostName(), _controllerPorts.get(0),
          request.getBootstrapTableDir()).execute()) {
        throw new RuntimeException("Failed to bootstrap table with request - " + request);
      }
    }
  }

  @Deprecated
  public void addTable()
      throws Exception {
    for (QuickstartTableRequest request : _tableRequests) {
      new AddTableCommand().setSchemaFile(request.getSchemaFile().getAbsolutePath())
          .setTableConfigFile(request.getTableRequestFile().getAbsolutePath())
          .setControllerPort(String.valueOf(_controllerPorts.get(0))).setExecute(true).execute();
    }
  }

  @Deprecated
  public void launchDataIngestionJob()
      throws Exception {
    for (QuickstartTableRequest request : _tableRequests) {
      if (request.getTableType() == TableType.OFFLINE) {
        try (Reader reader = new BufferedReader(new FileReader(request.getIngestionJobFile().getAbsolutePath()))) {
          SegmentGenerationJobSpec spec = new Yaml().loadAs(reader, SegmentGenerationJobSpec.class);
          String inputDirURI = spec.getInputDirURI();
          if (!new File(inputDirURI).exists()) {
            URL resolvedInputDirURI = QuickstartRunner.class.getClassLoader().getResource(inputDirURI);
            if (resolvedInputDirURI.getProtocol().equals("jar")) {
              String[] splits = resolvedInputDirURI.getFile().split("!");
              String inputDir = new File(_tempDir, "inputData").toString();
              JarUtils.copyResourcesToDirectory(splits[0], splits[1].substring(1), inputDir);
              spec.setInputDirURI(inputDir);
            } else {
              spec.setInputDirURI(resolvedInputDirURI.toString());
            }
          }
          IngestionJobLauncher.runIngestionJob(spec);
        }
      }
    }
  }

  public JsonNode runQuery(String query)
      throws Exception {
    int brokerPort = _brokerPorts.get(RANDOM.nextInt(_brokerPorts.size()));
    return JsonUtils.stringToJsonNode(new PostQueryCommand().setBrokerPort(String.valueOf(brokerPort))
        .setQueryType(CommonConstants.Broker.Request.SQL).setQuery(query).run());
  }

  public static void registerDefaultPinotFS() {
    registerPinotFS("s3", "org.apache.pinot.plugin.filesystem.S3PinotFS", ImmutableMap.of("region", System.getProperty("AWS_REGION", "us-west-2")));
  }

  public static void registerPinotFS(String scheme, String fsClassName, Map<String, Object> configs) {
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      LOGGER.info("PinotFS for scheme: {} is already registered.", scheme);
      return;
    }
    try {
      PinotFSFactory.register(scheme, fsClassName, new PinotConfiguration(configs));
      LOGGER.info("Registered PinotFS for scheme: {}", scheme);
    } catch (Exception e) {
      LOGGER.info("Unable to init PinotFS for scheme: {}, class name: {}, configs: {}, Error: {}", scheme, fsClassName, configs, e);
    }
  }
}