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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.BootstrapTableTool;
import org.apache.pinot.tools.QuickstartTableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuickstartRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuickstartRunner.class.getName());
  private static final Random RANDOM = new Random();
  private static final String CLUSTER_NAME = "QuickStartCluster";

  private static final int ZK_PORT = 2123;
  private static final String ZK_ADDRESS = "localhost:" + ZK_PORT;

  private static final int DEFAULT_CONTROLLER_PORT = 9000;
  private static final int DEFAULT_BROKER_PORT = 8000;
  private static final int DEFAULT_SERVER_ADMIN_API_PORT = 7500;
  private static final int DEFAULT_SERVER_NETTY_PORT = 7050;
  private static final int DEFAULT_SERVER_GRPC_PORT = 7100;
  private static final int DEFAULT_MINION_PORT = 6000;

  private static final String DEFAULT_ZK_DIR = "PinotZkDir";
  private static final String DEFAULT_CONTROLLER_DIR = "PinotControllerDir";
  private static final String DEFAULT_SERVER_DATA_DIR = "PinotServerDataDir";
  private static final String DEFAULT_SERVER_SEGMENT_DIR = "PinotServerSegmentDir";

  private final List<QuickstartTableRequest> _tableRequests;
  private final int _numControllers;
  private final int _numBrokers;
  private final int _numServers;
  private final int _numMinions;
  private final File _tempDir;
  private final boolean _enableTenantIsolation;
  private final AuthProvider _authProvider;
  private final Map<String, Object> _configOverrides;
  private final boolean _deleteExistingData;

  // If this field is non-null, an embedded Zookeeper instance will not be launched
  private final String _zkExternalAddress;

  private final List<Integer> _controllerPorts = new ArrayList<>();
  private final List<Integer> _brokerPorts = new ArrayList<>();
  private boolean _isStopped = false;

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numControllers, int numBrokers,
      int numServers, int numMinions, File tempDir, Map<String, Object> configOverrides)
      throws Exception {
    this(tableRequests, numControllers, numBrokers, numServers, numMinions, tempDir, true, null, configOverrides, null,
        true);
  }

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numControllers, int numBrokers,
      int numServers, int numMinions, File tempDir, boolean enableIsolation, AuthProvider authProvider,
      Map<String, Object> configOverrides, String zkExternalAddress, boolean deleteExistingData)
      throws Exception {
    _tableRequests = tableRequests;
    _numControllers = numControllers;
    _numBrokers = numBrokers;
    _numServers = numServers;
    _numMinions = numMinions;
    _tempDir = tempDir;
    _enableTenantIsolation = enableIsolation;
    _authProvider = authProvider;
    _configOverrides = new HashMap<>(configOverrides);
    if (numMinions > 0) {
      // configure the controller to schedule tasks when minion is enabled
      _configOverrides.put("controller.task.scheduler.enabled", true);
      _configOverrides.put("controller.task.skipLateCronSchedule", true);
    }
    _zkExternalAddress = zkExternalAddress;
    _deleteExistingData = deleteExistingData;
    if (deleteExistingData) {
      clean();
    }
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
      controllerStarter.setControllerPort(String.valueOf(DEFAULT_CONTROLLER_PORT + i))
          .setZkAddress(_zkExternalAddress != null ? _zkExternalAddress : ZK_ADDRESS).setClusterName(CLUSTER_NAME)
          .setTenantIsolation(_enableTenantIsolation)
          .setDataDir(new File(_tempDir, DEFAULT_CONTROLLER_DIR + i).getAbsolutePath())
          .setConfigOverrides(_configOverrides);
      if (!controllerStarter.execute()) {
        throw new RuntimeException("Failed to start Controller");
      }
      _controllerPorts.add(DEFAULT_CONTROLLER_PORT + i);
    }
  }

  private void startBrokers()
      throws Exception {
    for (int i = 0; i < _numBrokers; i++) {
      StartBrokerCommand brokerStarter = new StartBrokerCommand();
      brokerStarter.setPort(DEFAULT_BROKER_PORT + i)
          .setZkAddress(_zkExternalAddress != null ? _zkExternalAddress : ZK_ADDRESS).setClusterName(CLUSTER_NAME)
          .setConfigOverrides(_configOverrides);
      if (!brokerStarter.execute()) {
        throw new RuntimeException("Failed to start Broker");
      }
      _brokerPorts.add(DEFAULT_BROKER_PORT + i);
    }
  }

  private void startServers()
      throws Exception {
    for (int i = 0; i < _numServers; i++) {
      StartServerCommand serverStarter = new StartServerCommand();
      serverStarter.setPort(DEFAULT_SERVER_NETTY_PORT + i).setAdminPort(DEFAULT_SERVER_ADMIN_API_PORT + i)
          .setGrpcPort(DEFAULT_SERVER_GRPC_PORT + i)
          .setZkAddress(_zkExternalAddress != null ? _zkExternalAddress : ZK_ADDRESS).setClusterName(CLUSTER_NAME)
          .setDataDir(new File(_tempDir, DEFAULT_SERVER_DATA_DIR + i).getAbsolutePath())
          .setSegmentDir(new File(_tempDir, DEFAULT_SERVER_SEGMENT_DIR + i).getAbsolutePath())
          .setConfigOverrides(_configOverrides);
      if (!serverStarter.execute()) {
        throw new RuntimeException("Failed to start Server");
      }
    }
  }

  private void startMinions()
      throws Exception {
    for (int i = 0; i < _numMinions; i++) {
      StartMinionCommand minionStarter = new StartMinionCommand();
      minionStarter.setMinionPort(DEFAULT_MINION_PORT + i)
          .setZkAddress(_zkExternalAddress != null ? _zkExternalAddress : ZK_ADDRESS).setClusterName(CLUSTER_NAME)
          .setConfigOverrides(_configOverrides);
      if (!minionStarter.execute()) {
        throw new RuntimeException("Failed to start Minion");
      }
    }
  }

  private void clean()
      throws Exception {
    FileUtils.cleanDirectory(_tempDir);
  }

  public void startAll()
      throws Exception {
    registerDefaultPinotFS();
    if (_zkExternalAddress == null) {
      startZookeeper();
    }
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

    // TODO: Stop Minion
    StopProcessCommand stopper = new StopProcessCommand(false);
    if (_zkExternalAddress == null) {
      stopper.stopController().stopBroker().stopServer().stopZookeeper();
    }
    stopper.execute();
    if (_deleteExistingData) {
      clean();
    }
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
      if (!new BootstrapTableTool("http", "localhost", _controllerPorts.get(0),
          request.getBootstrapTableDir(), _authProvider).execute()) {
        throw new RuntimeException("Failed to bootstrap table with request - " + request);
      }
    }
  }

  public JsonNode runQuery(String query)
      throws Exception {
    return runQuery(query, Collections.emptyMap());
  }

  public JsonNode runQuery(String query, Map<String, String> additionalOptions)
      throws Exception {
    int brokerPort = _brokerPorts.get(RANDOM.nextInt(_brokerPorts.size()));
    return JsonUtils.stringToJsonNode(
        new PostQueryCommand().setBrokerPort(String.valueOf(brokerPort)).setAuthProvider(_authProvider)
            .setAdditionalOptions(additionalOptions).setQuery(query).run());
  }

  public static void registerDefaultPinotFS() {
    registerPinotFS("s3", "org.apache.pinot.plugin.filesystem.S3PinotFS",
        ImmutableMap.of("region", System.getProperty("AWS_REGION", "us-west-2")));
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
      LOGGER.error("Unable to init PinotFS for scheme: {}, class name: {}, configs: {}", scheme, fsClassName, configs,
          e);
    }
  }
}
