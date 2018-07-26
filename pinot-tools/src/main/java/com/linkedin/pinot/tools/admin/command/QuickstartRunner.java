/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.tools.QuickstartTableRequest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;


public class QuickstartRunner {
  private static final Random RANDOM = new Random();
  private static final String CLUSTER_NAME = "QuickStartCluster";

  private static final int ZK_PORT = 2123;
  private static final String ZK_ADDRESS = "localhost:" + ZK_PORT;

  private static final int DEFAULT_SERVER_NETTY_PORT = 7000;
  private static final int DEFAULT_SERVER_ADMIN_API_PORT = 7500;
  private static final int DEFAULT_BROKER_PORT = 8000;
  private static final int DEFAULT_CONTROLLER_PORT = 9000;

  private final List<QuickstartTableRequest> _tableRequests;
  private final int _numServers;
  private final int _numBrokers;
  private final int _numControllers;
  private final File _tempDir;
  private final boolean _enableTenantIsolation;

  private final List<Integer> _brokerPorts = new ArrayList<>();
  private final List<Integer> _controllerPorts = new ArrayList<>();
  private final List<String> _segmentDirs = new ArrayList<>();
  private boolean _isStopped = false;

  public QuickstartRunner(List<QuickstartTableRequest> tableRequests, int numServers, int numBrokers,
      int numControllers, File tempDir, boolean enableIsolation)
      throws Exception {
    _tableRequests = tableRequests;
    _numServers = numServers;
    _numBrokers = numBrokers;
    _numControllers = numControllers;
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
    zkStarter.execute();
  }

  private void startControllers()
      throws Exception {
    for (int i = 0; i < _numControllers; i++) {
      StartControllerCommand controllerStarter = new StartControllerCommand();
      controllerStarter.setControllerPort(String.valueOf(DEFAULT_CONTROLLER_PORT + i))
          .setZkAddress(ZK_ADDRESS)
          .setClusterName(CLUSTER_NAME)
          .setTenantIsolation(_enableTenantIsolation);
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
      serverStarter.setPort(DEFAULT_SERVER_NETTY_PORT + i)
          .setAdminPort(DEFAULT_SERVER_ADMIN_API_PORT + i)
          .setZkAddress(ZK_ADDRESS)
          .setClusterName(CLUSTER_NAME)
          .setDataDir(new File(_tempDir, "PinotServerData" + i).getAbsolutePath())
          .setSegmentDir(new File(_tempDir, "PinotServerSegment" + i).getAbsolutePath());
      serverStarter.execute();
    }
  }

  private void clean()
      throws Exception {
    FileUtils.cleanDirectory(_tempDir);
  }

  public void startAll()
      throws Exception {
    startZookeeper();
    startControllers();
    startBrokers();
    startServers();
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
    new AddTenantCommand().setControllerUrl("http://localhost:" + _controllerPorts.get(0))
        .setName(tenantName)
        .setOffline(numOffline)
        .setRealtime(numRealtime)
        .setInstances(numOffline + numRealtime)
        .setRole(TenantRole.SERVER)
        .setExecute(true)
        .execute();
  }

  public void createBrokerTenantWith(int number, String tenantName)
      throws Exception {
    new AddTenantCommand().setControllerUrl("http://localhost:" + _controllerPorts.get(0))
        .setName(tenantName)
        .setInstances(number)
        .setRole(TenantRole.BROKER)
        .setExecute(true)
        .execute();
  }

  public void addSchema()
      throws Exception {
    for (QuickstartTableRequest request : _tableRequests) {
      new AddSchemaCommand().setControllerPort(String.valueOf(_controllerPorts.get(0)))
          .setSchemaFilePath(request.getSchemaFile().getAbsolutePath())
          .setExecute(true)
          .execute();
    }
  }

  public void addTable()
      throws Exception {
    for (QuickstartTableRequest request : _tableRequests) {
      new AddTableCommand().setFilePath(request.getTableRequestFile().getAbsolutePath())
          .setControllerPort(String.valueOf(_controllerPorts.get(0)))
          .setExecute(true)
          .execute();
    }
  }

  public void buildSegment()
      throws Exception {
    for (QuickstartTableRequest request : _tableRequests) {
      if (request.getTableType() == TableType.OFFLINE) {
        File tempDir = new File(_tempDir, request.getTableName() + "_segment");
        new CreateSegmentCommand().setDataDir(request.getDataDir().getAbsolutePath())
            .setFormat(request.getSegmentFileFormat())
            .setSchemaFile(request.getSchemaFile().getAbsolutePath())
            .setTableName(request.getTableName())
            .setSegmentName(request.getTableName() + "_" + System.currentTimeMillis())
            .setOutDir(tempDir.getAbsolutePath())
            .execute();
        _segmentDirs.add(tempDir.getAbsolutePath());
      }
    }
  }

  public void pushSegment()
      throws Exception {
    for (String segmentDir : _segmentDirs) {
      new UploadSegmentCommand().setControllerPort(String.valueOf(_controllerPorts.get(0)))
          .setSegmentDir(segmentDir)
          .execute();
    }
  }

  public JSONObject runQuery(String query)
      throws Exception {
    int brokerPort = _brokerPorts.get(RANDOM.nextInt(_brokerPorts.size()));
    return new JSONObject(new PostQueryCommand().setBrokerPort(String.valueOf(brokerPort)).setQuery(query).run());
  }
}
