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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


/**
 * Base class for integration tests that can share the same rich Pinot cluster within a TestNG suite.
 *
 * <p>The sharing behavior is disabled by default so the existing per-class lifecycle is preserved for direct
 * {@code -Dtest=...} runs. Suite XMLs can opt in by setting {@value #SHARED_RICH_CLUSTER_ENABLED_PROPERTY} to
 * {@code true}.
 */
public abstract class SharedRichClusterIntegrationTest extends BaseClusterIntegrationTest {
  public static final String SHARED_RICH_CLUSTER_ENABLED_PROPERTY = "pinot.integration.sharedRichCluster.enabled";

  private static final Logger LOGGER = LoggerFactory.getLogger(SharedRichClusterIntegrationTest.class);
  private static final String SHARED_CLUSTER_NAME = "SharedRichClusterIntegrationTestSuite";

  protected static SharedRichClusterIntegrationTest _sharedRichClusterTestSuite;

  @BeforeSuite(alwaysRun = true)
  public void setUpSharedRichClusterSuite()
      throws Exception {
    if (!isSharedRichClusterEnabled()) {
      return;
    }
    synchronized (SharedRichClusterIntegrationTest.class) {
      if (_sharedRichClusterTestSuite != null) {
        return;
      }
      _sharedRichClusterTestSuite = this;
    }

    LOGGER.warn("Setting up shared rich integration test suite");
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    super.startZk();
    super.startKafkaWithoutTopic();
    Map<String, Object> controllerConfig = super.getDefaultControllerConfiguration();
    controllerConfig.put(ControllerConf.CONSOLE_SWAGGER_ENABLE, true);
    super.startController(controllerConfig);
    super.startBrokers(1);
    super.startServers(2);
    super.startMinion();
    attachSharedRichCluster();
    LOGGER.warn("Finished setting up shared rich integration test suite");
  }

  @AfterSuite(alwaysRun = true)
  public void tearDownSharedRichClusterSuite()
      throws Exception {
    if (!isSharedRichClusterOwner()) {
      return;
    }

    LOGGER.warn("Tearing down shared rich integration test suite");
    try {
      if (_minionStarter != null) {
        super.stopMinion();
      }
      if (!_serverStarters.isEmpty()) {
        super.stopServer();
      }
      if (!_brokerStarters.isEmpty()) {
        super.stopBroker();
      }
      if (_controllerStarter != null) {
        super.stopController();
      }
      super.stopKafka();
      super.stopZk();
      FileUtils.deleteDirectory(_tempDir);
      LOGGER.warn("Finished tearing down shared rich integration test suite");
    } finally {
      synchronized (SharedRichClusterIntegrationTest.class) {
        _sharedRichClusterTestSuite = null;
      }
    }
  }

  protected boolean isSharedRichClusterEnabled() {
    return Boolean.getBoolean(SHARED_RICH_CLUSTER_ENABLED_PROPERTY);
  }

  protected boolean isSharedRichClusterOwner() {
    return isSharedRichClusterEnabled() && _sharedRichClusterTestSuite == this;
  }

  protected void attachSharedRichCluster() {
    if (!isSharedRichClusterEnabled()) {
      return;
    }
    SharedRichClusterIntegrationTest sharedSuite = _sharedRichClusterTestSuite;
    if (sharedSuite == null) {
      throw new IllegalStateException("Shared rich cluster has not been initialized");
    }
    if (sharedSuite == this) {
      return;
    }

    _controllerStarter = sharedSuite._controllerStarter;
    _controllerPort = sharedSuite._controllerPort;
    _controllerConfig = sharedSuite._controllerConfig;
    _controllerBaseApiUrl = sharedSuite._controllerBaseApiUrl;
    _controllerRequestURLBuilder = sharedSuite._controllerRequestURLBuilder;
    _controllerDataDir = sharedSuite._controllerDataDir;
    _helixResourceManager = sharedSuite._helixResourceManager;
    _helixManager = sharedSuite._helixManager;
    _helixDataAccessor = sharedSuite._helixDataAccessor;
    _helixAdmin = sharedSuite._helixAdmin;
    _propertyStore = sharedSuite._propertyStore;
    _tableRebalanceManager = sharedSuite._tableRebalanceManager;
    _tableSizeReader = sharedSuite._tableSizeReader;

    _brokerStarters.clear();
    _brokerStarters.addAll(sharedSuite._brokerStarters);
    _brokerPorts.clear();
    _brokerPorts.addAll(sharedSuite._brokerPorts);
    _brokerBaseApiUrl = sharedSuite._brokerBaseApiUrl;
    _brokerGrpcEndpoint = sharedSuite._brokerGrpcEndpoint;

    _serverStarters.clear();
    _serverStarters.addAll(sharedSuite._serverStarters);
    _serverGrpcPort = sharedSuite._serverGrpcPort;
    _serverAdminApiPort = sharedSuite._serverAdminApiPort;
    _serverNettyPort = sharedSuite._serverNettyPort;

    _minionStarter = sharedSuite._minionStarter;
    _minionBaseApiUrl = sharedSuite._minionBaseApiUrl;
    _kafkaStarters = sharedSuite._kafkaStarters;
  }

  @Override
  public String getHelixClusterName() {
    if (isSharedRichClusterEnabled()) {
      return SHARED_CLUSTER_NAME;
    }
    return super.getHelixClusterName();
  }

  @Override
  public String getZkUrl() {
    if (isSharedRichClusterEnabled()) {
      return _sharedRichClusterTestSuite == this ? super.getZkUrl() : _sharedRichClusterTestSuite.getZkUrl();
    }
    return super.getZkUrl();
  }

  @Override
  protected String getBrokerBaseApiUrl() {
    if (isSharedRichClusterEnabled()) {
      return _sharedRichClusterTestSuite == this
          ? super.getBrokerBaseApiUrl()
          : _sharedRichClusterTestSuite.getBrokerBaseApiUrl();
    }
    return super.getBrokerBaseApiUrl();
  }

  @Override
  protected String getBrokerGrpcEndpoint() {
    if (isSharedRichClusterEnabled()) {
      return _sharedRichClusterTestSuite == this
          ? super.getBrokerGrpcEndpoint()
          : _sharedRichClusterTestSuite.getBrokerGrpcEndpoint();
    }
    return super.getBrokerGrpcEndpoint();
  }

  @Override
  public int getControllerPort() {
    if (isSharedRichClusterEnabled()) {
      return _sharedRichClusterTestSuite == this
          ? super.getControllerPort()
          : _sharedRichClusterTestSuite.getControllerPort();
    }
    return super.getControllerPort();
  }

  @Override
  protected int getRandomBrokerPort() {
    if (isSharedRichClusterEnabled()) {
      return _sharedRichClusterTestSuite == this
          ? super.getRandomBrokerPort()
          : _sharedRichClusterTestSuite.getRandomBrokerPort();
    }
    return super.getRandomBrokerPort();
  }

  @Override
  public String getMinionBaseApiUrl() {
    if (isSharedRichClusterEnabled()) {
      return _sharedRichClusterTestSuite == this
          ? super.getMinionBaseApiUrl()
          : _sharedRichClusterTestSuite.getMinionBaseApiUrl();
    }
    return super.getMinionBaseApiUrl();
  }

  @Override
  public void startZk() {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startZk();
  }

  @Override
  public void startZk(int port) {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startZk(port);
  }

  @Override
  public void startController()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startController();
  }

  @Override
  public void startController(Map<String, Object> properties)
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startController(properties);
  }

  @Override
  public void startControllerWithSwagger()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startControllerWithSwagger();
  }

  @Override
  protected void startBroker()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startBroker();
  }

  @Override
  protected void startBrokers(int numBrokers)
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startBrokers(numBrokers);
  }

  @Override
  protected void startServer()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startServer();
  }

  @Override
  protected void startServers(int numServers)
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startServers(numServers);
  }

  @Override
  protected void startMinion()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startMinion();
  }

  @Override
  protected void startKafka() {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      createKafkaTopic(getKafkaTopic());
      return;
    }
    super.startKafka();
  }

  @Override
  protected void startKafkaWithoutTopic() {
    if (isSharedRichClusterEnabled()) {
      attachSharedRichCluster();
      return;
    }
    super.startKafkaWithoutTopic();
  }

  @Override
  public void stopZk() {
    if (!isSharedRichClusterEnabled()) {
      super.stopZk();
    }
  }

  @Override
  public void stopController() {
    if (!isSharedRichClusterEnabled()) {
      super.stopController();
    }
  }

  @Override
  protected void stopBroker() {
    if (!isSharedRichClusterEnabled()) {
      super.stopBroker();
    }
  }

  @Override
  protected void stopServer() {
    if (!isSharedRichClusterEnabled()) {
      super.stopServer();
    }
  }

  @Override
  protected void stopMinion() {
    if (!isSharedRichClusterEnabled()) {
      super.stopMinion();
    }
  }

  @Override
  protected void stopKafka() {
    if (!isSharedRichClusterEnabled()) {
      super.stopKafka();
    }
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    attachSharedRichCluster();
    super.pushAvroIntoKafka(avroFiles);
  }

  @Override
  public PinotAdminClient getOrCreateAdminClient()
      throws IOException {
    attachSharedRichCluster();
    return super.getOrCreateAdminClient();
  }
}
