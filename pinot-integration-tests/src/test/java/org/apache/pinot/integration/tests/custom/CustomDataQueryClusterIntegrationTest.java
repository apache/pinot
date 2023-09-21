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
package org.apache.pinot.integration.tests.custom;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;


public abstract class CustomDataQueryClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CustomDataQueryClusterIntegrationTest.class);

  @BeforeSuite
  public void setUpSuite()
      throws Exception {
    LOGGER.warn("Setting up integration test suite");
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    LOGGER.warn("Finished setting up integration test suite");
  }

  @AfterSuite
  public void tearDownSuite()
      throws Exception {
    LOGGER.warn("Tearing down integration test suite");
    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test suite");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl("http://localhost:" + DEFAULT_CONTROLLER_PORT);
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // create & upload segments
    File avroFile = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    waitForAllDocsLoaded(60_000);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
    dropOfflineTable(getTableName());
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  @Override
  public String getZkUrl() {
    return ZkStarter.getDefaultZkStr();
  }

  @Override
  public String getHelixClusterName() {
    return "CustomDataQueryClusterIntegrationTest";
  }

  @Override
  protected String getBrokerBaseApiUrl() {
    return "http://localhost:" + DEFAULT_BROKER_PORT;
  }

  @Override
  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  @Override
  protected abstract String getTableName();

  @Override
  protected abstract Schema createSchema();

  protected abstract File createAvroFile()
      throws Exception;
}
