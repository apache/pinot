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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MultiStageEngineCustomTenantIntegrationTest extends MultiStageEngineIntegrationTest {
  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  private static final String TEST_TENANT = "TestTenant";

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @Override
  protected String getBrokerTenant() {
    return TEST_TENANT;
  }

  @Override
  protected String getServerTenant() {
    return TEST_TENANT;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    startController(properties);

    startBroker();
    startServer();
    createBrokerTenant(TEST_TENANT, 1);
    createServerTenant(TEST_TENANT, 1, 0);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  @Override
  public void testHardcodedQueriesMultiStage()
      throws Exception {
    super.testHardcodedQueriesMultiStage();
  }

  @Test
  @Override
  public void testGeneratedQueries()
      throws Exception {
    // test multistage engine, currently we don't support MV columns.
    super.testGeneratedQueries(false, true);
  }

  @Test
  public void testQueryOptions()
      throws Exception {
    String pinotQuery = "SET multistageLeafLimit = 1; SELECT * FROM mytable;";
    String h2Query = "SELECT * FROM mytable limit 1";
    ClusterIntegrationTestUtils.testQueryWithMatchingRowCount(pinotQuery, getBrokerBaseApiUrl(), getPinotConnection(),
        h2Query, getH2Connection(), null, ImmutableMap.of("queryOptions", "useMultistageEngine=true"));
  }

  @Override
  protected Connection getPinotConnection() {
    Properties properties = new Properties();
    properties.put("queryOptions", "useMultistageEngine=true");
    if (_pinotConnection == null) {
      _pinotConnection = ConnectionFactory.fromZookeeper(properties, getZkUrl() + "/" + getHelixClusterName());
    }
    return _pinotConnection;
  }

  @Override
  protected void testQuery(String pinotQuery, String h2Query) {
    ClusterIntegrationTestUtils.testQueryViaController(pinotQuery, getControllerBaseApiUrl(), getPinotConnection(),
        h2Query, getH2Connection(), null, ImmutableMap.of("queryOptions", "useMultistageEngine=true"));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
