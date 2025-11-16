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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for Page Cache Warmup functionality.
 * Tests the complete flow of:
 * 1. Enabling page cache warmup in table config
 * 2. Storing and retrieving warmup queries
 * 3. Starting servers with warmup enabled
 * 4. Cleanup and deletion of warmup queries
 */
public class PageCacheWarmupIntegrationTest extends BaseClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "myTable";
  private static final int NUM_SERVERS = 1;

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zookeeper
    startZk();

    // Start Pinot cluster
    startController();
    startBroker();
    startServer();

    // Create and upload schema
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(DEFAULT_TABLE_NAME)
        .addSingleValueDimension("Origin", org.apache.pinot.spi.data.FieldSpec.DataType.STRING)
        .addMetric("Count", org.apache.pinot.spi.data.FieldSpec.DataType.LONG)
        .build();
    addSchema(schema);

    // Create and upload table config
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(DEFAULT_TABLE_NAME)
        .build();
    addTableConfig(tableConfig);

    // Create and upload test segments
    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Wait for all segments to be online
    waitForAllDocsLoaded(60_000L);
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Test
  public void testPageCacheWarmupFlow() throws IOException {
    BaseServerStarter additionalServer = null;
    try {
      // Step 1: Enable page cache warmup in table config
      PageCacheWarmupConfig pageCacheWarmupConfig = new PageCacheWarmupConfig(true, true, 0, null, null, null);
      TableConfig tableConfig = getOfflineTableConfig();
      tableConfig.setPageCacheWarmupConfig(pageCacheWarmupConfig);
      updateTableConfig(tableConfig);

      // Step 2: Store warmup queries
      List<String> queries = List.of(
          "SELECT COUNT(*) FROM " + getTableName(),
          "SELECT DISTINCT Origin FROM " + getTableName() + " LIMIT 10"
      );
      Assert.assertTrue(_controllerRequestClient.storePageCacheWarmupQueries(DEFAULT_TABLE_NAME, "OFFLINE", queries),
          "Failed to store page cache warmup queries");

      // Step 3: Retrieve and verify stored queries
      List<String> storedQueries = _controllerRequestClient.getPageCacheWarmupQueries(DEFAULT_TABLE_NAME, "OFFLINE");
      Assert.assertEquals(storedQueries.size(), queries.size(), "Number of stored queries does not match");
      for (int i = 0; i < queries.size(); i++) {
        Assert.assertEquals(storedQueries.get(i), queries.get(i), "Query at index " + i + " does not match");
      }

      // Step 4: Start an additional server with page cache warmup enabled
      additionalServer = startOneServer(1);

      // Verify the server was added to the table
      PinotHelixResourceManager pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
      int finalServers = pinotHelixResourceManager.getServerInstancesForTable(getTableName(), TableType.OFFLINE).size();
      Assert.assertEquals(finalServers, getNumServers() + 1, "Server was not added to the table");

      // Step 5: Verify queries still work after warmup
      String testQuery = "SELECT COUNT(*) FROM " + getTableName();
      JsonNode queryResponse = postQuery(testQuery);
      Assert.assertTrue(queryResponse.get("exceptions").isEmpty());
    } catch (Exception e) {
      Assert.fail("Failed during page cache warmup flow: " + e.getMessage(), e);
    } finally {
      // Cleanup: Reset the page cache warmup config
      TableConfig tableConfig = getOfflineTableConfig();
      tableConfig.setPageCacheWarmupConfig(null);
      updateTableConfig(tableConfig);
      // Cleanup: Stop the additional server
      if (additionalServer != null) {
        additionalServer.stop();
        BaseServerStarter finalServerStarter = additionalServer;
        TestUtils.waitForCondition(
            aVoid -> getHelixResourceManager().dropInstance(finalServerStarter.getInstanceId()).isSuccessful(),
            60_000L, "Failed to drop added server");
      }
      // Cleanup: Delete the warmup queries
      _controllerRequestClient.deletePageCacheWarmupQueries(DEFAULT_TABLE_NAME, "OFFLINE");
    }
  }

  @AfterClass
  public void tearDown() throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
