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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.api.resources.ServerReloadTableControllerJobStatusResponse;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ReloadTableIntegrationTest extends BaseClusterIntegrationTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testTableReload()
      throws Exception {
    String responseStr = ControllerTest.sendPostRequest(
        StringUtil.join("/", getControllerBaseApiUrl(), "tables", getTableName(), "reload?force=true"), null);
    assertTrue(responseStr.contains("Submitted reload table job"));

    // TODO fix this once SuccessResponse has a top-level field for returning job IDs
    final String jobId = responseStr.split("with id: ")[1].split(",")[0];
    final int maxDelay = 10_000;
    int delay = 1_000;

    // wait for reload to finish
    ServerReloadTableControllerJobStatusResponse response;
    do {
      Thread.sleep(delay);
      responseStr = ControllerTest.sendGetRequest(
          StringUtil.join("/", getControllerBaseApiUrl(), "tables", "tableReloadStatus", jobId), null);

      response = JsonUtils.stringToObject(responseStr, ServerReloadTableControllerJobStatusResponse.class);
      delay = Math.min(delay * 2, maxDelay);
    } while (!response.isCompleted());

    // Validate no change
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
  }
}
