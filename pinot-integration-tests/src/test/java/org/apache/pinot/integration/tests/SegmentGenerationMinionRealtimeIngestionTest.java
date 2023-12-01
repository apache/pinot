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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SegmentGenerationMinionRealtimeIngestionTest extends BaseClusterIntegrationTest {
  private TableConfig _realtimeTableConfig;
  private static final String REALTIME_TABLE_NAME = "mytable";
  private static final String REALTIME_SCHEMA_NAME = "mytable";

  @BeforeTest
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    startKafka();
    startMinion();

    Schema realtimeTableSchema = createSchema();
    addSchemaWithCustomSchemaName(realtimeTableSchema, REALTIME_SCHEMA_NAME);
    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create the realtime table
    _realtimeTableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(_realtimeTableConfig);
  }

  @AfterTest
  public void tearDown() {
    try {
      stopMinion();
      stopKafka();
      stopServer();
      stopBroker();
      stopController();
      stopZk();
    } finally {
      FileUtils.deleteQuietly(_tempDir);
    }
  }

  private void addSchemaWithCustomSchemaName(Schema schema, String schemaName)
      throws IOException {
    schema.setSchemaName(schemaName);
    getControllerRequestClient().addSchema(schema);
  }

  /**
   * This test validates if we are able to ingest segments into realtime table.
   * @throws Exception
   */
  @Test
  public void testIngestionIntoRealtimeTable()
      throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();

    taskConfigs.put(BatchConfigProperties.INPUT_DIR_URI, _tempDir.getAbsolutePath());
    taskConfigs.put(BatchConfigProperties.INPUT_FORMAT, "avro");

    AdhocTaskConfig adhocTaskConfig =
        new AdhocTaskConfig("SegmentGenerationAndPushTask", REALTIME_TABLE_NAME, null, taskConfigs);

    String url = getControllerBaseApiUrl() + "/tasks/execute";
    sendPostRequest(url, JsonUtils.objectToString(adhocTaskConfig),
        Collections.singletonMap("accept", "application/json"));
    TestUtils.waitForCondition(aVoid -> {
      try {
        int totalDocs = getTotalDocs(REALTIME_TABLE_NAME);
        return totalDocs == DEFAULT_COUNT_STAR_RESULT;
      } catch (Exception e) {
        return false;
      }
    }, 5000L, 600_000L, "Failed to load " + DEFAULT_COUNT_STAR_RESULT + " documents", true);
    JsonNode result = postQuery("SELECT COUNT(*) FROM " + REALTIME_TABLE_NAME);
    assertEquals(result.get("numSegmentsQueried").asInt(), 14);
  }

  private int getTotalDocs(String tableName)
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + tableName;
    JsonNode response = postQuery(query);
    JsonNode resTbl = response.get("resultTable");
    return (resTbl == null) ? 0 : resTbl.get("rows").get(0).get(0).asInt();
  }
}
