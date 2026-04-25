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
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SegmentGenerationMinionRealtimeIngestionTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_REALTIME_TABLE_NAME = "segment_generation_minion_realtime_ingestion";
  private static final String SEGMENT_GENERATION_AND_PUSH_TASK = "SegmentGenerationAndPushTask";

  private TableConfig _realtimeTableConfig;

  @BeforeClass
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

    cleanupRealtimeTableAndSchema();
    Schema realtimeTableSchema = createSchema();
    addSchemaWithCustomSchemaName(realtimeTableSchema, getTableName());
    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create the realtime table
    _realtimeTableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(_realtimeTableConfig);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    try {
      cleanupTaskQueue();
      cleanupRealtimeTableAndSchema();
    } finally {
      try {
        if (_minionStarter != null) {
          stopMinion();
        }
        stopKafka();
        if (!_serverStarters.isEmpty()) {
          stopServer();
        }
        if (!_brokerStarters.isEmpty()) {
          stopBroker();
        }
        if (_controllerStarter != null) {
          stopController();
        }
        stopZk();
      } finally {
        FileUtils.deleteQuietly(_tempDir);
      }
    }
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_REALTIME_TABLE_NAME : super.getTableName();
  }

  private void addSchemaWithCustomSchemaName(Schema schema, String schemaName)
      throws IOException {
    schema.setSchemaName(schemaName);
    addSchema(schema);
  }

  /**
   * Validates if we are able to ingest segments into realtime table, via adhoc mode.
   * @throws Exception
   */
  @Test
  public void testAdhocIngestionIntoRealtimeTable()
      throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();

    taskConfigs.put(BatchConfigProperties.INPUT_DIR_URI, _tempDir.getAbsolutePath());
    taskConfigs.put(BatchConfigProperties.INPUT_FORMAT, "avro");

    AdhocTaskConfig adhocTaskConfig =
        new AdhocTaskConfig(SEGMENT_GENERATION_AND_PUSH_TASK, getTableName(), null, taskConfigs);

    String url = getControllerBaseApiUrl() + "/tasks/execute";
    sendPostRequest(url, JsonUtils.objectToString(adhocTaskConfig),
        Collections.singletonMap("accept", "application/json"));
    TestUtils.waitForCondition(aVoid -> {
      try {
        int totalDocs = getTotalDocs(getTableName());
        return totalDocs == DEFAULT_COUNT_STAR_RESULT;
      } catch (Exception e) {
        return false;
      }
    }, 5000L, 600_000L, "Failed to load " + DEFAULT_COUNT_STAR_RESULT + " documents");
    JsonNode result = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertEquals(result.get("numSegmentsQueried").asInt(), 14);
  }

  /**
   * Validates ingestion to realtime table via scheduled mode
   * @throws Exception
   */
  @Test
  public void testScheduledIngestionIntoRealtimeTable()
      throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(BatchConfigProperties.INPUT_DIR_URI, _tempDir.getAbsolutePath());
    taskConfigs.put(BatchConfigProperties.INPUT_FORMAT, "avro");

    TableTaskConfig tableTaskConfig =
        new TableTaskConfig(Collections.singletonMap(SEGMENT_GENERATION_AND_PUSH_TASK, taskConfigs));
    BatchIngestionConfig batchIngestionConfig = new BatchIngestionConfig(List.of(taskConfigs), "APPEND", "DAILY");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(batchIngestionConfig);
    _realtimeTableConfig.setIngestionConfig(ingestionConfig);
    _realtimeTableConfig.setTaskConfig(tableTaskConfig);
    updateTableConfig(_realtimeTableConfig);
    String url = getControllerBaseApiUrl() + "/tasks/schedule?taskType=" + SEGMENT_GENERATION_AND_PUSH_TASK
        + "&tableName=" + TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    sendPostRequest(url, null, Collections.singletonMap("accept", "application/json"));
    TestUtils.waitForCondition(aVoid -> {
      try {
        int totalDocs = getTotalDocs(getTableName());
        return totalDocs == DEFAULT_COUNT_STAR_RESULT;
      } catch (Exception e) {
        return false;
      }
    }, 5000L, 600_000L, "Failed to load " + DEFAULT_COUNT_STAR_RESULT + " documents");
    JsonNode result = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertEquals(result.get("numSegmentsQueried").asInt(), 14);
  }

  private void cleanupTaskQueue() {
    if (_controllerStarter == null) {
      return;
    }
    if (_controllerStarter.getHelixTaskResourceManager().getTaskTypes().contains(SEGMENT_GENERATION_AND_PUSH_TASK)) {
      _controllerStarter.getHelixTaskResourceManager().deleteTaskQueue(SEGMENT_GENERATION_AND_PUSH_TASK, false);
    }
  }

  private void cleanupRealtimeTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }
    if (_helixResourceManager.hasRealtimeTable(getTableName())) {
      dropRealtimeTable(getTableName());
    }
    if (_helixResourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
    }
  }

  private int getTotalDocs(String tableName)
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + tableName;
    JsonNode response = postQuery(query);
    JsonNode resTbl = response.get("resultTable");
    return (resTbl == null) ? 0 : resTbl.get("rows").get(0).get(0).asInt();
  }
}
