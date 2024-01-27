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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class UpsertCompactionMinionClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected PinotHelixTaskResourceManager _helixTaskResourceManager;
  protected PinotTaskManager _taskManager;
  private static final String PRIMARY_KEY_COL = "clientId";
  protected static final int SEGMENT_FLUSH_SIZE = 10;
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME);
  private static List<File> _avroFiles;
  private TableConfig _tableConfig;
  private Schema _schema;
  private String _avroTarFileName = "upsert_compaction_test0.tar.gz";

  @Override
  protected String getSchemaFileName() {
    return "upsert_upload_segment_test.schema";
  }

  @Override
  protected String getAvroTarFileName() {
    return _avroTarFileName;
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return SEGMENT_FLUSH_SIZE;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  private TableTaskConfig getCompactionTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put(MinionConstants.UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "0d");
    tableTaskConfigs.put(MinionConstants.UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT, "1");
//    tableTaskConfigs.put(MinionConstants.UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT, "1");
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.UpsertCompactionTask.TASK_TYPE, tableTaskConfigs));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(1);

    // Start Kafka and push data into Kafka
    startKafka();

    // Unpack the Avro files
    _avroFiles = unpackAvroData(_tempDir);
    pushAvroIntoKafka(_avroFiles);

    // Create and upload the schema and table config
    _schema = createSchema();
    addSchema(_schema);
    _tableConfig = createUpsertTableConfig(_avroFiles.get(0), PRIMARY_KEY_COL, null, getNumKafkaPartitions());
    _tableConfig.setTaskConfig(getCompactionTaskConfig());
    addTableConfig(_tableConfig);

//    ClusterIntegrationTestUtils.buildSegmentsFromAvro(_avroFiles, _tableConfig, _schema, 0, _segmentDir, _tarDir);

    startMinion();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
  }

  @BeforeMethod
  public void beforeMethod()
      throws Exception {
    // Create and upload segments
//    uploadSegments(getTableName(), TableType.REALTIME, _tarDir);
  }

  protected void waitForAllDocsLoaded(long timeoutMs, long expectedCount)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResultWithoutUpsert() == expectedCount;
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
  }

  private long getCurrentCountStarResultWithoutUpsert() {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + getTableName() + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0);
  }

  private long getSalary() {
    return getPinotConnection().execute("SELECT salary FROM " + getTableName() + " WHERE clientId=100001")
        .getResultSet(0).getLong(0);
  }

  @Override
  protected long getCountStarResult() {
    return 2;
  }

  @AfterMethod
  public void afterMethod()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // Test dropping all segments one by one
    List<String> segments = listSegments(realtimeTableName);
    assertFalse(segments.isEmpty());
    for (String segment : segments) {
      dropSegment(realtimeTableName, segment);
    }
    // NOTE: There is a delay to remove the segment from property store
    TestUtils.waitForCondition((aVoid) -> {
      try {
        return listSegments(realtimeTableName).isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to drop the segments");
    restartServers();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    dropRealtimeTable(realtimeTableName);
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testCompaction()
      throws Exception {
    waitForAllDocsLoaded(600_000L, 283);
    assertEquals(getSalary(), 9747108);

    assertNotNull(_taskManager.scheduleTasks(REALTIME_TABLE_NAME).get(MinionConstants.UpsertCompactionTask.TASK_TYPE));
    waitForTaskToComplete();
    waitForAllDocsLoaded(600_000L, 3);
    assertEquals(getSalary(), 9747108);
  }

  @Test
  public void testCompactionDeletesSegments()
      throws Exception {
    pushAvroIntoKafka(_avroFiles);
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L, 566);
    assertEquals(getSalary(), 9747108);

    assertNull(_taskManager.scheduleTasks(REALTIME_TABLE_NAME).get(MinionConstants.UpsertCompactionTask.TASK_TYPE));
    waitForTaskToComplete();
    waitForAllDocsLoaded(600_000L, 283);
    assertEquals(getSalary(), 9747108);
  }

  @Test
  public void testRecompaction()
    throws Exception {

    waitForAllDocsLoaded(10_000L, 10);

    Map<String, String> shceduledTasks = _taskManager.scheduleTasks(REALTIME_TABLE_NAME);
    assertNotNull(shceduledTasks.get(MinionConstants.UpsertCompactionTask.TASK_TYPE));
    waitForTaskToComplete();
    waitForAllDocsLoaded(30_000L, 2);

//    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    _avroTarFileName = "upsert_compaction_test1.tar.gz";
    _avroFiles = unpackAvroData(_tempDir);
    pushAvroIntoKafka(_avroFiles);
    waitForAllDocsLoaded(10_000L, 13);

    shceduledTasks = _taskManager.scheduleTasks(REALTIME_TABLE_NAME);
    assertNotNull(shceduledTasks.get(MinionConstants.UpsertCompactionTask.TASK_TYPE));
    waitForTaskToComplete();
    waitForAllDocsLoaded(10_000L, 2);

  }

  protected void waitForTaskToComplete() {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(MinionConstants.UpsertCompactionTask.TASK_TYPE)
          .values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");
  }
}
