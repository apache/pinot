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

import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.integration.tests.models.DummyTableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Input data - Scores of players
 * Schema
 *  - Dimension fields: playerId:int (primary key), name:string, game:string, deleted:boolean
 *  - Metric fields: score:float
 *  - DataTime fields: timestampInEpoch:long
 */
public class UpsertTableIntegrationTest extends BaseClusterIntegrationTest {
  private static final String INPUT_DATA_SMALL_TAR_FILE = "gameScores_csv.tar.gz";
  private static final String INPUT_DATA_LARGE_TAR_FILE = "gameScores_large_csv.tar.gz";
  private static final String INPUT_DATA_PARTIAL_UPSERT_TAR_FILE = "gameScores_partial_upsert_csv.tar.gz";

  private static final String CSV_SCHEMA_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final String PARTIAL_UPSERT_TABLE_SCHEMA = "partial_upsert_table_test.schema";
  private static final String CSV_DELIMITER = ",";
  private static final String TABLE_NAME = "gameScores";
  private static final int NUM_SERVERS = 2;
  private static final String DELETE_COL = "deleted";
  public static final String PRIMARY_KEY_COL = "playerId";
  public static final String TIME_COL_NAME = "timestampInEpoch";
  public static final String UPSERT_SCHEMA_FILE_NAME = "upsert_table_test.schema";

  protected PinotTaskManager _taskManager;
  protected PinotHelixTaskResourceManager _helixTaskResourceManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(NUM_SERVERS);
    startMinion();

    // Start Kafka
    startKafkaWithoutTopic();

    // Push data to Kafka and set up table
    String kafkaTopicName = getKafkaTopic();
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);
    setUpTable(getTableName(), kafkaTopicName, null);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
    assertEquals(getCurrentCountStarResult(), getCountStarResult());

    // Create partial upsert table schema
    Schema partialUpsertSchema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    addSchema(partialUpsertSchema);
    _taskManager = _controllerStarter.getTaskManager();
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    dropRealtimeTable(realtimeTableName);
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected String getSchemaFileName() {
    return UPSERT_SCHEMA_FILE_NAME;
  }

  @Nullable
  @Override
  protected String getTimeColumnName() {
    return TIME_COL_NAME;
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100, 101, 102
    return 3;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 500;
  }

  private long getCountStarResultWithoutUpsert() {
    return 10;
  }

  private long queryCountStarWithoutUpsert(String tableName) {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0);
  }

  private long queryCountStar(String tableName) {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getResultSet(0).getLong(0);
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    waitForAllDocsLoaded(getTableName(), timeoutMs, getCountStarResultWithoutUpsert());
  }

  private void waitForAllDocsLoaded(String tableName, long timeoutMs, long expectedCountStarWithoutUpsertResult) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert(tableName) == expectedCountStarWithoutUpsertResult;
      } catch (Exception e) {
        return null;
      }
    }, timeoutMs, "Failed to load all documents");
  }

  private void waitForNumQueriedSegmentsToConverge(String tableName, long timeoutMs, int expectedNumSegmentsQueried) {
    waitForNumQueriedSegmentsToConverge(tableName, timeoutMs, expectedNumSegmentsQueried, -1);
  }

  private void waitForNumQueriedSegmentsToConverge(String tableName, long timeoutMs, int expectedNumSegmentsQueried,
      int expectedNumConsumingSegmentsQueried) {
    // Do not tolerate exception here because it is always followed by the docs check
    TestUtils.waitForCondition(aVoid -> {
      ExecutionStats executionStats =
          getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getExecutionStats();
      return executionStats.getNumSegmentsQueried() == expectedNumSegmentsQueried && (
          expectedNumConsumingSegmentsQueried < 0
              || executionStats.getNumConsumingSegmentsQueried() == expectedNumConsumingSegmentsQueried);
    }, timeoutMs, "Failed to load all segments");
  }

  @Test
  protected void testDeleteWithFullUpsert()
      throws Exception {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);
    testDeleteWithFullUpsert(getKafkaTopic() + "-with-deletes", "gameScoresWithDelete", upsertConfig);
  }

  protected void testDeleteWithFullUpsert(String kafkaTopicName, String tableName, UpsertConfig upsertConfig)
      throws Exception {
    // SETUP
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);
    setUpTable(tableName, kafkaTopicName, upsertConfig);

    // TEST 1: Delete existing primary key
    // Push 2 records with deleted = true - deletes pks 100 and 102
    List<String> deleteRecords = List.of("102,Clifford,counter-strike,102,1681254200000,true",
        "100,Zook,counter-strike,2050,1681377200000,true");
    pushCsvIntoKafka(deleteRecords, kafkaTopicName, 0);

    // Wait for all docs (12 with skipUpsert=true) to be loaded
    waitForAllDocsLoaded(tableName, 600_000L, 12);

    // Query for number of records in the table - should only return 1
    ResultSet rs = getPinotConnection().execute("SELECT * FROM " + tableName).getResultSet(0);
    assertEquals(rs.getRowCount(), 1);

    // pk 101 - not deleted - only record available
    int columnCount = rs.getColumnCount();
    int playerIdColumnIndex = -1;
    for (int i = 0; i < columnCount; i++) {
      String columnName = rs.getColumnName(i);
      if ("playerId".equalsIgnoreCase(columnName)) {
        playerIdColumnIndex = i;
        break;
      }
    }
    assertNotEquals(playerIdColumnIndex, -1);
    assertEquals(rs.getString(0, playerIdColumnIndex), "101");

    // Validate deleted records
    rs = getPinotConnection().execute(
        "SELECT playerId FROM " + tableName + " WHERE deleted = true OPTION(skipUpsert=true)").getResultSet(0);
    assertEquals(rs.getRowCount(), 2);
    for (int i = 0; i < rs.getRowCount(); i++) {
      String playerId = rs.getString(i, 0);
      assertTrue("100".equalsIgnoreCase(playerId) || "102".equalsIgnoreCase(playerId));
    }

    // TEST 2: Revive a previously deleted primary key
    // Revive pk - 100 by adding a record with a newer timestamp
    List<String> revivedRecord = Collections.singletonList("100,Zook-New,,0.0,1684707335000,false");
    pushCsvIntoKafka(revivedRecord, kafkaTopicName, 0);
    // Wait for the new record (13 with skipUpsert=true) to be indexed
    waitForAllDocsLoaded(tableName, 600_000L, 13);

    // Validate: pk is queryable and all columns are overwritten with new value
    rs = getPinotConnection().execute("SELECT playerId, name, game FROM " + tableName + " WHERE playerId = 100")
        .getResultSet(0);
    assertEquals(rs.getRowCount(), 1);
    assertEquals(rs.getInt(0, 0), 100);
    assertEquals(rs.getString(0, 1), "Zook-New");
    assertEquals(rs.getString(0, 2), "null");

    // Validate: pk lineage still exists
    rs = getPinotConnection().execute(
        "SELECT playerId, name FROM " + tableName + " WHERE playerId = 100 OPTION(skipUpsert=true)").getResultSet(0);

    assertTrue(rs.getRowCount() > 1);

    // TEARDOWN
    dropRealtimeTable(tableName);
  }

  private void setUpKafka(String kafkaTopicName, String inputDataFile)
      throws Exception {
    createKafkaTopic(kafkaTopicName);
    List<File> dataFiles = unpackTarData(inputDataFile, _tempDir);
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);
  }

  private TableConfig setUpTable(String tableName, String kafkaTopicName, UpsertConfig upsertConfig)
      throws Exception {
    Schema schema = createSchema();
    schema.setSchemaName(tableName);
    addSchema(schema);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, PRIMARY_KEY_COL);
    addTableConfig(tableConfig);

    return tableConfig;
  }

  @Test
  public void testDeleteWithPartialUpsert()
      throws Exception {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);
    testDeleteWithPartialUpsert(getKafkaTopic() + "-partial-upsert-with-deletes", "gameScoresPartialUpsertWithDelete",
        upsertConfig);
  }

  protected void testDeleteWithPartialUpsert(String kafkaTopicName, String tableName, UpsertConfig upsertConfig)
      throws Exception {
    // Create kafka topic and push records
    setUpKafka(kafkaTopicName, INPUT_DATA_PARTIAL_UPSERT_TAR_FILE);

    // Create table with delete Record column
    Schema partialUpsertSchema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    partialUpsertSchema.setSchemaName(tableName);
    addSchema(partialUpsertSchema);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    upsertConfig.setPartialUpsertStrategies(
        Map.of("game", UpsertConfig.Strategy.UNION, "score", UpsertConfig.Strategy.INCREMENT));
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, PRIMARY_KEY_COL);
    addTableConfig(tableConfig);

    // TEST 1: Delete existing primary key
    // Push 2 records with deleted = true - deletes pks 100 and 102
    List<String> deleteRecords = List.of("102,Clifford,counter-strike,102,1681054200000,true",
        "100,Zook,counter-strike,2050,1681377200000,true");
    pushCsvIntoKafka(deleteRecords, kafkaTopicName, 0);

    // Wait for all docs (12 with skipUpsert=true) to be loaded
    waitForAllDocsLoaded(tableName, 600_000L, 12);

    // Query for number of records in the table - should only return 1
    ResultSet rs = getPinotConnection().execute("SELECT * FROM " + tableName).getResultSet(0);
    assertEquals(rs.getRowCount(), 1);

    // pk 101 - not deleted - only record available
    int columnCount = rs.getColumnCount();
    int playerIdColumnIndex = -1;
    for (int i = 0; i < columnCount; i++) {
      String columnName = rs.getColumnName(i);
      if ("playerId".equalsIgnoreCase(columnName)) {
        playerIdColumnIndex = i;
        break;
      }
    }
    assertNotEquals(playerIdColumnIndex, -1);
    assertEquals(rs.getString(0, playerIdColumnIndex), "101");

    // Validate deleted records
    rs = getPinotConnection().execute(
        "SELECT playerId FROM " + tableName + " WHERE deleted = true OPTION(skipUpsert=true)").getResultSet(0);
    assertEquals(rs.getRowCount(), 2);
    for (int i = 0; i < rs.getRowCount(); i++) {
      String playerId = rs.getString(i, 0);
      assertTrue("100".equalsIgnoreCase(playerId) || "102".equalsIgnoreCase(playerId));
    }

    // TEST 2: Revive a previously deleted primary key
    // Revive pk - 100 by adding a record with a newer timestamp
    List<String> revivedRecord = Collections.singletonList("100,Zook,,0.0,1684707335000,false");
    pushCsvIntoKafka(revivedRecord, kafkaTopicName, 0);
    // Wait for the new record (13 with skipUpsert=true) to be indexed
    waitForAllDocsLoaded(tableName, 600_000L, 13);

    // Validate: pk is queryable and all columns are overwritten with new value
    rs = getPinotConnection().execute("SELECT playerId, name, game FROM " + tableName + " WHERE playerId = 100")
        .getResultSet(0);
    assertEquals(rs.getRowCount(), 1);
    assertEquals(rs.getInt(0, 0), 100);
    assertEquals(rs.getString(0, 1), "Zook");
    assertEquals(rs.getString(0, 2), "[\"null\"]");

    // Validate: pk lineage still exists
    rs = getPinotConnection().execute(
        "SELECT playerId, name FROM " + tableName + " WHERE playerId = 100 OPTION(skipUpsert=true)").getResultSet(0);

    assertTrue(rs.getRowCount() > 1);
  }

  @Test
  public void testDefaultMetadataManagerClass()
      throws Exception {
    PinotConfiguration serverConf = getServerConf(NUM_SERVERS);
    serverConf.setProperty(Joiner.on(".").join(CommonConstants.Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX,
            HelixInstanceDataManagerConfig.UPSERT_CONFIG_PREFIX,
            TableUpsertMetadataManagerFactory.UPSERT_DEFAULT_METADATA_MANAGER_CLASS),
        DummyTableUpsertMetadataManager.class.getName());
    BaseServerStarter serverStarter = startOneServer(serverConf);

    // Tag the new server
    String instanceId = serverStarter.getInstanceId();
    _helixResourceManager.updateInstanceTags(instanceId, "DummyTag_REALTIME", false);

    // Create a dummy table on the new server
    String dummyTableName = "dummyTable123";
    Schema schema = createSchema();
    schema.setSchemaName(dummyTableName);
    addSchema(schema);
    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(dummyTableName, getKafkaTopic(), getNumKafkaPartitions(), csvDecoderProperties, null,
            PRIMARY_KEY_COL);
    TenantConfig tenantConfig = new TenantConfig(TagNameUtils.DEFAULT_TENANT_NAME, "DummyTag", null);
    tableConfig.setTenantConfig(tenantConfig);
    addTableConfig(tableConfig);

    // Verify DummyTableUpsertMetadataManager is used
    String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(dummyTableName);
    TestUtils.waitForCondition(aVoid -> {
      RealtimeTableDataManager tableDataManager =
          (RealtimeTableDataManager) serverStarter.getServerInstance().getInstanceDataManager()
              .getTableDataManager(realtimeTableName);
      return tableDataManager != null
          && tableDataManager.getTableUpsertMetadataManager() instanceof DummyTableUpsertMetadataManager;
    }, 60_000L, "Failed to create DummyTableUpsertMetadataManager");

    // Clean up
    dropRealtimeTable(dummyTableName);
    deleteSchema(dummyTableName);
    waitForEVToDisappear(dummyTableName);
    serverStarter.stop();
    TestUtils.waitForCondition(aVoid -> _helixResourceManager.dropInstance(instanceId).isSuccessful(), 60_000L,
        "Failed to drop server");
    // Re-initialize the executor for SegmentBuildTimeLeaseExtender to avoid the NullPointerException for the other
    // tests.
    SegmentBuildTimeLeaseExtender.initExecutor();
  }

  @Test
  public void testUpsertCompaction()
      throws Exception {
    String kafkaTopicName = getKafkaTopic() + "-with-compaction";
    setUpKafka(kafkaTopicName, INPUT_DATA_LARGE_TAR_FILE);

    String tableName = "gameScoresWithCompaction";
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);
    upsertConfig.setEnableSnapshot(true);
    TableConfig tableConfig = setUpTable(tableName, kafkaTopicName, upsertConfig);
    tableConfig.setTaskConfig(getCompactionTaskConfig());
    updateTableConfig(tableConfig);

    waitForAllDocsLoaded(tableName, 600_000L, 1000);
    assertEquals(queryCountStar(tableName), 3);
    assertEquals(getScore(tableName), 3692);
    waitForNumQueriedSegmentsToConverge(tableName, 10_000L, 3, 2);

    // NOTE:
    // Snapshot is taken only for immutable segments when a new consuming segment starts consuming. In order to get
    // consistent behavior, we take the following steps:
    // 1. Pause consumption
    // 2. Wait until all consuming segments are committed and sealed (loaded as immutable segment)
    // 3. Resume consumption to trigger the snapshot
    // 4. Wait until new consuming segments show up
    // 5. Schedule compaction task which compact the segments based on the snapshot
    sendPostRequest(_controllerRequestURLBuilder.forPauseConsumption(tableName));
    waitForNumQueriedSegmentsToConverge(tableName, 600_000L, 3, 0);
    sendPostRequest(_controllerRequestURLBuilder.forResumeConsumption(tableName));
    waitForNumQueriedSegmentsToConverge(tableName, 600_000L, 5, 2);
    String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    assertNotNull(_taskManager.scheduleAllTasksForTable(realtimeTableName, null)
        .get(MinionConstants.UpsertCompactionTask.TASK_TYPE));
    waitForTaskToComplete();
    // 2 segments should be compacted (351 rows -> 1 row; 500 rows -> 2 rows), 1 segment (149 rows) should be deleted
    waitForAllDocsLoaded(tableName, 600_000L, 3);
    assertEquals(queryCountStar(tableName), 3);
    assertEquals(getScore(tableName), 3692);
    waitForNumQueriedSegmentsToConverge(tableName, 10_000L, 4, 2);
  }

  @Test
  public void testUpsertCompactionInMemory()
      throws Exception {
    String kafkaTopicName = getKafkaTopic() + "-with-compaction-in-memory";
    setUpKafka(kafkaTopicName, INPUT_DATA_LARGE_TAR_FILE);

    String tableName = "gameScoresWithCompactionInMemory";
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);
    TableConfig tableConfig = setUpTable(tableName, kafkaTopicName, upsertConfig);
    TableTaskConfig taskConfig = getCompactionTaskConfig();
    taskConfig.getConfigsForTaskType(MinionConstants.UpsertCompactionTask.TASK_TYPE)
        .put("validDocIdsType", ValidDocIdsType.IN_MEMORY.name());
    tableConfig.setTaskConfig(taskConfig);
    updateTableConfig(tableConfig);

    waitForAllDocsLoaded(tableName, 600_000L, 1000);
    assertEquals(queryCountStar(tableName), 3);
    assertEquals(getScore(tableName), 3692);
    waitForNumQueriedSegmentsToConverge(tableName, 10_000L, 3);

    // NOTE: When in-memory valid doc ids are used, no need to pause/resume consumption to trigger the snapshot.
    String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    assertNotNull(_taskManager.scheduleAllTasksForTable(realtimeTableName, null)
        .get(MinionConstants.UpsertCompactionTask.TASK_TYPE));
    waitForTaskToComplete();
    // 1 segment should be compacted (500 rows -> 2 rows)
    waitForAllDocsLoaded(tableName, 600_000L, 502);
    assertEquals(queryCountStar(tableName), 3);
    assertEquals(getScore(tableName), 3692);
    waitForNumQueriedSegmentsToConverge(tableName, 10_000L, 3);
  }

  @Test
  public void testUpsertCompactionWithSoftDelete()
      throws Exception {
    String kafkaTopicName = getKafkaTopic() + "-with-compaction-in-memory-with-delete";
    setUpKafka(kafkaTopicName, INPUT_DATA_LARGE_TAR_FILE);

    String tableName = "gameScoresWithCompactionInMemoryWithDelete";
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);
    TableConfig tableConfig = setUpTable(tableName, kafkaTopicName, upsertConfig);
    TableTaskConfig taskConfig = getCompactionTaskConfig();
    taskConfig.getConfigsForTaskType(MinionConstants.UpsertCompactionTask.TASK_TYPE)
        .put("validDocIdsType", ValidDocIdsType.IN_MEMORY_WITH_DELETE.name());
    tableConfig.setTaskConfig(taskConfig);
    updateTableConfig(tableConfig);

    waitForAllDocsLoaded(tableName, 600_000L, 1000);
    assertEquals(queryCountStar(tableName), 3);
    assertEquals(getScore(tableName), 3692);
    waitForNumQueriedSegmentsToConverge(tableName, 10_000L, 3);

    // Push data to delete 2 rows
    List<String> deleteRecords = List.of("102,Clifford,counter-strike,102,1681254200000,true",
        "100,Zook,counter-strike,2050,1681377200000,true");
    pushCsvIntoKafka(deleteRecords, kafkaTopicName, 0);
    waitForAllDocsLoaded(tableName, 600_000L, 1002);
    assertEquals(queryCountStar(tableName), 1);

    // Force commit the segments to ensure the deleting rows are part of the committed segments
    sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableName));
    waitForNumQueriedSegmentsToConverge(tableName, 10_000L, 5, 2);

    String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    assertNotNull(_taskManager.scheduleAllTasksForTable(realtimeTableName, null)
        .get(MinionConstants.UpsertCompactionTask.TASK_TYPE));
    waitForTaskToComplete();
    // 1 segment should be compacted (351 rows -> 1 rows), 2 segments (500 rows, 151 rows) should be deleted
    waitForAllDocsLoaded(tableName, 600_000L, 1);
    assertEquals(queryCountStar(tableName), 1);
    assertEquals(getNumDeletedRows(tableName), 0);
    assertEquals(getScore(tableName), 3692);
    waitForNumQueriedSegmentsToConverge(tableName, 10_000L, 3, 2);
  }

  private long getScore(String tableName) {
    return (long) getPinotConnection().execute("SELECT score FROM " + tableName + " WHERE playerId = 101")
        .getResultSet(0).getFloat(0);
  }

  private long getNumDeletedRows(String tableName) {
    return getPinotConnection().execute(
            "SELECT COUNT(*) FROM " + tableName + " WHERE deleted = true OPTION(skipUpsert=true)").getResultSet(0)
        .getLong(0);
  }

  private void waitForTaskToComplete() {
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

  private TableTaskConfig getCompactionTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put(MinionConstants.UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "0d");
    tableTaskConfigs.put(MinionConstants.UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT, "1");
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.UpsertCompactionTask.TASK_TYPE, tableTaskConfigs));
  }
}
