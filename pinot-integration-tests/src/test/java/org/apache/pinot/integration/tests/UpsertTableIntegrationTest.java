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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.integration.tests.models.DummyTableUpsertMetadataManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Input data - Scores of players
 * Schema
 *  - Dimension fields: playerId:int (primary key), name:string, game:string, deleted:boolean
 *  - Metric fields: score:float
 *  - DataTime fields: timestampInEpoch:long
 */
public class UpsertTableIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String INPUT_DATA_TAR_FILE = "gameScores_csv.tar.gz";
  private static final String CSV_SCHEMA_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final String PARTIAL_UPSERT_TABLE_SCHEMA = "partial_upsert_table_test.schema";
  private static final String CSV_DELIMITER = ",";
  private static final String TABLE_NAME = "gameScores";
  private static final int NUM_SERVERS = 2;
  private static final String PRIMARY_KEY_COL = "playerId";
  protected static final String DELETE_COL = "deleted";

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

    // Start Kafka and push data into Kafka
    startKafka();

    List<File> unpackDataFiles = unpackTarData(INPUT_DATA_TAR_FILE, _tempDir);
    pushCsvIntoKafka(unpackDataFiles.get(0), getKafkaTopic(), 0);  // TODO: Fix

    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(getTableName(), getKafkaTopic(), getNumKafkaPartitions(), csvDecoderProperties, null,
            PRIMARY_KEY_COL);
    addTableConfig(tableConfig);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);

    // Create partial upsert table schema
    Schema partialUpsertSchema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    addSchema(partialUpsertSchema);
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
    return "upsert_table_test.schema";
  }

  @Nullable
  @Override
  protected String getTimeColumnName() {
    return "timestampInEpoch";
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

  private Schema createSchema(String schemaFileName)
      throws IOException {
    InputStream inputStream = BaseClusterIntegrationTest.class.getClassLoader().getResourceAsStream(schemaFileName);
    Assert.assertNotNull(inputStream);
    return Schema.fromInputStream(inputStream);
  }

  private long queryCountStarWithoutUpsert(String tableName) {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0);
  }

  private long getCountStarResultWithoutUpsert() {
    return 10;
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert(getTableName()) == getCountStarResultWithoutUpsert();
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
  }

  @Test
  protected void testDeleteWithFullUpsert()
      throws Exception {
    final UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);

    testDeleteWithFullUpsert(getKafkaTopic() + "-with-deletes", "gameScoresWithDelete", upsertConfig);
  }

  protected void testDeleteWithFullUpsert(String kafkaTopicName, String tableName, UpsertConfig upsertConfig)
      throws Exception {
    // SETUP
    // Create table with delete Record column
    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    Schema upsertSchema = createSchema();
    upsertSchema.setSchemaName(tableName);
    addSchema(upsertSchema);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, PRIMARY_KEY_COL);
    addTableConfig(tableConfig);

    // Push initial 10 upsert records - 3 pks 100, 101 and 102
    List<File> dataFiles = unpackTarData(INPUT_DATA_TAR_FILE, _tempDir);
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);

    // TEST 1: Delete existing primary key
    // Push 2 records with deleted = true - deletes pks 100 and 102
    List<String> deleteRecords = ImmutableList.of("102,Clifford,counter-strike,102,1681054200000,true",
        "100,Zook,counter-strike,2050,1681377200000,true");
    pushCsvIntoKafka(deleteRecords, kafkaTopicName, 0);

    // Wait for all docs (12 with skipUpsert=true) to be loaded
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert(tableName) == 12;
      } catch (Exception e) {
        return null;
      }
    }, 100L, 600_000L, "Failed to load all upsert records for testDeleteWithFullUpsert");

    // Query for number of records in the table - should only return 1
    ResultSet rs = getPinotConnection().execute("SELECT * FROM " + tableName).getResultSet(0);
    Assert.assertEquals(rs.getRowCount(), 1);

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
    Assert.assertNotEquals(playerIdColumnIndex, -1);
    Assert.assertEquals(rs.getString(0, playerIdColumnIndex), "101");

    // Validate deleted records
    rs = getPinotConnection().execute(
        "SELECT playerId FROM " + tableName + " WHERE deleted = true OPTION(skipUpsert=true)").getResultSet(0);
    Assert.assertEquals(rs.getRowCount(), 2);
    for (int i = 0; i < rs.getRowCount(); i++) {
      String playerId = rs.getString(i, 0);
      Assert.assertTrue("100".equalsIgnoreCase(playerId) || "102".equalsIgnoreCase(playerId));
    }

    // TEST 2: Revive a previously deleted primary key
    // Revive pk - 100 by adding a record with a newer timestamp
    List<String> revivedRecord = Collections.singletonList("100,Zook-New,,0.0,1684707335000,false");
    pushCsvIntoKafka(revivedRecord, kafkaTopicName, 0);
    // Wait for the new record (13 with skipUpsert=true) to be indexed
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert(tableName) == 13;
      } catch (Exception e) {
        return null;
      }
    }, 100L, 600_000L, "Failed to load all upsert records for testDeleteWithFullUpsert");

    // Validate: pk is queryable and all columns are overwritten with new value
    rs = getPinotConnection().execute("SELECT playerId, name, game FROM " + tableName + " WHERE playerId = 100")
        .getResultSet(0);
    Assert.assertEquals(rs.getRowCount(), 1);
    Assert.assertEquals(rs.getInt(0, 0), 100);
    Assert.assertEquals(rs.getString(0, 1), "Zook-New");
    Assert.assertEquals(rs.getString(0, 2), "null");

    // Validate: pk lineage still exists
    rs = getPinotConnection().execute(
        "SELECT playerId, name FROM " + tableName + " WHERE playerId = 100 OPTION(skipUpsert=true)").getResultSet(0);

    Assert.assertTrue(rs.getRowCount() > 1);

    // TEARDOWN
    dropRealtimeTable(tableName);
  }

  @Test
  public void testDeleteWithPartialUpsert()
      throws Exception {
    final UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setDeleteRecordColumn(DELETE_COL);

    testDeleteWithPartialUpsert(getKafkaTopic() + "-partial-upsert-with-deletes", "gameScoresPartialUpsertWithDelete",
        upsertConfig);
  }

  protected void testDeleteWithPartialUpsert(String kafkaTopicName, String tableName, UpsertConfig upsertConfig)
      throws Exception {
    final String inputDataTarFile = "gameScores_partial_upsert_csv.tar.gz";

    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("game", UpsertConfig.Strategy.UNION);
    partialUpsertStrategies.put("score", UpsertConfig.Strategy.INCREMENT);
    partialUpsertStrategies.put(DELETE_COL, UpsertConfig.Strategy.OVERWRITE);
    upsertConfig.setPartialUpsertStrategies(partialUpsertStrategies);

    // Create table with delete Record column
    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    Schema partialUpsertSchema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    partialUpsertSchema.setSchemaName(tableName);
    addSchema(partialUpsertSchema);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, PRIMARY_KEY_COL);
    addTableConfig(tableConfig);

    // Push initial 10 upsert records - 3 pks 100, 101 and 102
    List<File> dataFiles = unpackTarData(inputDataTarFile, _tempDir);
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);

    // TEST 1: Delete existing primary key
    // Push 2 records with deleted = true - deletes pks 100 and 102
    List<String> deleteRecords = ImmutableList.of("102,Clifford,counter-strike,102,1681054200000,true",
        "100,Zook,counter-strike,2050,1681377200000,true");
    pushCsvIntoKafka(deleteRecords, kafkaTopicName, 0);

    // Wait for all docs (12 with skipUpsert=true) to be loaded
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert(tableName) == 12;
      } catch (Exception e) {
        return null;
      }
    }, 100L, 600_000L, "Failed to load all upsert records for testDeleteWithFullUpsert");

    // Query for number of records in the table - should only return 1
    ResultSet rs = getPinotConnection().execute("SELECT * FROM " + tableName).getResultSet(0);
    Assert.assertEquals(rs.getRowCount(), 1);

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
    Assert.assertNotEquals(playerIdColumnIndex, -1);
    Assert.assertEquals(rs.getString(0, playerIdColumnIndex), "101");

    // Validate deleted records
    rs = getPinotConnection().execute(
        "SELECT playerId FROM " + tableName + " WHERE deleted = true OPTION(skipUpsert=true)").getResultSet(0);
    Assert.assertEquals(rs.getRowCount(), 2);
    for (int i = 0; i < rs.getRowCount(); i++) {
      String playerId = rs.getString(i, 0);
      Assert.assertTrue("100".equalsIgnoreCase(playerId) || "102".equalsIgnoreCase(playerId));
    }

    // TEST 2: Revive a previously deleted primary key
    // Revive pk - 100 by adding a record with a newer timestamp
    List<String> revivedRecord = Collections.singletonList("100,Zook,,0.0,1684707335000,false");
    pushCsvIntoKafka(revivedRecord, kafkaTopicName, 0);
    // Wait for the new record (13 with skipUpsert=true) to be indexed
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert(tableName) == 13;
      } catch (Exception e) {
        return null;
      }
    }, 100L, 600_000L, "Failed to load all upsert records for testDeleteWithFullUpsert");

    // Validate: pk is queryable and all columns are overwritten with new value
    rs = getPinotConnection().execute("SELECT playerId, name, game FROM " + tableName + " WHERE playerId = 100")
        .getResultSet(0);
    Assert.assertEquals(rs.getRowCount(), 1);
    Assert.assertEquals(rs.getInt(0, 0), 100);
    Assert.assertEquals(rs.getString(0, 1), "Zook");
    Assert.assertEquals(rs.getString(0, 2), "[\"null\"]");

    // Validate: pk lineage still exists
    rs = getPinotConnection().execute(
        "SELECT playerId, name FROM " + tableName + " WHERE playerId = 100 OPTION(skipUpsert=true)").getResultSet(0);

    Assert.assertTrue(rs.getRowCount() > 1);

    // TEARDOWN
    dropRealtimeTable(tableName);
  }

  @Test
  public void testDefaultMetadataManagerClass()
      throws Exception {
    PinotConfiguration config = getServerConf(12345);
    config.setProperty(CommonConstants.Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "."
            + HelixInstanceDataManagerConfig.UPSERT_DEFAULT_METADATA_MANAGER_CLASS,
        DummyTableUpsertMetadataManager.class.getName());

    BaseServerStarter serverStarter = startOneServer(config);
    String dummyTableName = "dummyTable123";
    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);

    TableConfig tableConfig =
        createCSVUpsertTableConfig(dummyTableName, getKafkaTopic(), getNumKafkaPartitions(), csvDecoderProperties, null,
            PRIMARY_KEY_COL);
    Schema schema = createSchema();
    schema.setSchemaName(dummyTableName);
    addSchema(schema);
    addTableConfig(tableConfig);

    Thread.sleep(10000L);
    RealtimeTableDataManager tableDataManager =
        (RealtimeTableDataManager) serverStarter.getServerInstance().getInstanceDataManager()
            .getTableDataManager(TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(dummyTableName));
    Assert.assertTrue(tableDataManager.getTableUpsertMetadataManager() instanceof DummyTableUpsertMetadataManager);
    serverStarter.stop();
  }
}
