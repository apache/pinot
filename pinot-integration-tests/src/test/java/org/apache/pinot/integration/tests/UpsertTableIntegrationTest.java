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
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class UpsertTableIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_SERVERS = 2;
  private static final String PRIMARY_KEY_COL = "playerId";
  private static final String DELETED_COL = "deleted";
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

    List<File> unpackDataFiles = unpackTarData("gameScores.tar.gz", _tempDir);
    pushCsvIntoKafka(unpackDataFiles.get(0), getKafkaTopic(), 0);  // TODO: Fix

    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createCSVUpsertTableConfig("gameScores",
        PRIMARY_KEY_COL, null, getKafkaTopic(), getNumKafkaPartitions());
    addTableConfig(tableConfig);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
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

  @Override
  protected String getSchemaName() {
    return "playerScores";
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
    return "gameScores";
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100, 101, 102
    return 3;
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
        return queryCountStarWithoutUpsert("gameScores") == getCountStarResultWithoutUpsert();
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
  }

  @Test
  public void testDeleteWithUpsert()
      throws Exception {
    String kafkaTopicName = getKafkaTopic() + "-with-deletes";
    String tableName = "gameScoresWithDelete";

    // Create table with delete Record column
    TableConfig tableConfig = createCSVUpsertTableConfig(tableName,
        PRIMARY_KEY_COL, DELETED_COL, kafkaTopicName, getNumKafkaPartitions());
    addTableConfig(tableConfig);

    // Push initial 10 upsert records - 3 pks 100, 101 and 102
    List<File> dataFiles = unpackTarData("gameScores.tar.gz", _tempDir);
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);

    // Push 2 records with deleted = true - deletes pks 100 and 102
    dataFiles = unpackTarData("gameScores_with_deleteRecords.tar.gz", _tempDir);
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);

    // Wait for all docs (12 with skipUpsert=true) to be loaded
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert("gameScoresWithDelete") == 12;
      } catch (Exception e) {
        return null;
      }
    }, 100L, 600_000L, "Failed to load all upsert records for testDeleteWithUpsert");

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
    rs = getPinotConnection()
        .execute("SELECT playerId FROM " + tableName
            + " WHERE deleted = true OPTION(skipUpsert=true)").getResultSet(0);
    Assert.assertEquals(rs.getRowCount(), 2);
    for (int i = 0; i < rs.getRowCount(); i++) {
      String playerId = rs.getString(i, 0);
      Assert.assertTrue("100".equalsIgnoreCase(playerId) || "102".equalsIgnoreCase(playerId));
    }
  }
}
