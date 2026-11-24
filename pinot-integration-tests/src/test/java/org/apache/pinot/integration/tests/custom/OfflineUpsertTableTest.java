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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;


/// Integration test for offline table upsert support.
///
/// Tests that OFFLINE tables with upsert enabled correctly deduplicate records by primary key,
/// keeping the latest record based on the comparison column (time column).
///
/// Test data layout:
///   Segment 1: playerId=100 (score=2000, ts=1671036400000), playerId=101 (score=3000, ts=1671036400000)
///   Segment 2: playerId=100 (score=2500, ts=1681036400000), playerId=102 (score=4000, ts=1671036400000)
///   Segment 3: playerId=101 (score=3500, ts=1681036400000), playerId=102 (score=4500, ts=1681036400000)
///
/// After upsert dedup (latest by timestampInEpoch):
///   playerId=100 -> score=2500, playerId=101 -> score=3500, playerId=102 -> score=4500
@Test(suiteName = "CustomClusterIntegrationTest")
public class OfflineUpsertTableTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "OfflineUpsertTableTest";
  private static final String PRIMARY_KEY_COL = "playerId";
  private static final String TIME_COL_NAME = "timestampInEpoch";
  private static final int NUM_PARTITIONS = 1;
  private static final int TOTAL_RAW_RECORDS = 6;
  private static final int UNIQUE_PRIMARY_KEYS = 3;
  private static final String COLUMNS = "playerId, name, game, score, timestampInEpoch";
  private static final String ROWS_QUERY = "SELECT " + COLUMNS + " FROM " + TABLE_NAME + " ORDER BY playerId LIMIT 100";
  private static final String RAW_ROWS_QUERY = "SELECT " + COLUMNS + " FROM " + TABLE_NAME
      + " ORDER BY playerId, timestampInEpoch LIMIT 100 OPTION(skipUpsert=true)";
  private static final List<Object> ROW_100_OLD = List.of(100, "Alice", "chess", 2000.0, 1671036400000L);
  private static final List<Object> ROW_100 = List.of(100, "Alice", "chess", 2500.0, 1681036400000L);
  private static final List<Object> ROW_101_OLD = List.of(101, "Bob", "chess", 3000.0, 1671036400000L);
  private static final List<Object> ROW_101 = List.of(101, "Bob", "chess", 3500.0, 1681036400000L);
  private static final List<Object> ROW_102_OLD = List.of(102, "Charlie", "chess", 4000.0, 1671036400000L);
  private static final List<Object> ROW_102 = List.of(102, "Charlie", "chess", 4500.0, 1681036400000L);

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return UNIQUE_PRIMARY_KEYS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(PRIMARY_KEY_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addSingleValueDimension("game", FieldSpec.DataType.STRING)
        .addMetric("score", FieldSpec.DataType.FLOAT)
        .addDateTime(TIME_COL_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(PRIMARY_KEY_COL))
        .build();
  }

  @Override
  public List<File> createAvroFiles() {
    return List.of();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);

    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(PRIMARY_KEY_COL, new ColumnPartitionConfig("Murmur", NUM_PARTITIONS));
    InstanceTagPoolConfig tagPoolConfig = new InstanceTagPoolConfig(
        TagNameUtils.getOfflineTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME), false, 0, null);
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, 2, 0, NUM_PARTITIONS, 1, false, null);
    InstanceAssignmentConfig instanceAssignmentConfig =
        new InstanceAssignmentConfig(tagPoolConfig, null, replicaGroupPartitionConfig, null, false);

    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL_NAME)
        .setNumReplicas(2)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null,
            RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.OFFLINE.name(), instanceAssignmentConfig))
        .setSegmentAssignmentConfigMap(Map.of(InstancePartitionsType.OFFLINE.name(),
            new SegmentAssignmentConfig(null, PRIMARY_KEY_COL)))
        .build();
  }

  @Override
  protected void setUpTable()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    buildAndUploadTestSegments(tableConfig, schema);
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    waitForAllDocsLoaded(TOTAL_RAW_RECORDS, timeoutMs);
    waitForClusterStateSettled(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), timeoutMs);
    assertRows(ROWS_QUERY, List.of(ROW_100, ROW_101, ROW_102));
  }

  @Test
  public void testUpsertQueryResults()
      throws Exception {
    // The latest row per player
    assertRows(ROWS_QUERY, List.of(ROW_100, ROW_101, ROW_102));
    // Every uploaded row
    assertRows(RAW_ROWS_QUERY, List.of(ROW_100_OLD, ROW_100, ROW_101_OLD, ROW_101, ROW_102_OLD, ROW_102));
  }

  @Test(dependsOnMethods = "testUpsertQueryResults")
  public void testUpsertAfterAdditionalSegmentUpload()
      throws Exception {
    Schema schema = createSchema();
    TableConfig tableConfig = getHelixResourceManager().getOfflineTableConfig(TABLE_NAME);

    List<GenericRow> rows = new ArrayList<>();
    GenericRow row = new GenericRow();
    row.putValue(PRIMARY_KEY_COL, 100);
    row.putValue("name", "UpdatedPlayer");
    row.putValue("game", "chess");
    row.putValue("score", 9999.0f);
    row.putValue(TIME_COL_NAME, 1691036400000L);
    rows.add(row);

    File newSegmentDir = new File(_tempDir, "newSegmentDir");
    File newTarDir = new File(_tempDir, "newTarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(newSegmentDir, newTarDir);
    buildSegment(tableConfig, schema, "segment_update_0", rows, newSegmentDir, newTarDir);
    uploadSegments(TABLE_NAME, newTarDir);

    waitForAllDocsLoaded(TOTAL_RAW_RECORDS + 1, 600_000L);
    waitForClusterStateSettled(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME), 600_000L);

    // The uploaded row replaces player 100; the other players keep their rows
    List<Object> updatedRow100 = List.of(100, "UpdatedPlayer", "chess", 9999.0, 1691036400000L);
    assertRows(ROWS_QUERY, List.of(updatedRow100, ROW_101, ROW_102));
    // Every uploaded row is still stored, the new one included
    assertRows(RAW_ROWS_QUERY,
        List.of(ROW_100_OLD, ROW_100, updatedRow100, ROW_101_OLD, ROW_101, ROW_102_OLD, ROW_102));
  }

  private void waitForAllDocsLoaded(int expectedRawRecords, long timeoutMs) {
    int numServers = getSharedServerStarters().size();
    AtomicInteger consecutiveSuccesses = new AtomicInteger();
    TestUtils.waitForCondition(() -> {
      try {
        if (getCurrentRawCountStarResult(TABLE_NAME) != expectedRawRecords) {
          consecutiveSuccesses.set(0);
          return false;
        }
        return consecutiveSuccesses.incrementAndGet() >= numServers;
      } catch (Exception e) {
        consecutiveSuccesses.set(0);
        throw e;
      }
    }, 100L, timeoutMs, "Failed to load " + expectedRawRecords + " documents on all servers",
        Duration.ofMillis(timeoutMs / 10));
  }

  private void buildAndUploadTestSegments(TableConfig tableConfig, Schema schema)
      throws Exception {
    List<GenericRow> segment1Rows = new ArrayList<>();
    segment1Rows.add(createRow(100, "Alice", "chess", 2000.0f, 1671036400000L));
    segment1Rows.add(createRow(101, "Bob", "chess", 3000.0f, 1671036400000L));
    buildSegment(tableConfig, schema, "segment_0", segment1Rows, _segmentDir, _tarDir);

    List<GenericRow> segment2Rows = new ArrayList<>();
    segment2Rows.add(createRow(100, "Alice", "chess", 2500.0f, 1681036400000L));
    segment2Rows.add(createRow(102, "Charlie", "chess", 4000.0f, 1671036400000L));
    buildSegment(tableConfig, schema, "segment_1", segment2Rows, _segmentDir, _tarDir);

    List<GenericRow> segment3Rows = new ArrayList<>();
    segment3Rows.add(createRow(101, "Bob", "chess", 3500.0f, 1681036400000L));
    segment3Rows.add(createRow(102, "Charlie", "chess", 4500.0f, 1681036400000L));
    buildSegment(tableConfig, schema, "segment_2", segment3Rows, _segmentDir, _tarDir);

    uploadSegments(TABLE_NAME, _tarDir);
  }

  private GenericRow createRow(int playerId, String name, String game, float score, long timestamp) {
    GenericRow row = new GenericRow();
    row.putValue(PRIMARY_KEY_COL, playerId);
    row.putValue("name", name);
    row.putValue("game", game);
    row.putValue("score", score);
    row.putValue(TIME_COL_NAME, timestamp);
    return row;
  }

  private void buildSegment(TableConfig tableConfig, Schema schema, String segmentName,
      List<GenericRow> rows, File segmentDir, File tarDir)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(segmentDir.getPath());
    config.setTableName(tableConfig.getTableName());
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader recordReader = new GenericRowRecordReader(rows);
    driver.init(config, recordReader);
    driver.build();

    File indexDir = new File(segmentDir, segmentName);
    File segmentTarFile = new File(tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
  }
}
