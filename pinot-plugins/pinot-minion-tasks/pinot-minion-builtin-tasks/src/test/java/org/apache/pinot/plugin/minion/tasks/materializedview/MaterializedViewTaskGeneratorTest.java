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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.apache.pinot.common.minion.MaterializedViewTaskMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MaterializedViewTaskGeneratorTest {

  @Test
  public void testExtractSourceTableName() {
    assertEquals(MaterializedViewAnalyzer.extractSourceTableName(
        "select column1 from another_table group by column1"), "another_table");
    assertEquals(MaterializedViewAnalyzer.extractSourceTableName(
        "select a from spacedTable where x = 1"), "spacedTable");
  }

  @Test
  public void testAppendTimeRangeNoWhere() {
    String sql = "select column1, min(column1) as min_column1 from orders group by column1;";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "ts", "1000", "2000");
    assertEquals(result,
        "select column1, min(column1) as min_column1 from orders WHERE ts >= 1000 AND ts < 2000 group by column1");
  }

  @Test
  public void testAppendTimeRangeWithExistingWhere() {
    String sql = "select column1 from orders where status = 'active' group by column1;";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "ts", "1000", "2000");
    assertTrue(result.contains("status = 'active' AND ts >= 1000 AND ts < 2000"));
    assertTrue(result.contains("group by column1"));
  }

  @Test
  public void testAppendTimeRangeNoGroupBy() {
    String sql = "select count(*) as cnt from orders;";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "created_at", "5000", "6000");
    assertTrue(result.contains("WHERE created_at >= 5000 AND created_at < 6000"));
  }

  @Test
  public void testAppendTimeRangeWithDaysEpochFormat() {
    DateTimeFormatSpec formatSpec = new DateTimeFormatSpec("1:DAYS:EPOCH");
    long windowStartMs = 1711497600000L; // 2024-03-27 00:00:00 UTC
    long windowEndMs = 1711584000000L;   // 2024-03-28 00:00:00 UTC
    String windowStart = formatSpec.fromMillisToFormat(windowStartMs);
    String windowEnd = formatSpec.fromMillisToFormat(windowEndMs);

    assertEquals(windowStart, "19808");
    assertEquals(windowEnd, "19809");

    String sql = "select column1, count(*) as cnt from orders group by column1;";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "DaysSinceEpoch", windowStart, windowEnd);
    assertEquals(result,
        "select column1, count(*) as cnt from orders"
            + " WHERE DaysSinceEpoch >= 19808 AND DaysSinceEpoch < 19809 group by column1");
  }

  @Test
  public void testAppendTimeRangeWithHoursEpochFormat() {
    DateTimeFormatSpec formatSpec = new DateTimeFormatSpec("1:HOURS:EPOCH");
    long windowStartMs = 1711497600000L;
    long windowEndMs = 1711501200000L; // +1 hour
    String windowStart = formatSpec.fromMillisToFormat(windowStartMs);
    String windowEnd = formatSpec.fromMillisToFormat(windowEndMs);

    assertEquals(windowStart, "475416");
    assertEquals(windowEnd, "475417");

    String sql = "select column1 from orders;";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "HoursSinceEpoch", windowStart, windowEnd);
    assertTrue(result.contains("WHERE HoursSinceEpoch >= 475416 AND HoursSinceEpoch < 475417"));
  }

  @Test
  public void testAppendTimeRangeWithMillisEpochFormat() {
    DateTimeFormatSpec formatSpec = new DateTimeFormatSpec("1:MILLISECONDS:EPOCH");
    long windowStartMs = 1711497600000L;
    long windowEndMs = 1711584000000L;
    String windowStart = formatSpec.fromMillisToFormat(windowStartMs);
    String windowEnd = formatSpec.fromMillisToFormat(windowEndMs);

    assertEquals(windowStart, "1711497600000");
    assertEquals(windowEnd, "1711584000000");

    String sql = "select column1 from orders;";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "ts", windowStart, windowEnd);
    assertTrue(result.contains("WHERE ts >= 1711497600000 AND ts < 1711584000000"));
  }

  @Test
  public void testAppendTimeColumnWithGroupBy() {
    String sql = "select city, count(*) as cnt from orders group by city;";
    String result = MaterializedViewTaskGenerator.appendTimeColumnToSelect(sql, "DaysSinceEpoch");
    assertEquals(result,
        "select DaysSinceEpoch, city, count(*) as cnt from orders group by city, DaysSinceEpoch");
  }

  @Test
  public void testAppendTimeColumnWithoutGroupBy() {
    String sql = "select city, count(*) as cnt from orders;";
    String result = MaterializedViewTaskGenerator.appendTimeColumnToSelect(sql, "ts");
    assertEquals(result, "select ts, city, count(*) as cnt from orders");
  }

  @Test
  public void testAppendTimeColumnAlreadyInSelect() {
    String sql = "select ts, city, count(*) as cnt from orders group by ts, city";
    String result = MaterializedViewTaskGenerator.appendTimeColumnToSelect(sql, "ts");
    assertEquals(result, sql);
  }

  @Test
  public void testAppendTimeColumnWithGroupByAndOrderBy() {
    String sql = "select city, sum(amount) as total from orders group by city order by total";
    String result = MaterializedViewTaskGenerator.appendTimeColumnToSelect(sql, "DaysSinceEpoch");
    assertEquals(result,
        "select DaysSinceEpoch, city, sum(amount) as total from orders"
            + " group by city, DaysSinceEpoch order by total");
  }

  /**
   * Verifies that on cold-start the generator:
   * <ol>
   *   <li>Initializes {@link MaterializedViewMetadata} in ZK with the correct base table info</li>
   *   <li>Includes {@code SOURCE_TABLE_NAME_KEY} in the generated task config</li>
   * </ol>
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testGenerateTasksColdStartPersistsMetadata() {
    String mvTableName = "mv_orders_OFFLINE";
    String sourceTableName = "orders";
    String sourceTableWithType = "orders_OFFLINE";
    String timeColumn = "DaysSinceEpoch";
    long segmentStartTimeMs = 1711497600000L; // 2024-03-27 00:00:00 UTC
    long bucketMs = 86400000L; // 1 day

    // Build MV table config
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MaterializedViewTask.DEFINED_SQL_KEY,
        "SELECT city, count(*) as cnt FROM orders GROUP BY city");
    taskConfigs.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(MaterializedViewTask.TASK_TYPE, taskConfigs);
    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(mvTableName)
        .setTimeColumnName(timeColumn)
        .setTaskConfig(new TableTaskConfig(taskConfigsMap))
        .build();

    // Build source table config
    TableConfig sourceTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(sourceTableWithType)
        .setTimeColumnName(timeColumn)
        .build();

    // Source schema with time column
    Schema sourceSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(timeColumn, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    // Build source segment metadata
    SegmentZKMetadata segmentMetadata = new SegmentZKMetadata("orders_segment_0");
    segmentMetadata.setStartTime(segmentStartTimeMs);
    segmentMetadata.setEndTime(segmentStartTimeMs + bucketMs);
    List<SegmentZKMetadata> segmentsList = new ArrayList<>();
    segmentsList.add(segmentMetadata);

    // Mock ClusterInfoAccessor
    ClusterInfoAccessor mockAccessor = mock(ClusterInfoAccessor.class);
    when(mockAccessor.getTaskStates(MaterializedViewTask.TASK_TYPE))
        .thenReturn(Collections.emptyMap());
    when(mockAccessor.getVipUrl()).thenReturn("http://localhost:9000");
    when(mockAccessor.getTableConfig(sourceTableWithType)).thenReturn(sourceTableConfig);
    when(mockAccessor.getTableSchema(sourceTableWithType)).thenReturn(sourceSchema);
    when(mockAccessor.getSegmentsZKMetadata(sourceTableWithType)).thenReturn(segmentsList);

    IdealState idealState = new IdealState(sourceTableWithType);
    idealState.setPartitionState("orders_segment_0", "Server_localhost_7050", "ONLINE");
    when(mockAccessor.getIdealState(sourceTableWithType)).thenReturn(idealState);

    // Cold-start: no existing task metadata
    when(mockAccessor.getMinionTaskMetadataZNRecord(anyString(), anyString())).thenReturn(null);

    // Capture watermark metadata writes
    Map<String, MaterializedViewTaskMetadata> capturedTaskMetadata = new HashMap<>();
    doAnswer(invocation -> {
      MaterializedViewTaskMetadata md = invocation.getArgument(0);
      capturedTaskMetadata.put(md.getTableNameWithType(), md);
      return null;
    }).when(mockAccessor).setMinionTaskMetadata(
        any(MaterializedViewTaskMetadata.class), eq(MaterializedViewTask.TASK_TYPE), anyInt());

    // Mock property store for MaterializedViewMetadata persistence
    HelixPropertyStore<ZNRecord> mockPropertyStore = mock(HelixPropertyStore.class);
    when(mockPropertyStore.set(anyString(), any(ZNRecord.class), anyInt(), anyInt())).thenReturn(true);
    PinotHelixResourceManager mockResourceManager = mock(PinotHelixResourceManager.class);
    when(mockResourceManager.getPropertyStore()).thenReturn(mockPropertyStore);
    when(mockAccessor.getPinotHelixResourceManager()).thenReturn(mockResourceManager);

    // Capture the ZNRecord written for MaterializedViewMetadata
    List<ZNRecord> capturedMvMetadataRecords = new ArrayList<>();
    doAnswer(invocation -> {
      ZNRecord record = invocation.getArgument(1);
      capturedMvMetadataRecords.add(record);
      return true;
    }).when(mockPropertyStore).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());

    MaterializedViewTaskGenerator generator = new MaterializedViewTaskGenerator();
    generator.init(mockAccessor);

    List<PinotTaskConfig> result = generator.generateTasks(Collections.singletonList(mvTableConfig));

    // Verify task config contains SOURCE_TABLE_NAME_KEY and ORIGINAL_DEFINED_SQL_KEY
    assertEquals(result.size(), 1);
    PinotTaskConfig taskConfig = result.get(0);
    assertEquals(taskConfig.getConfigs().get(MaterializedViewTask.SOURCE_TABLE_NAME_KEY), sourceTableName);
    assertEquals(taskConfig.getConfigs().get(MaterializedViewTask.ORIGINAL_DEFINED_SQL_KEY),
        "SELECT city, count(*) as cnt FROM orders GROUP BY city");

    // Verify watermark was initialized
    assertTrue(capturedTaskMetadata.containsKey(mvTableName));
    long expectedWatermark = (segmentStartTimeMs / bucketMs) * bucketMs;
    assertEquals(capturedTaskMetadata.get(mvTableName).getWatermarkMs(), expectedWatermark);

    // Verify MaterializedViewMetadata was persisted with correct fields
    assertEquals(capturedMvMetadataRecords.size(), 1);
    MaterializedViewMetadata mvMetadata = MaterializedViewMetadata.fromZNRecord(capturedMvMetadataRecords.get(0));
    assertEquals(mvMetadata.getMvTableNameWithType(), mvTableName);
    assertNotNull(mvMetadata.getBaseTables());
    assertEquals(mvMetadata.getBaseTables().size(), 1);
    assertEquals(mvMetadata.getBaseTables().get(0), sourceTableName);
    assertEquals(mvMetadata.getTimeRangeRefTable(), sourceTableName);
    assertEquals(mvMetadata.getDefinedSql(), "SELECT city, count(*) as cnt FROM orders GROUP BY city");
    assertTrue(mvMetadata.getBaseToMvPartitionMap().isEmpty());
    assertTrue(mvMetadata.getMvToBasePartitionMap().isEmpty());
  }
}
