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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class UpsertCompactionTaskGeneratorTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";

  @Test
  public void testValidate() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .build();
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfig,
        UpsertCompactionTask.TASK_TYPE));

    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME);
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));

    tableConfigBuilder = tableConfigBuilder
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL));
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));

    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    tableConfigBuilder = tableConfigBuilder.setTaskConfig(new TableTaskConfig(tableTaskConfigs));
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));

    Map<String, String> compactionConfigs = new HashMap<>();
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    tableConfigBuilder = tableConfigBuilder.setTaskConfig(new TableTaskConfig(tableTaskConfigs));
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));

    compactionConfigs.put(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, "1d");
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    tableConfigBuilder = tableConfigBuilder.setTaskConfig(new TableTaskConfig(tableTaskConfigs));
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));

    compactionConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "7d");
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    tableConfigBuilder = tableConfigBuilder.setTaskConfig(new TableTaskConfig(tableTaskConfigs));
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));

    compactionConfigs.put(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT, "5000000");
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    tableConfigBuilder = tableConfigBuilder.setTaskConfig(new TableTaskConfig(tableTaskConfigs));
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));

    tableConfigBuilder.setSegmentPartitionConfig(new SegmentPartitionConfig(
        Collections.singletonMap("memberId",
            new ColumnPartitionConfig("murmur", 1))));
    assertTrue(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build(),
        UpsertCompactionTask.TASK_TYPE));
  }

  @Test
  public void testGenerateTasksValidatesTableConfigs() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .build();
    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    TableConfig realtimeTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .build();
    pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());
  }

  @Test
  public void testGenerateTasksWithNoSegments() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    compactionConfigs.put(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, "1d");
    compactionConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "7d");
    compactionConfigs.put(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT, "5000000");
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("memberId",
                new ColumnPartitionConfig("murmur", 1))))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(Collections.emptyList()));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithConsumingSegment() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    compactionConfigs.put(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, "1d");
    compactionConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "7d");
    compactionConfigs.put(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT, "5000000");
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("memberId",
                new ColumnPartitionConfig("murmur", 1))))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    SegmentZKMetadata consumingSegment = new SegmentZKMetadata("testTable__0");
    consumingSegment.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(consumingSegment));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithNewlyCompletedSegment() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    compactionConfigs.put(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, "1d");
    compactionConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "7d");
    compactionConfigs.put(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT, "5000000");
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("memberId",
                new ColumnPartitionConfig("murmur", 1))))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    SegmentZKMetadata completedSegment = new SegmentZKMetadata("testTable__0");
    completedSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    Long endTime = System.currentTimeMillis();
    Long startTime = endTime - TimeUtils.convertPeriodToMillis("1d");
    completedSegment.setStartTime(startTime);
    completedSegment.setEndTime(endTime);
    completedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(completedSegment));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithOlderCompletedSegments() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    String bucketPeriod = "1d";
    compactionConfigs.put(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, bucketPeriod);
    String bufferPeriod = "7d";
    compactionConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, bufferPeriod);
    String maxRecordsPerSegment = "5000000";
    compactionConfigs.put(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT, maxRecordsPerSegment);
    String invalidRecordsThreshold = "2500000";
    compactionConfigs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD, invalidRecordsThreshold);
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("memberId",
                new ColumnPartitionConfig("murmur", 1))))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    SegmentZKMetadata completedSegment0 = new SegmentZKMetadata("testTable__0");
    completedSegment0.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    Long endTime = System.currentTimeMillis()
        - TimeUtils.convertPeriodToMillis(bufferPeriod)
        - TimeUtils.convertPeriodToMillis("1d");
    Long startTime = endTime - TimeUtils.convertPeriodToMillis("1d");
    completedSegment0.setStartTime(startTime);
    completedSegment0.setEndTime(endTime);
    completedSegment0.setTimeUnit(TimeUnit.MILLISECONDS);
    SegmentZKMetadata completedSegment1 = new SegmentZKMetadata("testTable__1");
    completedSegment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    endTime = System.currentTimeMillis() - TimeUtils.convertPeriodToMillis(bufferPeriod);
    startTime = endTime - TimeUtils.convertPeriodToMillis("1d");
    completedSegment1.setStartTime(startTime);
    completedSegment1.setEndTime(endTime);
    completedSegment1.setTimeUnit(TimeUnit.MILLISECONDS);
    SegmentPartitionMetadata partitionMetadata = new SegmentPartitionMetadata(
        Collections.singletonMap("memberId",
            new ColumnPartitionMetadata("murmur"))
    )
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(completedSegment0, completedSegment1));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 1);
    Map<String, String> configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    String[] segmentNames = configs.get(MinionConstants.SEGMENT_NAME_KEY)
        .split(MinionConstants.SEGMENT_NAME_SEPARATOR);
    assertEquals(segmentNames[0], completedSegment0.getSegmentName());
    assertEquals(segmentNames[1], completedSegment1.getSegmentName());
    assertEquals(configs.get(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY), bucketPeriod);
    assertEquals(configs.get(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT), maxRecordsPerSegment);
    assertEquals(configs.get(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD), invalidRecordsThreshold);
  }

  @Test
  public void testGenerateTasksPartitionedTable() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    String bucketPeriod = "1d";
    compactionConfigs.put(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, bucketPeriod);
    String bufferPeriod = "7d";
    compactionConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, bufferPeriod);
    String maxRecordsPerSegment = "5000000";
    compactionConfigs.put(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT, maxRecordsPerSegment);
    String invalidRecordsThreshold = "2500000";
    compactionConfigs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD, invalidRecordsThreshold);
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Collections.singletonMap("memberId",
                new ColumnPartitionConfig("murmur", 2))))
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    Long endTime = System.currentTimeMillis() - TimeUtils.convertPeriodToMillis(bufferPeriod);
    Long startTime = endTime - TimeUtils.convertPeriodToMillis("1d");
    SegmentZKMetadata completedSegment0 = new SegmentZKMetadata("testTable__0");
    completedSegment0.setStartTime(startTime);
    completedSegment0.setEndTime(endTime);
    completedSegment0.setTimeUnit(TimeUnit.MILLISECONDS);
    SegmentPartitionMetadata partitionMetadata0 = new SegmentPartitionMetadata(
        Collections.singletonMap("memberId",
            new ColumnPartitionMetadata("murmur", 2,
                Collections.singleton(0), null)));
    completedSegment0.setPartitionMetadata(partitionMetadata0);
    completedSegment0.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    SegmentZKMetadata completedSegment1 = new SegmentZKMetadata("testTable__1");
    SegmentPartitionMetadata partitionMetadata1 = new SegmentPartitionMetadata(
        Collections.singletonMap("memberId",
            new ColumnPartitionMetadata("murmur", 2,
                Collections.singleton(1), null)));
    completedSegment1.setPartitionMetadata(partitionMetadata1);
    completedSegment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    completedSegment1.setStartTime(startTime);
    completedSegment1.setEndTime(endTime);
    completedSegment1.setTimeUnit(TimeUnit.MILLISECONDS);
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(completedSegment0, completedSegment1));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 2);
    boolean foundTaskConfig0 = false;
    boolean foundTaskConfig1 = false;
    for (PinotTaskConfig pinotTaskConfig : pinotTaskConfigs) {
      Map<String, String> configs = pinotTaskConfig.getConfigs();
      assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
      String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
      if (segmentName == completedSegment0.getSegmentName()) {
        foundTaskConfig0 = true;
      } else if (segmentName == completedSegment1.getSegmentName()) {
        foundTaskConfig1 = true;
      }
      assertEquals(configs.get(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY), bucketPeriod);
      assertEquals(configs.get(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT), maxRecordsPerSegment);
      assertEquals(configs.get(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD), invalidRecordsThreshold);
    }
    assertTrue(foundTaskConfig0 && foundTaskConfig1);
  }
}
