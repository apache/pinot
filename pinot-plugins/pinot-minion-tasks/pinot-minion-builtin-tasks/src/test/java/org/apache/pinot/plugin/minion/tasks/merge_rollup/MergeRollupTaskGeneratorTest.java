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
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.SegmentType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link MergeRollupTaskGenerator}
 */
public class MergeRollupTaskGeneratorTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private static final String DAILY = "daily";
  private static final String MONTHLY = "monthly";

  private TableConfig getOfflineTableConfig(Map<String, Map<String, String>> taskConfigsMap) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
        .setTaskConfig(new TableTaskConfig(taskConfigsMap)).build();
  }

  /**
   * Tests for some config checks
   */
  @Test
  public void testGenerateTasksCheckConfigs() {
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    when(mockClusterInfoProvide.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)).thenReturn(new HashMap<>());
    SegmentZKMetadata metadata1 = getSegmentZKMetadata("testTable__0", 5000, 50_000, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(Lists.newArrayList(metadata1));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);

    // Skip task generation, if realtime table
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).build();
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // Skip task generation, if REFRESH table
    IngestionConfig ingestionConfig =
        new IngestionConfig(new BatchIngestionConfig(null, "REFRESH", null), null, null, null, null);
    offlineTableConfig = getOfflineTableConfig(new HashMap<>());
    offlineTableConfig.setIngestionConfig(ingestionConfig);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());
  }

  private void checkPinotTaskConfig(Map<String, String> pinotTaskConfig, String segments, String mergeLevel,
      String mergeType, String partitionBucketTimePeriod, String roundBucketTimePeriod,
      String maxNumRecordsPerSegments) {
    assertEquals(pinotTaskConfig.get(MinionConstants.TABLE_NAME_KEY), OFFLINE_TABLE_NAME);
    assertEquals(pinotTaskConfig.get(MinionConstants.SEGMENT_NAME_KEY), segments);
    assertTrue("true".equalsIgnoreCase(pinotTaskConfig.get(MinionConstants.ENABLE_REPLACE_SEGMENTS_KEY)));
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY), mergeLevel);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY), mergeType);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeTask.PARTITION_BUCKET_TIME_PERIOD_KEY),
        partitionBucketTimePeriod);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY), roundBucketTimePeriod);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY),
        maxNumRecordsPerSegments);
    assertTrue(pinotTaskConfig.get(MinionConstants.MergeRollupTask.SEGMENT_NAME_PREFIX_KEY)
        .startsWith(MinionConstants.MergeRollupTask.MERGED_SEGMENT_NAME_PREFIX));
  }

  private void mockMergeRollupTaskMetadataGetterAndSetter(ClusterInfoAccessor mockClusterInfoProvide) {
    Map<String, MergeRollupTaskMetadata> mockMergeRollupTaskMetadataMap = new HashMap<>();
    doAnswer(invocation -> {
      Object[] arguments = invocation.getArguments();
      if (arguments != null && arguments.length > 0 && arguments[0] != null) {
        MergeRollupTaskMetadata mergeRollupTaskMetadata = (MergeRollupTaskMetadata) arguments[0];
        mockMergeRollupTaskMetadataMap.put(mergeRollupTaskMetadata.getTableNameWithType(), mergeRollupTaskMetadata);
      }
      return null;
    }).when(mockClusterInfoProvide).setMergeRollupTaskMetadata(any(MergeRollupTaskMetadata.class), anyInt());

    when(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(anyString())).thenAnswer(invocation -> {
      Object[] arguments = invocation.getArguments();
      if (arguments != null && arguments.length > 0 && arguments[0] != null) {
        String tableNameWithType = (String) arguments[0];
        if (mockMergeRollupTaskMetadataMap.containsKey(tableNameWithType)) {
          return mockMergeRollupTaskMetadataMap.get(tableNameWithType).toZNRecord();
        }
      }
      return null;
    });
  }

  /**
   * Test empty table
   */
  @Test
  public void testEmptyTable() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "1d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(Collections.emptyList()));
    mockMergeRollupTaskMetadataGetterAndSetter(mockClusterInfoProvide);

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertNull(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME));
    assertEquals(pinotTaskConfigs.size(), 0);
  }

  /**
   * Test empty segment
   */
  @Test
  public void testEmptySegment() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "1d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    String segmentName1 = "testTable__1";
    long currentTime = System.currentTimeMillis();
    SegmentZKMetadata metadata1 =
        getSegmentZKMetadata(segmentName1, currentTime - 500_000L, currentTime, TimeUnit.MILLISECONDS, null);
    metadata1.setTotalDocs(0);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(Lists.newArrayList(metadata1));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertNull(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME));
    assertEquals(pinotTaskConfigs.size(), 0);
  }

  /**
   * Test buffer time
   */
  @Test
  public void testBufferTime() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "1d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    String segmentName1 = "testTable__1";
    long currentTime = System.currentTimeMillis();
    SegmentZKMetadata metadata1 =
        getSegmentZKMetadata(segmentName1, currentTime - 500_000L, currentTime, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(Lists.newArrayList(metadata1));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 0);
  }

  /**
   * Test max number records per task
   */
  @Test
  public void testMaxNumRecordsPerTask() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    SegmentZKMetadata metadata1 =
        getSegmentZKMetadata(segmentName1, 86_400_000L, 90_000_000L, TimeUnit.MILLISECONDS, "download1");
    metadata1.setTotalDocs(2000000L);
    SegmentZKMetadata metadata2 =
        getSegmentZKMetadata(segmentName2, 86_400_000L, 100_000_000L, TimeUnit.MILLISECONDS, "download2");
    metadata2.setTotalDocs(4000000L);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2));

    // Single task
    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName1 + "," + segmentName2, "daily", "concat",
        "1d", null, "1000000");
    assertEquals(pinotTaskConfigs.get(0).getConfigs().get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");

    // Multiple tasks
    String segmentName3 = "testTable__3";
    SegmentZKMetadata metadata3 =
        getSegmentZKMetadata(segmentName3, 86_400_000L, 110_000_000L, TimeUnit.MILLISECONDS, null);
    metadata3.setTotalDocs(5000000L);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2, metadata3));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 2);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName1 + "," + segmentName2, "daily", "concat",
        "1d", null, "1000000");
    checkPinotTaskConfig(pinotTaskConfigs.get(1).getConfigs(), segmentName3, "daily", "concat", "1d", null, "1000000");
  }

  /**
   * Test partitioned table
   */
  @Test
  public void testPartitionedTable() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setSegmentPartitionConfig(new SegmentPartitionConfig(
                Collections.singletonMap("memberId", new ColumnPartitionConfig("murmur", 10))))
            .setTaskConfig(new TableTaskConfig(taskConfigsMap)).build();

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    String segmentName3 = "testTable__3";
    String segmentName4 = "testTable__4";
    SegmentZKMetadata metadata1 =
        getSegmentZKMetadata(segmentName1, 86_400_000L, 90_000_000L, TimeUnit.MILLISECONDS, null);
    metadata1.setPartitionMetadata(new SegmentPartitionMetadata(
        Collections.singletonMap("memberId", new ColumnPartitionMetadata("murmur", 10, Collections.singleton(0)))));
    SegmentZKMetadata metadata2 =
        getSegmentZKMetadata(segmentName2, 86_400_000L, 100_000_000L, TimeUnit.MILLISECONDS, null);
    metadata2.setPartitionMetadata(new SegmentPartitionMetadata(
        Collections.singletonMap("memberId", new ColumnPartitionMetadata("murmur", 10, Collections.singleton(0)))));
    SegmentZKMetadata metadata3 =
        getSegmentZKMetadata(segmentName3, 86_400_000L, 110_000_000L, TimeUnit.MILLISECONDS, null);
    metadata3.setPartitionMetadata(new SegmentPartitionMetadata(
        Collections.singletonMap("memberId", new ColumnPartitionMetadata("murmur", 10, Collections.singleton(1)))));
    SegmentZKMetadata metadata4 =
        getSegmentZKMetadata(segmentName4, 90_000_000L, 110_000_000L, TimeUnit.MILLISECONDS, null);
    metadata4.setPartitionMetadata(new SegmentPartitionMetadata(
        Collections.singletonMap("memberId", new ColumnPartitionMetadata("murmur", 10, Collections.singleton(1)))));
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2, metadata3, metadata4));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 2);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName1 + "," + segmentName2, "daily", "concat",
        "1d", null, "1000000");
    checkPinotTaskConfig(pinotTaskConfigs.get(1).getConfigs(), segmentName3 + "," + segmentName4, "daily", "concat",
        "1d", null, "1000000");

    // With numMaxRecordsPerTask constraints
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    metadata1.setTotalDocs(2000000L);
    metadata2.setTotalDocs(4000000L);
    metadata3.setTotalDocs(5000000L);
    metadata4.setTotalDocs(6000000L);

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 3);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName1 + "," + segmentName2, "daily", "concat",
        "1d", null, "1000000");
    checkPinotTaskConfig(pinotTaskConfigs.get(1).getConfigs(), segmentName3, "daily", "concat", "1d", null, "1000000");
    checkPinotTaskConfig(pinotTaskConfigs.get(2).getConfigs(), segmentName4, "daily", "concat", "1d", null, "1000000");
  }

  /**
   * Test update watermark
   */
  @Test
  public void testUpdateWatermark() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    SegmentZKMetadata metadata1 =
        getSegmentZKMetadata(segmentName1, 90_000_000L, 100_000_000L, TimeUnit.MILLISECONDS, null);
    SegmentZKMetadata metadata2 =
        getSegmentZKMetadata(segmentName2, 345_600_000L, 400_000_000L, TimeUnit.MILLISECONDS, null);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2));
    mockMergeRollupTaskMetadataGetterAndSetter(mockClusterInfoProvide);

    // Cold start, set watermark to smallest segment metadata start time round off to the nearest bucket boundary
    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(DAILY).longValue(), 86_400_000L);
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName1, "daily", "concat", "1d", null, "1000000");

    // Bump up watermark to the merge task execution window start time
    TreeMap<String, Long> waterMarkMap = new TreeMap<>();
    waterMarkMap.put(DAILY, 86_400_000L);
    mockClusterInfoProvide
        .setMergeRollupTaskMetadata(new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, waterMarkMap), -1);
    metadata1.setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, DAILY));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(DAILY).longValue(), 345_600_000L);
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName2, "daily", "concat", "1d", null, "1000000");

    // Not updating watermark if there's no unmerged segments
    waterMarkMap.put(DAILY, 345_600_000L);
    mockClusterInfoProvide
        .setMergeRollupTaskMetadata(new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, waterMarkMap), -1);
    metadata2.setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, DAILY));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(DAILY).longValue(), 345_600_000L);
    assertEquals(pinotTaskConfigs.size(), 0);
  }

  /**
   * Tests for incomplete task
   */
  @Test
  public void testIncompleteTask() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    String mergedSegmentName1 = "merged_testTable__1";
    SegmentZKMetadata metadata1 =
        getSegmentZKMetadata(segmentName1, 90_000_000L, 100_000_000L, TimeUnit.MILLISECONDS, null);
    SegmentZKMetadata metadata2 =
        getSegmentZKMetadata(segmentName2, 345_600_000L, 400_000_000L, TimeUnit.MILLISECONDS, null);
    SegmentZKMetadata merged_metadata1 =
        getSegmentZKMetadata(mergedSegmentName1, 90_000_000L, 100_000_000L, TimeUnit.MILLISECONDS, null);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    Map<String, Long> waterMarkMap = new TreeMap<>();
    waterMarkMap.put(DAILY, 86_400_000L);
    when(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME))
        .thenReturn(new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, waterMarkMap).toZNRecord());

    Map<String, TaskState> taskStatesMap = new HashMap<>();
    String taskName = "Task_MergeRollupTask_" + System.currentTimeMillis();
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.TABLE_NAME_KEY, OFFLINE_TABLE_NAME);
    taskConfigs.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, DAILY);
    taskConfigs.put(MinionConstants.SEGMENT_NAME_KEY, segmentName1);
    when(mockClusterInfoProvide.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)).thenReturn(taskStatesMap);
    when(mockClusterInfoProvide.getTaskConfigs(taskName))
        .thenReturn(Lists.newArrayList(new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, taskConfigs)));

    // If same task and table, IN_PROGRESS, then don't generate again
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2));
    taskStatesMap.put(taskName, TaskState.IN_PROGRESS);
    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // If same task and table, IN_PROGRESS, but older than 1 day, generate
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2));
    String oldTaskName = "Task_MergeRollupTask_" + (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3));
    taskStatesMap.remove(taskName);
    taskStatesMap.put(oldTaskName, TaskState.IN_PROGRESS);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName1, "daily", "concat", "1d", null, "1000000");

    // If same task and table, but COMPLETED, generate
    merged_metadata1
        .setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, DAILY));
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2, merged_metadata1));
    SegmentLineage segmentLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(Collections.singletonList(segmentName1), Collections.singletonList(mergedSegmentName1),
            LineageEntryState.COMPLETED, 11111L));
    when(mockClusterInfoProvide.getSegmentLineage(OFFLINE_TABLE_NAME)).thenReturn(segmentLineage);
    taskStatesMap.put(taskName, TaskState.COMPLETED);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), segmentName2, "daily", "concat", "1d", null, "1000000");
  }

  /**
   * Tests for multilevel selection
   */
  @Test
  public void testSegmentSelectionMultiLevels() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");

    tableTaskConfigs.put("monthly.mergeType", "rollup");
    tableTaskConfigs.put("monthly.bufferTimePeriod", "30d");
    tableTaskConfigs.put("monthly.bucketTimePeriod", "30d");
    tableTaskConfigs.put("monthly.roundBucketTimePeriod", "30d");
    tableTaskConfigs.put("monthly.maxNumRecordsPerSegment", "2000000");
    tableTaskConfigs.put("monthly.maxNumRecordsPerTask", "5000000");

    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    String segmentName3 = "testTable__3";
    String segmentName4 = "testTable__4";
    String segmentName5 = "testTable__5";
    SegmentZKMetadata metadata1 = getSegmentZKMetadata(segmentName1, 86_400_000L, 90_000_000L, TimeUnit.MILLISECONDS,
        null); // starts 1 day since epoch
    SegmentZKMetadata metadata2 = getSegmentZKMetadata(segmentName2, 86_400_000L, 100_000_000L, TimeUnit.MILLISECONDS,
        null); // starts 1 day since epoch
    SegmentZKMetadata metadata3 = getSegmentZKMetadata(segmentName3, 86_400_000L, 110_000_000L, TimeUnit.MILLISECONDS,
        null); // starts 1 day since epoch
    SegmentZKMetadata metadata4 =
        getSegmentZKMetadata(segmentName4, 2_505_600_000L, 2_592_010_000L, TimeUnit.MILLISECONDS,
            null); // starts 29 days since epoch
    SegmentZKMetadata metadata5 =
        getSegmentZKMetadata(segmentName5, 2_592_000_000L, 2_592_020_000L, TimeUnit.MILLISECONDS,
            null); // starts 30 days since epoch
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2, metadata3, metadata4, metadata5));
    mockMergeRollupTaskMetadataGetterAndSetter(mockClusterInfoProvide);

    // Cold start only schedule daily merge tasks
    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(DAILY).longValue(), 86_400_000L);
    assertEquals(pinotTaskConfigs.size(), 1);
    Map<String, String> taskConfigsDaily1 = pinotTaskConfigs.get(0).getConfigs();
    checkPinotTaskConfig(taskConfigsDaily1, segmentName1 + "," + segmentName2 + "," + segmentName3, "daily", "concat",
        "1d", null, "1000000");

    // Monthly task is not scheduled until there are 30 days daily merged segments available (monthly merge window endTimeMs > daily watermark)
    String segmentNameMergedDaily1 = "merged_testTable__1__2__3";
    SegmentZKMetadata metadataMergedDaily1 =
        getSegmentZKMetadata(segmentNameMergedDaily1, 86_400_000L, 110_000_000L, TimeUnit.MILLISECONDS, null);
    metadataMergedDaily1
        .setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, DAILY));
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2, metadata3, metadata4, metadata5, metadataMergedDaily1));

    SegmentLineage segmentLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(Arrays.asList(segmentName1, segmentName2, segmentName3),
            Collections.singletonList(segmentNameMergedDaily1), LineageEntryState.COMPLETED, 11111L));
    when(mockClusterInfoProvide.getSegmentLineage(OFFLINE_TABLE_NAME)).thenReturn(segmentLineage);

    Map<String, TaskState> taskStatesMap = new HashMap<>();
    String taskName1 = "Task_MergeRollupTask_1";
    taskStatesMap.put(taskName1, TaskState.COMPLETED);
    when(mockClusterInfoProvide.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)).thenReturn(taskStatesMap);
    when(mockClusterInfoProvide.getTaskConfigs(taskName1)).thenReturn(
        Lists.newArrayList(new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, taskConfigsDaily1)));

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));

    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(DAILY).longValue(), 2_505_600_000L);
    assertEquals(pinotTaskConfigs.size(), 1);
    Map<String, String> taskConfigsDaily2 = pinotTaskConfigs.get(0).getConfigs();
    checkPinotTaskConfig(taskConfigsDaily2, segmentName4, "daily", "concat", "1d", null, "1000000");

    // Schedule multiple tasks for both merge levels simultaneously
    String segmentNameMergedDaily2 = "merged_testTable__4_1";
    SegmentZKMetadata metadataMergedDaily2 =
        getSegmentZKMetadata(segmentNameMergedDaily2, 2_505_600_000L, 2_591_999_999L, TimeUnit.MILLISECONDS, null);
    metadataMergedDaily2
        .setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, DAILY));
    String segmentNameMergedDaily3 = "merged_testTable__4_2";
    SegmentZKMetadata metadataMergedDaily3 =
        getSegmentZKMetadata(segmentNameMergedDaily3, 2_592_000_000L, 2_592_010_000L, TimeUnit.MILLISECONDS, null);
    metadataMergedDaily3
        .setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, DAILY));
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(Lists
        .newArrayList(metadata1, metadata2, metadata3, metadata4, metadata5, metadataMergedDaily1, metadataMergedDaily2,
            metadataMergedDaily3));

    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(Collections.singletonList(segmentName4),
            Arrays.asList(segmentNameMergedDaily1, segmentNameMergedDaily2), LineageEntryState.COMPLETED, 11111L));

    String taskName2 = "Task_MergeRollupTask_2";
    taskStatesMap.put(taskName2, TaskState.COMPLETED);
    when(mockClusterInfoProvide.getTaskConfigs(taskName2)).thenReturn(
        Lists.newArrayList(new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, taskConfigsDaily2)));

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));

    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(DAILY).longValue(), 2_592_000_000L);
    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(MONTHLY).longValue(), 0L);
    assertEquals(pinotTaskConfigs.size(), 2);
    Map<String, String> taskConfigsDaily3 = pinotTaskConfigs.get(0).getConfigs();
    Map<String, String> taskConfigsMonthly1 = pinotTaskConfigs.get(1).getConfigs();
    checkPinotTaskConfig(taskConfigsDaily3, segmentNameMergedDaily3 + "," + segmentName5, "daily", "concat", "1d", null,
        "1000000");
    checkPinotTaskConfig(taskConfigsMonthly1, segmentNameMergedDaily1 + "," + segmentNameMergedDaily2, "monthly",
        "rollup", "30d", "30d", "2000000");

    // Not scheduling for daily tasks if there are no unmerged segments
    // Not scheduling for monthly tasks if there are no 30 days merged daily segments
    String segmentNameMergedDaily4 = "merged_testTable__4_2__5";
    SegmentZKMetadata metadataMergedDaily4 =
        getSegmentZKMetadata(segmentNameMergedDaily4, 2_592_000_000L, 2_592_020_000L, TimeUnit.MILLISECONDS, null);
    metadataMergedDaily4
        .setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, DAILY));
    String segmentNameMergedMonthly1 = "merged_testTable__1__2__3__4_1";
    SegmentZKMetadata metadataMergedMonthly1 =
        getSegmentZKMetadata(segmentNameMergedMonthly1, 86_400_000L, 2_591_999_999L, TimeUnit.MILLISECONDS, null);
    metadataMergedMonthly1
        .setCustomMap(ImmutableMap.of(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY, MONTHLY));
    when(mockClusterInfoProvide.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(Lists
        .newArrayList(metadata1, metadata2, metadata3, metadata4, metadata5, metadataMergedDaily1, metadataMergedDaily2,
            metadataMergedDaily3, metadataMergedDaily4, metadataMergedMonthly1));

    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(Arrays.asList(segmentNameMergedDaily3, segmentName5),
            Collections.singletonList(segmentNameMergedDaily4), LineageEntryState.COMPLETED, 11111L));
    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(Arrays.asList(segmentNameMergedDaily1, segmentNameMergedDaily2),
            Collections.singletonList(segmentNameMergedMonthly1), LineageEntryState.COMPLETED, 11111L));

    String taskName3 = "Task_MergeRollupTask_3";
    taskStatesMap.put(taskName3, TaskState.COMPLETED);
    when(mockClusterInfoProvide.getTaskConfigs(taskName3)).thenReturn(
        Lists.newArrayList(new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, taskConfigsDaily3)));
    String taskName4 = "Task_MergeRollupTask_4";
    taskStatesMap.put(taskName4, TaskState.COMPLETED);
    when(mockClusterInfoProvide.getTaskConfigs(taskName4)).thenReturn(
        Lists.newArrayList(new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, taskConfigsMonthly1)));

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));

    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(DAILY).longValue(), 2_592_000_000L); // 30 days since epoch
    assertEquals(MergeRollupTaskMetadata
        .fromZNRecord(mockClusterInfoProvide.getMinionMergeRollupTaskZNRecord(OFFLINE_TABLE_NAME)).getWatermarkMap()
        .get(MONTHLY).longValue(), 0L);
    assertEquals(pinotTaskConfigs.size(), 0);
  }

  private SegmentZKMetadata getSegmentZKMetadata(String segmentName, long startTime, long endTime, TimeUnit timeUnit,
      String downloadURL) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    segmentZKMetadata.setSegmentType(SegmentType.OFFLINE);
    segmentZKMetadata.setStartTime(startTime);
    segmentZKMetadata.setEndTime(endTime);
    segmentZKMetadata.setTimeUnit(timeUnit);
    segmentZKMetadata.setDownloadUrl(downloadURL);
    segmentZKMetadata.setTotalDocs(1000);
    return segmentZKMetadata;
  }
}
