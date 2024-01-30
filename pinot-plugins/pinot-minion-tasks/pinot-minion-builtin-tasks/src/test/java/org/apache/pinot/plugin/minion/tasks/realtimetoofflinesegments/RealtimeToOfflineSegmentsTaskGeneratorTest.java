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
package org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.RealtimeToOfflineSegmentsTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link RealtimeToOfflineSegmentsTaskGenerator}
 */
public class RealtimeToOfflineSegmentsTaskGeneratorTest {

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private final Map<String, String> _streamConfigs = new HashMap<>();

  @BeforeClass
  public void setup() {
    _streamConfigs.put(StreamConfigProperties.STREAM_TYPE, "kafka");
    _streamConfigs
        .put(StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME),
            "myTopic");
    _streamConfigs
        .put(StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_DECODER_CLASS),
            "org.foo.Decoder");
  }

  private TableConfig getRealtimeTableConfig(Map<String, Map<String, String>> taskConfigsMap) {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
        .setStreamConfigs(_streamConfigs).setTaskConfig(new TableTaskConfig(taskConfigsMap)).build();
  }

  /**
   * Tests for some config checks
   */
  @Test
  public void testGenerateTasksCheckConfigs() {
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    SegmentZKMetadata segmentZKMetadata =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 5000, 50_000, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME))
        .thenReturn(getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList(segmentZKMetadata.getSegmentName())));

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);

    // Skip task generation, if offline table
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // No tableTaskConfig, error
    TableConfig realtimeTableConfig = getRealtimeTableConfig(new HashMap<>());
    realtimeTableConfig.setTaskConfig(null);
    try {
      generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
      Assert.fail("Should have failed for null tableTaskConfig");
    } catch (IllegalStateException e) {
      // expected
    }

    // No taskConfig for task, error
    realtimeTableConfig = getRealtimeTableConfig(new HashMap<>());
    try {
      generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
      Assert.fail("Should have failed for null taskConfig");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Tests for some constraints on simultaneous tasks scheduled
   */
  @Test
  public void testGenerateTasksSimultaneousConstraints() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    Map<String, TaskState> taskStatesMap = new HashMap<>();
    String taskName = "Task_RealtimeToOfflineSegmentsTask_" + System.currentTimeMillis();
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.TABLE_NAME_KEY, REALTIME_TABLE_NAME);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(taskStatesMap);
    when(mockClusterInfoProvide.getTaskConfigs(taskName))
        .thenReturn(Lists.newArrayList(new PinotTaskConfig(RealtimeToOfflineSegmentsTask.TASK_TYPE, taskConfigs)));
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 100_000L).toZNRecord());
    SegmentZKMetadata segmentZKMetadata =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 80_000_000, 90_000_000, TimeUnit.MILLISECONDS,
            null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME))
        .thenReturn(getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList(segmentZKMetadata.getSegmentName())));

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);

    // if same task and table, IN_PROGRESS, then don't generate again
    taskStatesMap.put(taskName, TaskState.IN_PROGRESS);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // if same task and table, but COMPLETED, generate
    taskStatesMap.put(taskName, TaskState.COMPLETED);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);

    // if same task and table, IN_PROGRESS, but older than 1 day, generate
    String oldTaskName =
        "Task_RealtimeToOfflineSegmentsTask_" + (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3));
    taskStatesMap.remove(taskName);
    taskStatesMap.put(oldTaskName, TaskState.IN_PROGRESS);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
  }

  /**
   * Tests for realtime table with no segments
   */
  @Test
  public void testGenerateTasksNoSegments() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    // No segments in table
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(Lists.newArrayList());
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME))
        .thenReturn(getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList()));

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // No COMPLETED segments in table
    SegmentZKMetadata segmentZKMetadata1 =
        getSegmentZKMetadata("testTable__0__0__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME))
        .thenReturn(getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList(segmentZKMetadata1.getSegmentName())));

    generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // 2 partitions. No COMPLETED segments for partition 0
    SegmentZKMetadata segmentZKMetadata2 =
        getSegmentZKMetadata("testTable__1__0__12345", Status.DONE, 5000, 10000, TimeUnit.MILLISECONDS, null);
    SegmentZKMetadata segmentZKMetadata3 =
        getSegmentZKMetadata("testTable__1__1__13456", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2, segmentZKMetadata3));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName(),
            segmentZKMetadata3.getSegmentName())));

    generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());
  }

  /**
   * Test cold start. No minion metadata exists. Watermark is calculated based on config or existing segments
   */
  @Test
  public void testGenerateTasksNoMinionMetadata() {
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME)).thenReturn(null);
    SegmentZKMetadata segmentZKMetadata1 =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 1590048000000L, 1590134400000L,
            TimeUnit.MILLISECONDS, "download1"); // 21 May 2020 8am to 22 May 2020 8am UTC
    SegmentZKMetadata segmentZKMetadata2 =
        getSegmentZKMetadata("testTable__1__0__12345", Status.DONE, 1590048000000L, 1590134400000L,
            TimeUnit.MILLISECONDS, "download2"); // 21 May 2020 8am to 22 May 2020 8am UTC
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    // StartTime calculated using segment metadata
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), RealtimeToOfflineSegmentsTask.TASK_TYPE);
    Map<String, String> configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "testTable__0__0__12345,testTable__1__0__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY), "1590019200000"); // 21 May 2020 UTC
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590105600000"); // 22 May 2020 UTC

    // Segment metadata in hoursSinceEpoch
    segmentZKMetadata1 = getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 441680L, 441703L, TimeUnit.HOURS,
        "download1"); // 21 May 2020 8am to 22 May 2020 8am UTC
    segmentZKMetadata2 = getSegmentZKMetadata("testTable__1__0__12345", Status.DONE, 441680L, 441703L, TimeUnit.HOURS,
        "download2"); // 21 May 2020 8am to 22 May 2020 8am UTC
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), RealtimeToOfflineSegmentsTask.TASK_TYPE);
    configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "testTable__0__0__12345,testTable__1__0__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY), "1590019200000"); // 21 May 2020 UTC
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590105600000");  // 22 May 2020 UTC
  }

  /**
   * Tests for subsequent runs after cold start
   */
  @Test
  public void testGenerateTasksWithMinionMetadata() {
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME)).thenReturn(
        new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 1590019200000L).toZNRecord()); // 21 May 2020 UTC
    SegmentZKMetadata segmentZKMetadata1 =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 1589972400000L, 1590048000000L,
            TimeUnit.MILLISECONDS, "download1"); // 05-20-2020T11:00:00 to 05-21-2020T08:00:00 UTC
    SegmentZKMetadata segmentZKMetadata2 =
        getSegmentZKMetadata("testTable__0__1__12345", Status.DONE, 1590048000000L, 1590134400000L,
            TimeUnit.MILLISECONDS, "download2"); // 05-21-2020T08:00:00 UTC to 05-22-2020T08:00:00 UTC
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    // Default configs
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), RealtimeToOfflineSegmentsTask.TASK_TYPE);
    Map<String, String> configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "testTable__0__0__12345,testTable__0__1__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY), "1590019200000"); // 5-21-2020
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590105600000"); // 5-22-2020

    // No segments match
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME)).thenReturn(
        new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 1590490800000L).toZNRecord()); // 26 May 2020 UTC
    generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 0);

    // Some segments match
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME)).thenReturn(
        new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 1590019200000L).toZNRecord()); // 21 May 2020 UTC
    taskConfigsMap = new HashMap<>();
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(RealtimeToOfflineSegmentsTask.BUCKET_TIME_PERIOD_KEY, "2h");
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, taskConfigs);
    realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), RealtimeToOfflineSegmentsTask.TASK_TYPE);
    configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "testTable__0__0__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY),
        "1590019200000"); // 05-21-2020T00:00:00
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590026400000"); // 05-21-2020T02:00:00

    // Segment Processor configs
    taskConfigsMap = new HashMap<>();
    taskConfigs = new HashMap<>();
    taskConfigs.put(RealtimeToOfflineSegmentsTask.ROUND_BUCKET_TIME_PERIOD_KEY, "1h");
    taskConfigs.put(RealtimeToOfflineSegmentsTask.MERGE_TYPE_KEY, "rollup");
    taskConfigs.put("m1" + RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX, "MAX");
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, taskConfigs);
    realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);
    generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), RealtimeToOfflineSegmentsTask.TASK_TYPE);
    configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY), "testTable__0__0__12345,testTable__0__1__12345");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY),
        "1590019200000"); // 05-21-2020T00:00:00
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590105600000"); // 05-22-2020T00:00:00
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.ROUND_BUCKET_TIME_PERIOD_KEY), "1h");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.MERGE_TYPE_KEY), "rollup");
    assertEquals(configs.get("m1" + RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX), "MAX");
  }

  /**
   * Tests for skipping task generation due to CONSUMING segments overlap with window
   */
  @Test
  public void testOverflowIntoConsuming() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());

    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 100_000L).toZNRecord());
    SegmentZKMetadata segmentZKMetadata1 =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 50_000, 150_000, TimeUnit.MILLISECONDS, null);
    SegmentZKMetadata segmentZKMetadata2 =
        getSegmentZKMetadata("testTable__0__1__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);

    // last COMPLETED segment's endTime is less than windowEnd time. CONSUMING segment overlap. Skip task
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    segmentZKMetadata1 =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 100_000, 200_000, TimeUnit.MILLISECONDS, null);
    segmentZKMetadata2 =
        getSegmentZKMetadata("testTable__0__1__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // last completed segment endtime ends at window end, allow
    segmentZKMetadata1 =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 200_000, 86_500_000, TimeUnit.MILLISECONDS, null);
    segmentZKMetadata2 =
        getSegmentZKMetadata("testTable__0__1__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
  }

  /**
   * Tests for task generation when there is time gap between segments.
   */
  @Test
  public void testTimeGap() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    when(mockClusterInfoProvide.getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE,
        REALTIME_TABLE_NAME)).thenReturn(
        new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 1590019200000L).toZNRecord()); // 21 May 2020 UTC
    SegmentZKMetadata segmentZKMetadata =
        getSegmentZKMetadata("testTable__0__1__12345", Status.DONE, 1590220800000L, 1590307200000L,
            TimeUnit.MILLISECONDS, "download2"); // 05-23-2020T08:00:00 UTC to 05-24-2020T08:00:00 UTC
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(
        Collections.singletonList(segmentZKMetadata));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata.getSegmentName())));

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);

    // Generated task should skip 2 days and have time window of [23 May 2020 UTC, 24 May 2020 UTC)
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    Map<String, String> configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY), "1590192000000");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590278400000");
  }

  @Test
  public void testBuffer() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    // default buffer - 2d
    long now = System.currentTimeMillis();
    long watermarkMs = now - TimeUnit.DAYS.toMillis(1);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, watermarkMs).toZNRecord());
    SegmentZKMetadata segmentZKMetadata =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, watermarkMs - 100, watermarkMs + 100,
            TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata.getSegmentName())));

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);

    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // custom buffer
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(RealtimeToOfflineSegmentsTask.BUFFER_TIME_PERIOD_KEY, "15d");
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, taskConfigs);
    realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    watermarkMs = now - TimeUnit.DAYS.toMillis(10);
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, watermarkMs).toZNRecord());
    segmentZKMetadata =
        getSegmentZKMetadata("testTable__0__0__12345", Status.DONE, watermarkMs - 100, watermarkMs + 100,
            TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata.getSegmentName())));

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());
  }

  private SegmentZKMetadata getSegmentZKMetadata(String segmentName, Status status, long startTime, long endTime,
      TimeUnit timeUnit, String downloadURL) {
    SegmentZKMetadata realtimeSegmentZKMetadata = new SegmentZKMetadata(segmentName);
    realtimeSegmentZKMetadata.setStatus(status);
    realtimeSegmentZKMetadata.setStartTime(startTime);
    realtimeSegmentZKMetadata.setEndTime(endTime);
    realtimeSegmentZKMetadata.setTimeUnit(timeUnit);
    realtimeSegmentZKMetadata.setDownloadUrl(downloadURL);
    return realtimeSegmentZKMetadata;
  }

  private IdealState getIdealState(String tableName, List<String> segmentNames) {
    IdealState idealState = new IdealState(tableName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    for (String segmentName: segmentNames) {
      idealState.setPartitionState(segmentName, "Server_0", "ONLINE");
    }
    return idealState;
  }
}
