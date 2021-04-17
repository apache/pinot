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
package org.apache.pinot.plugin.minion.tasks.realtime_to_offline_segments;

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.RealtimeToOfflineSegmentsTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamConfig;
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
  private final Map<String, String> streamConfigs = new HashMap<>();

  @BeforeClass
  public void setup() {
    streamConfigs.put(StreamConfigProperties.STREAM_TYPE, "kafka");
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_CONSUMER_TYPES),
        StreamConfig.ConsumerType.LOWLEVEL.toString());
    streamConfigs.put(StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME),
        "myTopic");
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.foo.Decoder");
  }

  private TableConfig getRealtimeTableConfig(Map<String, Map<String, String>> taskConfigsMap) {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
        .setStreamConfigs(streamConfigs).setTaskConfig(new TableTaskConfig(taskConfigsMap)).build();
  }

  /**
   * Tests for some config checks
   */
  @Test
  public void testGenerateTasksCheckConfigs() {
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    LLCRealtimeSegmentZKMetadata metadata1 =
        getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 5000, 50_000, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1));

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
    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 100_000L));
    LLCRealtimeSegmentZKMetadata metadata1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE,
        80_000_000, 90_000_000, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1));

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
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME)).thenReturn(Lists.newArrayList());

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // No COMPLETED segments in table
    LLCRealtimeSegmentZKMetadata seg1 =
        getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(seg1));

    generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // 2 partitions. No COMPLETED segments for partition 0
    LLCRealtimeSegmentZKMetadata seg2 =
        getRealtimeSegmentZKMetadata("testTable__1__0__12345", Status.DONE, 5000, 10000, TimeUnit.MILLISECONDS, null);
    LLCRealtimeSegmentZKMetadata seg3 =
        getRealtimeSegmentZKMetadata("testTable__1__1__13456", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(seg1, seg2, seg3));

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
    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME)).thenReturn(null);
    LLCRealtimeSegmentZKMetadata seg1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE,
        1590048000000L, 1590134400000L, TimeUnit.MILLISECONDS, "download1"); // 21 May 2020 8am to 22 May 2020 8am UTC
    LLCRealtimeSegmentZKMetadata seg2 = getRealtimeSegmentZKMetadata("testTable__1__0__12345", Status.DONE,
        1590048000000L, 1590134400000L, TimeUnit.MILLISECONDS, "download2"); // 21 May 2020 8am to 22 May 2020 8am UTC
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(seg1, seg2));

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
    seg1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 441680L, 441703L, TimeUnit.HOURS,
        "download1"); // 21 May 2020 8am to 22 May 2020 8am UTC
    seg2 = getRealtimeSegmentZKMetadata("testTable__1__0__12345", Status.DONE, 441680L, 441703L, TimeUnit.HOURS,
        "download2"); // 21 May 2020 8am to 22 May 2020 8am UTC
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(seg1, seg2));
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
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590105600000"); // 22 May 2020 UTC
  }

  /**
   * Tests for subsequent runs after cold start
   */
  @Test
  public void testGenerateTasksWithMinionMetadata() {
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 1590019200000L)); // 21 May 2020 UTC
    LLCRealtimeSegmentZKMetadata seg1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE,
        1589972400000L, 1590048000000L, TimeUnit.MILLISECONDS, "download1"); // 05-20-2020T11:00:00 to 05-21-2020T08:00:00 UTC
    LLCRealtimeSegmentZKMetadata seg2 = getRealtimeSegmentZKMetadata("testTable__0__1__12345", Status.DONE,
        1590048000000L, 1590134400000L, TimeUnit.MILLISECONDS, "download2"); // 05-21-2020T08:00:00 UTC to 05-22-2020T08:00:00 UTC
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(seg1, seg2));

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
    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 1590490800000L)); // 26 May 2020 UTC
    generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 0);

    // Some segments match
    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 1590019200000L)); // 21 May 2020 UTC
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
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY), "1590019200000"); // 05-21-2020T00:00:00
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590026400000"); // 05-21-2020T02:00:00

    // Segment Processor configs
    taskConfigsMap = new HashMap<>();
    taskConfigs = new HashMap<>();
    taskConfigs.put(RealtimeToOfflineSegmentsTask.TIME_COLUMN_TRANSFORM_FUNCTION_KEY, "foo");
    taskConfigs.put(RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, "rollup");
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
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY), "1590019200000"); // 05-21-2020T00:00:00
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590105600000"); // 05-22-2020T00:00:00
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.TIME_COLUMN_TRANSFORM_FUNCTION_KEY), "foo");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY), "rollup");
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

    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, 100_000L));
    LLCRealtimeSegmentZKMetadata metadata1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 50_000,
        150_000, TimeUnit.MILLISECONDS, null);
    LLCRealtimeSegmentZKMetadata metadata2 =
        getRealtimeSegmentZKMetadata("testTable__0__1__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2));

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);

    // last COMPLETED segment's endTime is less than windowEnd time. CONSUMING segment overlap. Skip task
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    metadata1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 100_000, 200_000,
        TimeUnit.MILLISECONDS, null);
    metadata2 =
        getRealtimeSegmentZKMetadata("testTable__0__1__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // last completed segment endtime ends at window end, allow
    metadata1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE, 200_000, 86_500_000,
        TimeUnit.MILLISECONDS, null);
    metadata2 =
        getRealtimeSegmentZKMetadata("testTable__0__1__12345", Status.IN_PROGRESS, -1, -1, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1, metadata2));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
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
    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, watermarkMs));
    LLCRealtimeSegmentZKMetadata metadata1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE,
        watermarkMs - 100, watermarkMs + 100, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1));

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
    when(mockClusterInfoProvide.getMinionRealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME))
        .thenReturn(new RealtimeToOfflineSegmentsTaskMetadata(REALTIME_TABLE_NAME, watermarkMs));
    metadata1 = getRealtimeSegmentZKMetadata("testTable__0__0__12345", Status.DONE, watermarkMs - 100,
        watermarkMs + 100, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getLLCRealtimeSegmentsMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(metadata1));

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());
  }

  private LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String segmentName, Status status, long startTime,
      long endTime, TimeUnit timeUnit, String downloadURL) {
    LLCRealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new LLCRealtimeSegmentZKMetadata();
    realtimeSegmentZKMetadata.setSegmentName(segmentName);
    realtimeSegmentZKMetadata.setStatus(status);
    realtimeSegmentZKMetadata.setStartTime(startTime);
    realtimeSegmentZKMetadata.setEndTime(endTime);
    realtimeSegmentZKMetadata.setTimeUnit(timeUnit);
    realtimeSegmentZKMetadata.setDownloadUrl(downloadURL);
    return realtimeSegmentZKMetadata;
  }
}
