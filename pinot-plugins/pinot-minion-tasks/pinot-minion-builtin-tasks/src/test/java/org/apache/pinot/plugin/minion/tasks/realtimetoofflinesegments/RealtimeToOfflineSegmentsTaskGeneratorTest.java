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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.RealtimeToOfflineCheckpointCheckPoint;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.RealtimeToOfflineSegmentsTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
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
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
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

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(new ArrayList<>());

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

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

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(new ArrayList<>());

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

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

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(new ArrayList<>());

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

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

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(new ArrayList<>());

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

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

  @Test
  public void testGenerateNewSegmentsToProcess() {
    List<SegmentZKMetadata> completedSegmentsZKMetadata = new ArrayList<>();

    long hourMillis = 3600 * 1000;
    long pastTime = System.currentTimeMillis() - (2 * 24 * hourMillis);

    ZNRecord znRecord1 = new ZNRecord("seg_1");
    znRecord1.setSimpleField(CommonConstants.Segment.START_TIME, String.valueOf(pastTime + hourMillis));
    znRecord1.setSimpleField(CommonConstants.Segment.END_TIME, String.valueOf(pastTime + 2 * hourMillis));

    ZNRecord znRecord2 = new ZNRecord("seg_2");
    znRecord2.setSimpleField(CommonConstants.Segment.START_TIME, String.valueOf(pastTime + hourMillis + 1));
    znRecord2.setSimpleField(CommonConstants.Segment.END_TIME, String.valueOf(pastTime + 2 * hourMillis - 90));

    ZNRecord znRecord3 = new ZNRecord("seg_3");
    znRecord3.setSimpleField(CommonConstants.Segment.START_TIME, String.valueOf(pastTime + 6 * hourMillis + 1));
    znRecord3.setSimpleField(CommonConstants.Segment.END_TIME, String.valueOf(pastTime + 8 * hourMillis));

    ZNRecord znRecord4 = new ZNRecord("seg_4");
    znRecord4.setSimpleField(CommonConstants.Segment.START_TIME, String.valueOf(pastTime + 6 * hourMillis + 90));
    znRecord4.setSimpleField(CommonConstants.Segment.END_TIME, String.valueOf(pastTime + 8 * hourMillis + 12));

    List<ZNRecord> znRecordList = ImmutableList.of(znRecord1, znRecord2, znRecord3, znRecord4);
    for (ZNRecord znRecord : znRecordList) {
      znRecord.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.MILLISECONDS.toString());
      completedSegmentsZKMetadata.add(new SegmentZKMetadata(znRecord));
    }

    Set<String> lastLLCSegmentPerPartition = new HashSet<>();
    lastLLCSegmentPerPartition.add("seg_4");

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        new RealtimeToOfflineSegmentsTaskMetadata("test_REALTIME", 1);

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    List<SegmentZKMetadata> segmentZKMetadataList =
        generator.generateNewSegmentsToProcess(completedSegmentsZKMetadata, pastTime, pastTime + hourMillis, hourMillis,
            (24 * hourMillis), "1d", lastLLCSegmentPerPartition,
            realtimeToOfflineSegmentsTaskMetadata);

    assert segmentZKMetadataList.size() == 2;
    assert "seg_1".equals(segmentZKMetadataList.get(0).getSegmentName());
    assert "seg_2".equals(segmentZKMetadataList.get(1).getSegmentName());
    assert (pastTime + hourMillis) == realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs();
    assert (pastTime + 2 * hourMillis) == realtimeToOfflineSegmentsTaskMetadata.getWindowEndMs();
  }

  @Test
  public void testGenerateTasksWithSegmentUploadFailure() {
    // store partial offline segments in Zk metadata.
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoProvide.getTaskStates(RealtimeToOfflineSegmentsTask.TASK_TYPE)).thenReturn(new HashMap<>());
    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        getRealtimeToOfflineSegmentsTaskMetadata();
    when(mockClusterInfoProvide
        .getMinionTaskMetadataZNRecord(RealtimeToOfflineSegmentsTask.TASK_TYPE, REALTIME_TABLE_NAME)).thenReturn(
        realtimeToOfflineSegmentsTaskMetadata.toZNRecord()); // 21 May 2020 UTC
    SegmentZKMetadata segmentZKMetadata1 =
        getSegmentZKMetadata("githubEvents__0__0__20241213T2002Z", Status.DONE, 1589972400000L, 1590048000000L,
            TimeUnit.MILLISECONDS, "download1"); // 05-20-2020T11:00:00 to 05-21-2020T08:00:00 UTC
    SegmentZKMetadata segmentZKMetadata2 =
        getSegmentZKMetadata("githubEvents__0__0__20241213T2003Z", Status.DONE, 1590048000000L, 1590134400000L,
            TimeUnit.MILLISECONDS, "download2"); // 05-21-2020T08:00:00 UTC to 05-22-2020T08:00:00 UTC
    when(mockClusterInfoProvide.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(segmentZKMetadata1, segmentZKMetadata2));
    when(mockClusterInfoProvide.getIdealState(REALTIME_TABLE_NAME)).thenReturn(getIdealState(REALTIME_TABLE_NAME,
        Lists.newArrayList(segmentZKMetadata1.getSegmentName(), segmentZKMetadata2.getSegmentName())));

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(
        List.of("githubEventsOffline__0__0__20241213T2002Z"));

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);
    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    when(mockPinotHelixResourceManager.deleteSegments(Mockito.eq(OFFLINE_TABLE_NAME), captor.capture())).thenReturn(
        PinotResourceManagerResponse.success(""));

    // Default configs
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    taskConfigsMap.put(RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    TableConfig realtimeTableConfig = getRealtimeTableConfig(taskConfigsMap);

    RealtimeToOfflineSegmentsTaskGenerator generator = new RealtimeToOfflineSegmentsTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(realtimeTableConfig));

    List<String> capturedList = captor.getValue();
    assert capturedList.size() == 1;
    assert capturedList.get(0).equals("githubEventsOffline__0__0__20241213T2002Z");

    assertEquals(pinotTaskConfigs.size(), 1);
    assertEquals(pinotTaskConfigs.get(0).getTaskType(), RealtimeToOfflineSegmentsTask.TASK_TYPE);
    Map<String, String> configs = pinotTaskConfigs.get(0).getConfigs();
    assertEquals(configs.get(MinionConstants.TABLE_NAME_KEY), REALTIME_TABLE_NAME);
    assertEquals(configs.get(MinionConstants.SEGMENT_NAME_KEY),
        "githubEvents__0__0__20241213T2002Z,githubEvents__0__0__20241213T2003Z");
    assertEquals(configs.get(MinionConstants.DOWNLOAD_URL_KEY), "download1,download2");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY), "1589972400000");
    assertEquals(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY), "1590058800000");
  }

  private RealtimeToOfflineSegmentsTaskMetadata getRealtimeToOfflineSegmentsTaskMetadata() {
    List<RealtimeToOfflineCheckpointCheckPoint> checkPoints = new ArrayList<>();
    RealtimeToOfflineCheckpointCheckPoint checkPoint =
        new RealtimeToOfflineCheckpointCheckPoint(
            new HashSet<>(Arrays.asList("githubEvents__0__0__20241213T2002Z", "githubEvents__0__0__20241213T2003Z")),
            new HashSet<>(Arrays.asList("githubEventsOffline__0__0__20241213T2002Z",
                "githubEventsOffline__0__0__20241213T2003Z")),
            "1");

    checkPoints.add(checkPoint);
    return new RealtimeToOfflineSegmentsTaskMetadata("testTable_REALTIME", 1589972400000L, 1590058800000L,
        checkPoints);
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

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(new ArrayList<>());

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

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

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(new ArrayList<>());

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

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

    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockPinotHelixResourceManager.getSegmentsFor(OFFLINE_TABLE_NAME, true)).thenReturn(new ArrayList<>());

    when(mockClusterInfoProvide.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

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

  @Test
  public void testRealtimeToOfflineSegmentsTaskConfig() {
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockClusterInfoAccessor.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();

    when(mockPinotHelixResourceManager.getSchemaForTableConfig(any())).thenReturn(schema);

    RealtimeToOfflineSegmentsTaskGenerator taskGenerator = new RealtimeToOfflineSegmentsTaskGenerator();
    taskGenerator.init(mockClusterInfoAccessor);

    Map<String, String> realtimeToOfflineTaskConfig =
        ImmutableMap.of("schedule", "0 */10 * ? * * *", "bucketTimePeriod", "6h", "bufferTimePeriod", "5d", "mergeType",
            "rollup", "myCol.aggregationType", "max");

    Map<String, String> segmentGenerationAndPushTaskConfig = ImmutableMap.of("schedule", "0 */10 * ? * * *");

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of("RealtimeToOfflineSegmentsTask", realtimeToOfflineTaskConfig,
            "SegmentGenerationAndPushTask", segmentGenerationAndPushTaskConfig))).build();

    // validate valid config
    taskGenerator.validateTaskConfigs(tableConfig, schema, realtimeToOfflineTaskConfig);

    // invalid Upsert config with RealtimeToOfflineTask
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).setTaskConfig(new TableTaskConfig(
                ImmutableMap.of("RealtimeToOfflineSegmentsTask", realtimeToOfflineTaskConfig,
                    "SegmentGenerationAndPushTask", segmentGenerationAndPushTaskConfig))).build();
    try {
      taskGenerator.validateTaskConfigs(tableConfig, schema, realtimeToOfflineTaskConfig);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("RealtimeToOfflineTask doesn't support upsert table"));
    }

    // invalid period
    HashMap<String, String> invalidPeriodConfig = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidPeriodConfig.put("roundBucketTimePeriod", "garbage");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidPeriodConfig, "SegmentGenerationAndPushTask",
                segmentGenerationAndPushTaskConfig))).build();
    try {
      taskGenerator.validateTaskConfigs(tableConfig, schema, invalidPeriodConfig);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid time spec"));
    }

    // invalid mergeType
    HashMap<String, String> invalidMergeType = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidMergeType.put("mergeType", "garbage");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidMergeType, "SegmentGenerationAndPushTask",
                segmentGenerationAndPushTaskConfig))).build();
    try {
      taskGenerator.validateTaskConfigs(tableConfig, schema, invalidMergeType);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("MergeType must be one of"));
    }

    // invalid column
    HashMap<String, String> invalidColumnConfig = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidColumnConfig.put("score.aggregationType", "max");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidColumnConfig, "SegmentGenerationAndPushTask",
                segmentGenerationAndPushTaskConfig))).build();
    try {
      taskGenerator.validateTaskConfigs(tableConfig, schema, invalidColumnConfig);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("not found in schema"));
    }

    // invalid agg
    HashMap<String, String> invalidAggConfig = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidAggConfig.put("myCol.aggregationType", "garbage");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidAggConfig, "SegmentGenerationAndPushTask",
                segmentGenerationAndPushTaskConfig))).build();
    try {
      taskGenerator.validateTaskConfigs(tableConfig, schema, invalidAggConfig);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("has invalid aggregate type"));
    }

    // aggregation function that exists but has no ValueAggregator available
    HashMap<String, String> invalidAgg2Config = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidAgg2Config.put("myCol.aggregationType", "Histogram");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidAgg2Config, "SegmentGenerationAndPushTask",
                segmentGenerationAndPushTaskConfig))).build();
    try {
      taskGenerator.validateTaskConfigs(tableConfig, schema, invalidAgg2Config);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("has invalid aggregate type"));
    }

    // valid agg
    HashMap<String, String> validAggConfig = new HashMap<>(realtimeToOfflineTaskConfig);
    validAggConfig.put("myCol.aggregationType", "distinctCountHLL");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", validAggConfig, "SegmentGenerationAndPushTask",
                segmentGenerationAndPushTaskConfig))).build();
    taskGenerator.validateTaskConfigs(tableConfig, schema, validAggConfig);

    // valid agg
    HashMap<String, String> validAgg2Config = new HashMap<>(realtimeToOfflineTaskConfig);
    validAgg2Config.put("myCol.aggregationType", "distinctCountHLLPlus");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", validAgg2Config, "SegmentGenerationAndPushTask",
                segmentGenerationAndPushTaskConfig))).build();
    taskGenerator.validateTaskConfigs(tableConfig, schema, validAgg2Config);
  }

  @Test
  public void testDivideSegmentsAmongSubtasks() {
    RealtimeToOfflineSegmentsTaskGenerator taskGenerator = new RealtimeToOfflineSegmentsTaskGenerator();

    ZNRecord znRecord1 = new ZNRecord("seg_1");
    znRecord1.setSimpleField(CommonConstants.Segment.TOTAL_DOCS, "70");
    znRecord1.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "seg_1.tar");

    ZNRecord znRecord2 = new ZNRecord("seg_2");
    znRecord2.setSimpleField(CommonConstants.Segment.TOTAL_DOCS, "30");
    znRecord2.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "seg_2.tar");

    ZNRecord znRecord3 = new ZNRecord("seg_3");
    znRecord3.setSimpleField(CommonConstants.Segment.TOTAL_DOCS, "101");
    znRecord3.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "seg_3.tar");

    ZNRecord znRecord4 = new ZNRecord("seg_4");
    znRecord4.setSimpleField(CommonConstants.Segment.TOTAL_DOCS, "1");
    znRecord4.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "seg_4.tar");

    ZNRecord znRecord5 = new ZNRecord("seg_5");
    znRecord5.setSimpleField(CommonConstants.Segment.TOTAL_DOCS, "98");
    znRecord5.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "seg_5.tar");

    ZNRecord znRecord6 = new ZNRecord("seg_6");
    znRecord6.setSimpleField(CommonConstants.Segment.TOTAL_DOCS, "123");
    znRecord6.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "seg_6.tar");

    ZNRecord znRecord7 = new ZNRecord("seg_7");
    znRecord7.setSimpleField(CommonConstants.Segment.TOTAL_DOCS, "1");
    znRecord7.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "seg_7.tar");

    List<ZNRecord> znRecordList =
        ImmutableList.of(znRecord1, znRecord2, znRecord3, znRecord4, znRecord5, znRecord6, znRecord7);

    List<SegmentZKMetadata> segmentsToBeScheduled = new ArrayList<>();
    List<List<String>> segmentNamesGroupList = new ArrayList<>();
    Map<String, String> segmentNameVsDownloadURL = new HashMap<>();
    int maxNumRecordsPerSubTask = 100;

    for (ZNRecord znRecord: znRecordList) {
      segmentsToBeScheduled.add(new SegmentZKMetadata(znRecord));
    }

    taskGenerator.divideSegmentsAmongSubtasks(segmentsToBeScheduled, segmentNamesGroupList, segmentNameVsDownloadURL,
        maxNumRecordsPerSubTask);

    assert segmentNamesGroupList.size() == 4;
    assert "seg_1,seg_2".equals(String.join(",", segmentNamesGroupList.get(0)));
    assert "seg_3".equals(String.join(",", segmentNamesGroupList.get(1)));
    assert "seg_4,seg_5,seg_6".equals(String.join(",", segmentNamesGroupList.get(2)));
    assert "seg_7".equals(String.join(",", segmentNamesGroupList.get(3)));

    assert segmentNameVsDownloadURL.size() == 7;
    for (String segmentName: segmentNameVsDownloadURL.keySet()) {
      assert (segmentName + ".tar").equals(segmentNameVsDownloadURL.get(segmentName));
    }
  }

  @Test
  public void testGetFailedCheckpoints() {
    RealtimeToOfflineSegmentsTaskGenerator taskGenerator = new RealtimeToOfflineSegmentsTaskGenerator();
    Set<String> segmentsPresentInOfflineTable =
        new HashSet<>(Arrays.asList("seg_1", "seg_2", "seg_3", "seg_4", "seg_5", "seg_6", "seg_7"));

    List<RealtimeToOfflineCheckpointCheckPoint> checkPoints = new ArrayList<>();
    checkPoints.add(new RealtimeToOfflineCheckpointCheckPoint(
        new HashSet<>(Arrays.asList("seg_realtime_1", "seg_realtime_2")),
        new HashSet<>(Arrays.asList("seg_1", "seg_4", "seg_5")),
        "1", "task_1", true)
    );
    checkPoints.add(new RealtimeToOfflineCheckpointCheckPoint(
        new HashSet<>(Arrays.asList("seg_realtime_3", "seg_realtime_4")),
        new HashSet<>(Arrays.asList("seg_2")),
        "2")
    );
    RealtimeToOfflineCheckpointCheckPoint checkPoint = new RealtimeToOfflineCheckpointCheckPoint(
        new HashSet<>(Arrays.asList("seg_realtime_5", "seg_realtime_6")),
        new HashSet<>(Arrays.asList("seg_6", "seg_8")),
        "2");
    checkPoints.add(checkPoint);

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        new RealtimeToOfflineSegmentsTaskMetadata("test_REALTIME", System.currentTimeMillis() - 100000,
            System.currentTimeMillis() - 1000, checkPoints);
    List<RealtimeToOfflineCheckpointCheckPoint> failedCheckpoints =
        taskGenerator.getFailedCheckpoints(realtimeToOfflineSegmentsTaskMetadata, segmentsPresentInOfflineTable);

    assert failedCheckpoints.size() == 1;
    assert !failedCheckpoints.get(0).isFailed();
    assert failedCheckpoints.get(0).getId().equals(checkPoint.getId());
  }

  @Test
  public void testFilterOutDeletedSegments() {
    RealtimeToOfflineSegmentsTaskGenerator taskGenerator = new RealtimeToOfflineSegmentsTaskGenerator();
    Set<String> segmentNames = new HashSet<>(Arrays.asList("seg_1", "seg_2", "seg_3", "seg_4"));
    List<SegmentZKMetadata> currentTableSegments =
        Arrays.asList(new SegmentZKMetadata("seg_1"), new SegmentZKMetadata("seg_3"), new SegmentZKMetadata("seg_4"));
    List<SegmentZKMetadata> segmentZKMetadataList =
        taskGenerator.filterOutDeletedSegments(segmentNames, currentTableSegments);
    assert segmentZKMetadataList.size() == 3;
    StringBuilder liveSegmentNames = new StringBuilder();
    for (SegmentZKMetadata segmentZKMetadata: segmentZKMetadataList) {
      liveSegmentNames.append(segmentZKMetadata.getSegmentName()).append(",");
    }
    assert "seg_1,seg_3,seg_4,".contentEquals(liveSegmentNames);
  }

  @Test
  public void testDeleteInvalidOfflineSegments() {
    Set<String> existingOfflineSegmentNames = new HashSet<>(Arrays.asList("seg_1", "seg_2", "seg_3", "seg_4"));

    List<RealtimeToOfflineCheckpointCheckPoint> checkPoints = new ArrayList<>();
    checkPoints.add(new RealtimeToOfflineCheckpointCheckPoint(
        new HashSet<>(Arrays.asList("seg_realtime_1", "seg_realtime_2")),
        new HashSet<>(Arrays.asList("seg_1", "seg_4", "seg_5")),
        "1")
    );
    checkPoints.add(new RealtimeToOfflineCheckpointCheckPoint(
        new HashSet<>(Arrays.asList("seg_realtime_3", "seg_realtime_4")),
        new HashSet<>(Arrays.asList("seg_2")),
        "1")
    );

    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(mockClusterInfoAccessor.getPinotHelixResourceManager()).thenReturn(mockPinotHelixResourceManager);
    RealtimeToOfflineSegmentsTaskGenerator taskGenerator = new RealtimeToOfflineSegmentsTaskGenerator();
    taskGenerator.init(mockClusterInfoAccessor);
    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    when(mockPinotHelixResourceManager.deleteSegments(Mockito.eq("test_OFFLINE"), captor.capture())).thenReturn(
        PinotResourceManagerResponse.success(""));

    taskGenerator.deleteInvalidOfflineSegments("test_OFFLINE", existingOfflineSegmentNames, checkPoints);
    List<String> capturedList = captor.getValue();

    assert checkPoints.get(0).isFailed();
    assert checkPoints.get(1).isFailed();

    StringBuilder segmentNames = new StringBuilder();
    for (String segmentName: capturedList) {
      segmentNames.append(segmentName).append(",");
    }
    assert "seg_1,seg_4,seg_2,".contentEquals(segmentNames);
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
    for (String segmentName : segmentNames) {
      idealState.setPartitionState(segmentName, "Server_0", "ONLINE");
    }
    return idealState;
  }
}
