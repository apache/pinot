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
package org.apache.pinot.plugin.minion.tasks.upsertcompactmerge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.upsertcompactmerge.UpsertCompactMergeTaskGenerator.SegmentMergerMetadata;
import org.apache.pinot.plugin.minion.tasks.upsertcompactmerge.UpsertCompactMergeTaskGenerator.SegmentSelectionResult;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class UpsertCompactMergeTaskGeneratorTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private UpsertCompactMergeTaskGenerator _taskGenerator;
  private SegmentZKMetadata _completedSegment;
  private SegmentZKMetadata _completedSegment2;
  private Map<String, SegmentZKMetadata> _completedSegmentsMap;

  @Mock
  private ClusterInfoAccessor _clusterInfoAccessor;

  @Mock
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @Mock
  private ServerSegmentMetadataReader _serverSegmentMetadataReader;

  @Mock
  private java.util.concurrent.Executor _executor;

  @BeforeClass
  public void setUpClass() {
    MockitoAnnotations.openMocks(this);
  }

  @BeforeMethod
  public void setUp() {
    _taskGenerator = new UpsertCompactMergeTaskGenerator();
    _taskGenerator.init(_clusterInfoAccessor);

    _completedSegment = new SegmentZKMetadata("testTable__0__0__12345");
    _completedSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("12d"));
    _completedSegment.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("11d"));
    _completedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment.setTotalDocs(100L);
    _completedSegment.setCrc(1000);
    _completedSegment.setCreationTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("12d"));
    _completedSegment.setDownloadUrl("fs://testTable__0__0__12345");

    _completedSegment2 = new SegmentZKMetadata("testTable__0__1__12346");
    _completedSegment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment2.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("10d"));
    _completedSegment2.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("9d"));
    _completedSegment2.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment2.setTotalDocs(10L);
    _completedSegment2.setCreationTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("9d"));
    _completedSegment2.setCrc(2000);
    _completedSegment2.setDownloadUrl("fs://testTable__0__1__12346");

    _completedSegmentsMap = new HashMap<>();
    _completedSegmentsMap.put(_completedSegment.getSegmentName(), _completedSegment);
    _completedSegmentsMap.put(_completedSegment2.getSegmentName(), _completedSegment2);
  }

  @AfterMethod
  public void tearDown() {
    Mockito.reset(_clusterInfoAccessor, _pinotHelixResourceManager, _serverSegmentMetadataReader);
  }

  @Test
  public void testUpsertCompactMergeTaskConfig() {
    // check with OFFLINE table
    Map<String, String> upsertCompactMergeTaskConfig = Map.of("bufferTimePeriod", "5d");
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setTaskConfig(
            new TableTaskConfig(Map.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, upsertCompactMergeTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(offlineTableConfig, new Schema(), upsertCompactMergeTaskConfig));

    // check with non-upsert REALTIME table
    TableConfig nonUpsertRealtimetableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTaskConfig(
            new TableTaskConfig(Map.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, upsertCompactMergeTaskConfig)))
        .build();

    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(nonUpsertRealtimetableConfig, new Schema(),
            upsertCompactMergeTaskConfig));

    // check with snapshot disabled
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setSnapshot(Enablement.DISABLE);
    TableConfig disabledSnapshotTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setTaskConfig(
            new TableTaskConfig(Map.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, upsertCompactMergeTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(disabledSnapshotTableConfig, new Schema(),
            upsertCompactMergeTaskConfig));

    // valid table configs
    upsertConfig.setSnapshot(Enablement.ENABLE);
    TableConfig validTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setTaskConfig(
            new TableTaskConfig(Map.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, upsertCompactMergeTaskConfig)))
        .build();
    _taskGenerator.validateTaskConfigs(validTableConfig, new Schema(), upsertCompactMergeTaskConfig);

    // invalid buffer time period
    Map<String, String> upsertCompactMergeTaskConfig1 = Map.of("bufferTimePeriod", "5hd");
    Assert.assertThrows(IllegalArgumentException.class,
        () -> _taskGenerator.validateTaskConfigs(validTableConfig, new Schema(), upsertCompactMergeTaskConfig1));
  }

  @Test
  public void testGetAlreadyMergedSegments() {
    SegmentZKMetadata mergedSegment = new SegmentZKMetadata("testTable__merged");
    mergedSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    Map<String, String> customMap = new HashMap<>();
    customMap.put(MinionConstants.UpsertCompactMergeTask.TASK_TYPE
        + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX, "testTable__0,testTable__1");
    mergedSegment.setCustomMap(customMap);

    // merged segment present
    List<SegmentZKMetadata> allSegments = Arrays.asList(_completedSegment, _completedSegment2, mergedSegment);
    Set<String> alreadyMergedSegments = UpsertCompactMergeTaskGenerator.getAlreadyMergedSegments(allSegments);
    Assert.assertEquals(alreadyMergedSegments.size(), 2);
    Assert.assertTrue(alreadyMergedSegments.contains("testTable__0"));
    Assert.assertTrue(alreadyMergedSegments.contains("testTable__1"));

    // no merging happened till now
    List<SegmentZKMetadata> segments = Arrays.asList(_completedSegment, _completedSegment2);
    alreadyMergedSegments = UpsertCompactMergeTaskGenerator.getAlreadyMergedSegments(segments);
    Assert.assertTrue(alreadyMergedSegments.isEmpty());

    // no segment present, empty list
    alreadyMergedSegments = UpsertCompactMergeTaskGenerator.getAlreadyMergedSegments(Collections.emptyList());
    Assert.assertTrue(alreadyMergedSegments.isEmpty());
  }

  @Test
  public void testGetCandidateSegments() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.BUFFER_TIME_PERIOD_KEY, "5d");

    // candidates are valid, outside buffer period and download urls
    List<SegmentZKMetadata> candidateSegments = UpsertCompactMergeTaskGenerator.getCandidateSegments(taskConfigs,
        new ArrayList<>(_completedSegmentsMap.values()), System.currentTimeMillis());
    Assert.assertEquals(candidateSegments.size(), 2);
    Assert.assertTrue(candidateSegments.contains(_completedSegment));
    Assert.assertTrue(candidateSegments.contains(_completedSegment2));

    // candidate have empty download url
    SegmentZKMetadata segmentWithNoDownloadUrl = new SegmentZKMetadata("testTable__2");
    segmentWithNoDownloadUrl.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentWithNoDownloadUrl.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("10d"));
    segmentWithNoDownloadUrl.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("9d"));
    segmentWithNoDownloadUrl.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentWithNoDownloadUrl.setTotalDocs(100L);
    segmentWithNoDownloadUrl.setCrc(1000);
    segmentWithNoDownloadUrl.setDownloadUrl("");
    candidateSegments = UpsertCompactMergeTaskGenerator.getCandidateSegments(taskConfigs,
        List.of(segmentWithNoDownloadUrl), System.currentTimeMillis());
    Assert.assertEquals(candidateSegments.size(), 0);

    // candidates are within buffer period
    SegmentZKMetadata segmentWithinBufferPeriod = new SegmentZKMetadata("testTable__3");
    segmentWithinBufferPeriod.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentWithinBufferPeriod.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("1d"));
    segmentWithinBufferPeriod.setEndTime(System.currentTimeMillis());
    segmentWithinBufferPeriod.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentWithinBufferPeriod.setTotalDocs(100L);
    segmentWithinBufferPeriod.setCrc(1000);
    segmentWithinBufferPeriod.setDownloadUrl("fs://testTable__3");
    candidateSegments = UpsertCompactMergeTaskGenerator.getCandidateSegments(taskConfigs,
        List.of(segmentWithinBufferPeriod), System.currentTimeMillis());
    Assert.assertEquals(candidateSegments.size(), 0);

    // no completed segment
    SegmentZKMetadata incompleteSegment = new SegmentZKMetadata("testTable__4");
    incompleteSegment.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    incompleteSegment.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("1d"));
    incompleteSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    incompleteSegment.setTotalDocs(100L);
    incompleteSegment.setCrc(1000);
    candidateSegments = UpsertCompactMergeTaskGenerator.getCandidateSegments(taskConfigs,
        List.of(incompleteSegment), System.currentTimeMillis());
    Assert.assertEquals(candidateSegments.size(), 0);
  }

  @Test
  public void testGetDownloadUrl() {
    // empty list
    List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata> segmentMergerMetadataList = Arrays.asList();
    Assert.assertEquals(_taskGenerator.getDownloadUrl(segmentMergerMetadataList), "");

    // single segment
    segmentMergerMetadataList =
        List.of(new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10, 100000));
    Assert.assertEquals(_taskGenerator.getDownloadUrl(segmentMergerMetadataList), "fs://testTable__0__0__12345");

    // multiple segments
    segmentMergerMetadataList = Arrays.asList(
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10, 100000),
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment2, 200, 20, 100000)
    );
    Assert.assertEquals(_taskGenerator.getDownloadUrl(segmentMergerMetadataList),
        "fs://testTable__0__0__12345,fs://testTable__0__1__12346");
  }

  @Test
  public void testGetSegmentCrcList() {
    // empty list
    List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata> segmentMergerMetadataList = Arrays.asList();
    Assert.assertEquals(_taskGenerator.getSegmentCrcList(segmentMergerMetadataList), "");

    // single segment
    segmentMergerMetadataList =
        List.of(new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10, 100000));
    Assert.assertEquals(_taskGenerator.getSegmentCrcList(segmentMergerMetadataList), "1000");

    // multiple segments
    segmentMergerMetadataList = Arrays.asList(
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10, 100000),
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment2, 200, 20, 100000)
    );
    Assert.assertEquals(_taskGenerator.getSegmentCrcList(segmentMergerMetadataList), "1000,2000");
  }

  @Test
  public void testGetMaxZKCreationTimeMillis() {
    // Set creation times for test segments
    long creationTime1 = System.currentTimeMillis() - 1000L;
    long creationTime2 = System.currentTimeMillis() - 500L;
    _completedSegment.setCreationTime(creationTime1);
    _completedSegment2.setCreationTime(creationTime2);

    // empty list - should return -1
    List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata> segmentMergerMetadataList = Arrays.asList();
    long result = _taskGenerator.getMaxZKCreationTimeMillis(segmentMergerMetadataList);
    Assert.assertEquals(result, -1);

    // single segment
    segmentMergerMetadataList =
        List.of(new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10, 100000));
    Assert.assertEquals(_taskGenerator.getMaxZKCreationTimeMillis(segmentMergerMetadataList), creationTime1);

    // multiple segments - should return the maximum creation time
    segmentMergerMetadataList =
        Arrays.asList(new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10, 100000),
            new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment2, 200, 20, 100000));
    Assert.assertEquals(_taskGenerator.getMaxZKCreationTimeMillis(segmentMergerMetadataList), creationTime2);
  }

  /**
   * Tests the generateTasks method with various scenarios.
   * This test is disabled because it requires complex mocking of server segment metadata reader.
   * The actual integration test covers this scenario.
   */
  @Test(enabled = false)
  public void testGenerateTasks()
      throws Exception {
    // This test would require extensive mocking of ServerSegmentMetadataReader
    // which is better covered in the integration test
  }

  /**
   * Tests task generation with incomplete tasks present.
   */
  @Test
  public void testGenerateTasksWithIncompleteTasks() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.BUFFER_TIME_PERIOD_KEY, "1d");

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setTaskConfig(new TableTaskConfig(Map.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, taskConfigs)))
        .build();

    // Mock incomplete tasks
    try (MockedStatic<TaskGeneratorUtils> taskGeneratorUtilsMock = Mockito.mockStatic(TaskGeneratorUtils.class)) {
      Map<String, TaskState> incompleteTasks = new HashMap<>();
      incompleteTasks.put("task1", TaskState.IN_PROGRESS);

      taskGeneratorUtilsMock.when(() -> TaskGeneratorUtils.getIncompleteTasks(
              Mockito.anyString(), Mockito.anyString(), Mockito.any()))
          .thenReturn(incompleteTasks);

      // Generate tasks
      List<PinotTaskConfig> tasks = _taskGenerator.generateTasks(Arrays.asList(tableConfig));

      // Verify no tasks were generated due to incomplete tasks
      Assert.assertTrue(tasks.isEmpty(), "Should not generate tasks when incomplete tasks exist");
    }
  }

  /**
   * Tests processValidDocIdsMetadata method.
   */
  @Test
  public void testProcessValidDocIdsMetadata() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.OUTPUT_SEGMENT_MAX_SIZE_KEY, "100M");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_SEGMENTS_PER_TASK_KEY, "5");

    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>(_completedSegmentsMap);

    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadata = new HashMap<>();
    validDocIdsMetadata.put("testTable__0__0__12345", Arrays.asList(
        new ValidDocIdsMetadataInfo("testTable__0__0__12345", 90, 10, 100, "1000",
            ValidDocIdsType.SNAPSHOT, 100000, System.currentTimeMillis(), "server1",
            ServiceStatus.Status.GOOD)));
    validDocIdsMetadata.put("testTable__0__1__12346", Arrays.asList(
        new ValidDocIdsMetadataInfo("testTable__0__1__12346", 8, 2, 10, "2000",
            ValidDocIdsType.SNAPSHOT, 10000, System.currentTimeMillis(), "server1",
            ServiceStatus.Status.GOOD)));

    Set<String> alreadyMergedSegments = Collections.emptySet();

    SegmentSelectionResult result = UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
        RAW_TABLE_NAME + "_REALTIME", taskConfigs, candidateSegmentsMap,
        validDocIdsMetadata, alreadyMergedSegments);

    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getSegmentsForCompactMergeByPartition());
    Assert.assertNotNull(result.getSegmentsForDeletion());

    // Verify segments are grouped by partition
    Map<Integer, List<List<SegmentMergerMetadata>>> segmentsByPartition =
        result.getSegmentsForCompactMergeByPartition();
    Assert.assertTrue(segmentsByPartition.containsKey(0), "Should have segments for partition 0");
  }

  /**
   * Tests processValidDocIdsMetadata with segments that should be deleted.
   */
  @Test
  public void testProcessValidDocIdsMetadataWithSegmentsForDeletion() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.OUTPUT_SEGMENT_MAX_SIZE_KEY, "100M");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_SEGMENTS_PER_TASK_KEY, "5");

    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>(_completedSegmentsMap);

    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadata = new HashMap<>();
    // Segment with 0 valid docs - should be marked for deletion
    validDocIdsMetadata.put("testTable__0__0__12345", Arrays.asList(
        new ValidDocIdsMetadataInfo("testTable__0__0__12345", 0, 100, 100, "1000",
            ValidDocIdsType.SNAPSHOT, 100000, System.currentTimeMillis(), "server1",
            ServiceStatus.Status.GOOD)));
    validDocIdsMetadata.put("testTable__0__1__12346", Arrays.asList(
        new ValidDocIdsMetadataInfo("testTable__0__1__12346", 8, 2, 10, "2000",
            ValidDocIdsType.SNAPSHOT, 10000, System.currentTimeMillis(), "server1",
            ServiceStatus.Status.GOOD)));

    Set<String> alreadyMergedSegments = Collections.emptySet();

    SegmentSelectionResult result = UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
        RAW_TABLE_NAME + "_REALTIME", taskConfigs, candidateSegmentsMap,
        validDocIdsMetadata, alreadyMergedSegments);

    Assert.assertNotNull(result);
    Assert.assertEquals(result.getSegmentsForDeletion().size(), 1, "Should have one segment for deletion");
    Assert.assertTrue(result.getSegmentsForDeletion().contains("testTable__0__0__12345"));
  }

  /**
   * Tests getCandidateSegments with various edge cases.
   */
  @Test
  public void testGetCandidateSegmentsEdgeCases() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.BUFFER_TIME_PERIOD_KEY, "0d");

    // Test with null download URL
    SegmentZKMetadata segmentWithNullUrl = new SegmentZKMetadata("testTable__0__2__12347");
    segmentWithNullUrl.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentWithNullUrl.setDownloadUrl(null);
    segmentWithNullUrl.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("1d"));

    List<SegmentZKMetadata> candidates = UpsertCompactMergeTaskGenerator.getCandidateSegments(
        taskConfigs, Arrays.asList(segmentWithNullUrl), System.currentTimeMillis());
    Assert.assertEquals(candidates.size(), 0, "Should exclude segments with null download URL");

    // Test with segment having endTime = 0
    SegmentZKMetadata segmentWithZeroEndTime = new SegmentZKMetadata("testTable__0__3__12348");
    segmentWithZeroEndTime.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentWithZeroEndTime.setDownloadUrl("fs://testTable__0__3__12348");
    segmentWithZeroEndTime.setEndTime(0);

    candidates = UpsertCompactMergeTaskGenerator.getCandidateSegments(
        taskConfigs, Arrays.asList(segmentWithZeroEndTime), System.currentTimeMillis());
    Assert.assertEquals(candidates.size(), 1, "Should include segments with endTime = 0");
  }

  /**
   * Tests task type method.
   */
  @Test
  public void testGetTaskType() {
    Assert.assertEquals(_taskGenerator.getTaskType(), MinionConstants.UpsertCompactMergeTask.TASK_TYPE);
  }

  /**
   * Tests validation with offline table (should fail).
   */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateTaskConfigsWithOfflineTable() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .build();

    _taskGenerator.validateTaskConfigs(tableConfig, new Schema(), Collections.emptyMap());
  }

  /**
   * Tests validation with invalid time period format.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidateTaskConfigsWithInvalidTimePeriod() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .build();

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.BUFFER_TIME_PERIOD_KEY, "invalid");

    _taskGenerator.validateTaskConfigs(tableConfig, new Schema(), taskConfigs);
  }

  /**
   * Tests getAlreadyMergedSegments with complex scenarios.
   */
  @Test
  public void testGetAlreadyMergedSegmentsComplex() {
    // Create multiple merged segments
    SegmentZKMetadata mergedSegment1 = new SegmentZKMetadata("testTable__merged__1");
    mergedSegment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    Map<String, String> customMap1 = new HashMap<>();
    customMap1.put(MinionConstants.UpsertCompactMergeTask.TASK_TYPE
        + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX, "seg1,seg2,seg3");
    mergedSegment1.setCustomMap(customMap1);

    SegmentZKMetadata mergedSegment2 = new SegmentZKMetadata("testTable__merged__2");
    mergedSegment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    Map<String, String> customMap2 = new HashMap<>();
    customMap2.put(MinionConstants.UpsertCompactMergeTask.TASK_TYPE
        + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX, "seg4,seg5");
    mergedSegment2.setCustomMap(customMap2);

    // Add a segment without custom map
    SegmentZKMetadata normalSegment = new SegmentZKMetadata("testTable__0__4__12349");
    normalSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);

    List<SegmentZKMetadata> allSegments = Arrays.asList(
        mergedSegment1, mergedSegment2, normalSegment, _completedSegment, _completedSegment2);

    Set<String> alreadyMerged = UpsertCompactMergeTaskGenerator.getAlreadyMergedSegments(allSegments);

    Assert.assertEquals(alreadyMerged.size(), 5, "Should have 5 already merged segments");
    Assert.assertTrue(alreadyMerged.contains("seg1"));
    Assert.assertTrue(alreadyMerged.contains("seg2"));
    Assert.assertTrue(alreadyMerged.contains("seg3"));
    Assert.assertTrue(alreadyMerged.contains("seg4"));
    Assert.assertTrue(alreadyMerged.contains("seg5"));
  }

  /**
   * Tests segment name and CRC list generation with edge cases.
   */
  @Test
  public void testSegmentListGenerationEdgeCases() {
    // Test with segments having special characters
    SegmentZKMetadata specialSegment = new SegmentZKMetadata("test-Table__0__0__12345");
    specialSegment.setCrc(9999);

    List<SegmentMergerMetadata> segmentList = Arrays.asList(
        new SegmentMergerMetadata(specialSegment, 100, 10, 100000));

    String downloadUrl = _taskGenerator.getDownloadUrl(segmentList);
    Assert.assertEquals(downloadUrl, "", "Should return empty string when download URL is null");

    String crcList = _taskGenerator.getSegmentCrcList(segmentList);
    Assert.assertEquals(crcList, "9999");
  }
}
