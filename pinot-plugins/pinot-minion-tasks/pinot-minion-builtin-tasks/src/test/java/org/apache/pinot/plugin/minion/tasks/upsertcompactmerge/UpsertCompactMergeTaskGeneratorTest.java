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
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class UpsertCompactMergeTaskGeneratorTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private UpsertCompactMergeTaskGenerator _taskGenerator;
  private SegmentZKMetadata _completedSegment;
  private SegmentZKMetadata _completedSegment2;
  private Map<String, SegmentZKMetadata> _completedSegmentsMap;

  @BeforeClass
  public void setUp() {
    _taskGenerator = new UpsertCompactMergeTaskGenerator();

    _completedSegment = new SegmentZKMetadata("testTable__0");
    _completedSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("12d"));
    _completedSegment.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("11d"));
    _completedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment.setTotalDocs(100L);
    _completedSegment.setCrc(1000);
    _completedSegment.setDownloadUrl("fs://testTable__0");

    _completedSegment2 = new SegmentZKMetadata("testTable__1");
    _completedSegment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment2.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("10d"));
    _completedSegment2.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("9d"));
    _completedSegment2.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment2.setTotalDocs(10L);
    _completedSegment2.setCrc(2000);
    _completedSegment2.setDownloadUrl("fs://testTable__1");

    _completedSegmentsMap = new HashMap<>();
    _completedSegmentsMap.put(_completedSegment.getSegmentName(), _completedSegment);
    _completedSegmentsMap.put(_completedSegment2.getSegmentName(), _completedSegment2);
  }

  @Test
  public void testProcessValidDocIdsMetadataComprehensive() {
    String tableNameWithType = "testTable_REALTIME";
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "1000000");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_TASK_KEY, "5000000");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_SEGMENTS_PER_TASK_KEY, "10");

    // Create test segments for same partition
    SegmentZKMetadata segment1 = new SegmentZKMetadata("testTable__0__0__20240101T0000Z");
    segment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment1.setDownloadUrl("fs://testTable__0__0__20240101T0000Z");
    segment1.setCrc(1000L);
    segment1.setCreationTime(System.currentTimeMillis() - 1000L);

    SegmentZKMetadata segment2 = new SegmentZKMetadata("testTable__0__1__20240101T0100Z");
    segment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment2.setDownloadUrl("fs://testTable__0__1__20240101T0100Z");
    segment2.setCrc(2000L);
    segment2.setCreationTime(System.currentTimeMillis() - 500L);

    // Create segment that should be deleted (all invalid docs)
    SegmentZKMetadata segment3 = new SegmentZKMetadata("testTable__0__2__20240101T0200Z");
    segment3.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment3.setDownloadUrl("fs://testTable__0__2__20240101T0200Z");
    segment3.setCrc(3000L);
    segment3.setCreationTime(System.currentTimeMillis() - 200L);

    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>();
    candidateSegmentsMap.put(segment1.getSegmentName(), segment1);
    candidateSegmentsMap.put(segment2.getSegmentName(), segment2);
    candidateSegmentsMap.put(segment3.getSegmentName(), segment3);

    // Create valid doc IDs metadata
    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataMap = new HashMap<>();

    // Segment 1: 60 valid, 40 invalid docs
    ValidDocIdsMetadataInfo metadata1 = new ValidDocIdsMetadataInfo(
        segment1.getSegmentName(), 60L, 40L, 100L, "1000", ValidDocIdsType.SNAPSHOT, 10000L,
        System.currentTimeMillis() - 1000L);
    validDocIdsMetadataMap.put(segment1.getSegmentName(), Arrays.asList(metadata1));

    // Segment 2: 70 valid, 30 invalid docs
    ValidDocIdsMetadataInfo metadata2 = new ValidDocIdsMetadataInfo(
        segment2.getSegmentName(), 70L, 30L, 100L, "2000", ValidDocIdsType.SNAPSHOT, 12000L,
        System.currentTimeMillis() - 500L);
    validDocIdsMetadataMap.put(segment2.getSegmentName(), Arrays.asList(metadata2));

    // Segment 3: 0 valid, 50 invalid docs (should be deleted)
    ValidDocIdsMetadataInfo metadata3 = new ValidDocIdsMetadataInfo(
        segment3.getSegmentName(), 0L, 50L, 50L, "3000", ValidDocIdsType.SNAPSHOT, 5000L,
        System.currentTimeMillis() - 200L);
    validDocIdsMetadataMap.put(segment3.getSegmentName(), Arrays.asList(metadata3));

    Set<String> alreadyMergedSegments = Collections.emptySet();

    // Process the metadata
    UpsertCompactMergeTaskGenerator.SegmentSelectionResult result =
        UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
            tableNameWithType, taskConfigs, candidateSegmentsMap, validDocIdsMetadataMap, alreadyMergedSegments);

    // Verify results
    Assert.assertNotNull(result);

    // Should have one segment marked for deletion (segment3 with all invalid docs)
    Assert.assertEquals(result.getSegmentsForDeletion().size(), 1);
    Assert.assertTrue(result.getSegmentsForDeletion().contains(segment3.getSegmentName()));

    // Should have segments for compact merge (segment1 and segment2 grouped together)
    Map<Integer, List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>>>
        segmentsForCompactMerge = result.getSegmentsForCompactMergeByPartition();
    Assert.assertNotNull(segmentsForCompactMerge);
    Assert.assertEquals(segmentsForCompactMerge.size(), 1); // One partition (partition 0)

    List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>> groups =
        segmentsForCompactMerge.get(0);
    Assert.assertNotNull(groups);
    Assert.assertEquals(groups.size(), 1); // One group containing both segments

    List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata> group = groups.get(0);
    Assert.assertEquals(group.size(), 2); // Two segments in the group

    // Verify the segments are ordered by creation time (older first)
    Assert.assertEquals(group.get(0).getSegmentZKMetadata().getSegmentName(), segment1.getSegmentName());
    Assert.assertEquals(group.get(1).getSegmentZKMetadata().getSegmentName(), segment2.getSegmentName());

    // Verify metadata values
    Assert.assertEquals(group.get(0).getValidDocIds(), 60L);
    Assert.assertEquals(group.get(0).getInvalidDocIds(), 40L);
    Assert.assertEquals(group.get(1).getValidDocIds(), 70L);
    Assert.assertEquals(group.get(1).getInvalidDocIds(), 30L);
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
    Assert.assertEquals(_taskGenerator.getDownloadUrl(segmentMergerMetadataList), "fs://testTable__0");

    // multiple segments
    segmentMergerMetadataList = Arrays.asList(
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10, 100000),
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment2, 200, 20, 100000)
    );
    Assert.assertEquals(_taskGenerator.getDownloadUrl(segmentMergerMetadataList),
        "fs://testTable__0,fs://testTable__1");
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
  public void testGenerateTasksWithNullTaskConfig() {
    // Create table config without task configuration
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .build();

    // Execute generateTasks - should return empty list when no task config
    List<TableConfig> tableConfigs = Arrays.asList(tableConfig);
    Assert.assertNotNull(_taskGenerator.generateTasks(tableConfigs));
    Assert.assertTrue(_taskGenerator.generateTasks(tableConfigs).isEmpty());
  }

  @Test
  public void testGenerateTasksWithEmptyTableConfigList() {
    // Execute generateTasks with empty list
    List<TableConfig> emptyTableConfigs = Collections.emptyList();
    Assert.assertNotNull(_taskGenerator.generateTasks(emptyTableConfigs));
    Assert.assertTrue(_taskGenerator.generateTasks(emptyTableConfigs).isEmpty());
  }

  @Test
  public void testProcessValidDocIdsMetadataMultiplePartitions() {
    String tableNameWithType = "testTable_REALTIME";
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "1000000");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_TASK_KEY, "5000000");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_SEGMENTS_PER_TASK_KEY, "10");

    // Create segments for different partitions
    SegmentZKMetadata partition0Segment1 = new SegmentZKMetadata("testTable__0__0__20240101T0000Z");
    partition0Segment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    partition0Segment1.setDownloadUrl("fs://testTable__0__0__20240101T0000Z");
    partition0Segment1.setCrc(1000L);
    partition0Segment1.setCreationTime(System.currentTimeMillis() - 1000L);

    SegmentZKMetadata partition0Segment2 = new SegmentZKMetadata("testTable__0__1__20240101T0100Z");
    partition0Segment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    partition0Segment2.setDownloadUrl("fs://testTable__0__1__20240101T0100Z");
    partition0Segment2.setCrc(2000L);
    partition0Segment2.setCreationTime(System.currentTimeMillis() - 500L);

    SegmentZKMetadata partition1Segment1 = new SegmentZKMetadata("testTable__1__0__20240101T0000Z");
    partition1Segment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    partition1Segment1.setDownloadUrl("fs://testTable__1__0__20240101T0000Z");
    partition1Segment1.setCrc(3000L);
    partition1Segment1.setCreationTime(System.currentTimeMillis() - 800L);

    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>();
    candidateSegmentsMap.put(partition0Segment1.getSegmentName(), partition0Segment1);
    candidateSegmentsMap.put(partition0Segment2.getSegmentName(), partition0Segment2);
    candidateSegmentsMap.put(partition1Segment1.getSegmentName(), partition1Segment1);

    // Create valid doc IDs metadata
    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataMap = new HashMap<>();

    validDocIdsMetadataMap.put(partition0Segment1.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(partition0Segment1.getSegmentName(), 60L, 40L, 100L, "1000",
            ValidDocIdsType.SNAPSHOT, 10000L, System.currentTimeMillis() - 1000L)));

    validDocIdsMetadataMap.put(partition0Segment2.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(partition0Segment2.getSegmentName(), 70L, 30L, 100L, "2000",
            ValidDocIdsType.SNAPSHOT, 12000L, System.currentTimeMillis() - 500L)));

    validDocIdsMetadataMap.put(partition1Segment1.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(partition1Segment1.getSegmentName(), 80L, 20L, 100L, "3000",
            ValidDocIdsType.SNAPSHOT, 11000L, System.currentTimeMillis() - 800L)));

    Set<String> alreadyMergedSegments = Collections.emptySet();

    // Process the metadata
    UpsertCompactMergeTaskGenerator.SegmentSelectionResult result =
        UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
            tableNameWithType, taskConfigs, candidateSegmentsMap, validDocIdsMetadataMap, alreadyMergedSegments);

    // Verify results
    Assert.assertNotNull(result);
    Assert.assertTrue(result.getSegmentsForDeletion().isEmpty()); // No segments marked for deletion

        // Should have segments for compact merge
    Map<Integer, List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>>>
        segmentsForCompactMerge = result.getSegmentsForCompactMergeByPartition();
    Assert.assertNotNull(segmentsForCompactMerge);
    // Only partition 0 will have groups since it has 2 segments; partition 1 has only 1 segment
    Assert.assertEquals(segmentsForCompactMerge.size(), 1);

    // Partition 0 should have a group with 2 segments
    Assert.assertTrue(segmentsForCompactMerge.containsKey(0));
    List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>> partition0Groups =
        segmentsForCompactMerge.get(0);
    Assert.assertEquals(partition0Groups.size(), 1);
    Assert.assertEquals(partition0Groups.get(0).size(), 2);

    // Partition 1 segments should not appear since single segments don't get grouped
    Assert.assertFalse(segmentsForCompactMerge.containsKey(1));
  }

  @Test
  public void testProcessValidDocIdsMetadataWithAlreadyMergedSegments() {
    String tableNameWithType = "testTable_REALTIME";
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "1000000");

    // Create test segments
    SegmentZKMetadata segment1 = new SegmentZKMetadata("testTable__0__0__20240101T0000Z");
    segment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment1.setDownloadUrl("fs://testTable__0__0__20240101T0000Z");
    segment1.setCrc(1000L);

    SegmentZKMetadata segment2 = new SegmentZKMetadata("testTable__0__1__20240101T0100Z");
    segment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment2.setDownloadUrl("fs://testTable__0__1__20240101T0100Z");
    segment2.setCrc(2000L);

    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>();
    candidateSegmentsMap.put(segment1.getSegmentName(), segment1);
    candidateSegmentsMap.put(segment2.getSegmentName(), segment2);

    // Create valid doc IDs metadata
    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataMap = new HashMap<>();
    validDocIdsMetadataMap.put(segment1.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(segment1.getSegmentName(), 60L, 40L, 100L, "1000",
            ValidDocIdsType.SNAPSHOT, 10000L, System.currentTimeMillis())));
    validDocIdsMetadataMap.put(segment2.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(segment2.getSegmentName(), 70L, 30L, 100L, "2000",
            ValidDocIdsType.SNAPSHOT, 12000L, System.currentTimeMillis())));

    // Mark segment1 as already merged
    Set<String> alreadyMergedSegments = Collections.singleton(segment1.getSegmentName());

    // Process the metadata
    UpsertCompactMergeTaskGenerator.SegmentSelectionResult result =
        UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
            tableNameWithType, taskConfigs, candidateSegmentsMap, validDocIdsMetadataMap, alreadyMergedSegments);

    // Verify results - segment1 should be skipped due to being already merged
    Assert.assertNotNull(result);
    Assert.assertTrue(result.getSegmentsForDeletion().isEmpty());

    Map<Integer, List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>>>
        segmentsForCompactMerge = result.getSegmentsForCompactMergeByPartition();

    // Only segment2 should be available, but since it's alone, no groups should be formed
    if (segmentsForCompactMerge.containsKey(0)) {
      Assert.assertTrue(segmentsForCompactMerge.get(0).isEmpty());
    }
  }

  @Test
  public void testProcessValidDocIdsMetadataWithCrcMismatch() {
    String tableNameWithType = "testTable_REALTIME";
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "1000000");

    // Create test segment
    SegmentZKMetadata segment1 = new SegmentZKMetadata("testTable__0__0__20240101T0000Z");
    segment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment1.setDownloadUrl("fs://testTable__0__0__20240101T0000Z");
    segment1.setCrc(1000L); // CRC in segment metadata

    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>();
    candidateSegmentsMap.put(segment1.getSegmentName(), segment1);

    // Create valid doc IDs metadata with different CRC
    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataMap = new HashMap<>();
    validDocIdsMetadataMap.put(segment1.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(segment1.getSegmentName(), 60L, 40L, 100L, "9999", // Different CRC
            ValidDocIdsType.SNAPSHOT, 10000L, System.currentTimeMillis())));

    Set<String> alreadyMergedSegments = Collections.emptySet();

    // Process the metadata
    UpsertCompactMergeTaskGenerator.SegmentSelectionResult result =
        UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
            tableNameWithType, taskConfigs, candidateSegmentsMap, validDocIdsMetadataMap, alreadyMergedSegments);

    // Verify results - segment should be skipped due to CRC mismatch
    Assert.assertNotNull(result);
    Assert.assertTrue(result.getSegmentsForDeletion().isEmpty());

    Map<Integer, List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>>>
        segmentsForCompactMerge = result.getSegmentsForCompactMergeByPartition();
    Assert.assertTrue(segmentsForCompactMerge.isEmpty());
  }

  @Test
  public void testProcessValidDocIdsMetadataWithMaxRecordsPerTaskLimit() {
    String tableNameWithType = "testTable_REALTIME";
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "1000000");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_TASK_KEY, "150"); // Low limit
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_SEGMENTS_PER_TASK_KEY, "10");

    // Create test segments with many records
    SegmentZKMetadata segment1 = new SegmentZKMetadata("testTable__0__0__20240101T0000Z");
    segment1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment1.setDownloadUrl("fs://testTable__0__0__20240101T0000Z");
    segment1.setCrc(1000L);
    segment1.setCreationTime(System.currentTimeMillis() - 1000L);

    SegmentZKMetadata segment2 = new SegmentZKMetadata("testTable__0__1__20240101T0100Z");
    segment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment2.setDownloadUrl("fs://testTable__0__1__20240101T0100Z");
    segment2.setCrc(2000L);
    segment2.setCreationTime(System.currentTimeMillis() - 500L);

    SegmentZKMetadata segment3 = new SegmentZKMetadata("testTable__0__2__20240101T0200Z");
    segment3.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segment3.setDownloadUrl("fs://testTable__0__2__20240101T0200Z");
    segment3.setCrc(3000L);
    segment3.setCreationTime(System.currentTimeMillis() - 200L);

    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>();
    candidateSegmentsMap.put(segment1.getSegmentName(), segment1);
    candidateSegmentsMap.put(segment2.getSegmentName(), segment2);
    candidateSegmentsMap.put(segment3.getSegmentName(), segment3);

    // Create valid doc IDs metadata - each segment has 100 total docs (valid + invalid)
    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataMap = new HashMap<>();

    validDocIdsMetadataMap.put(segment1.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(segment1.getSegmentName(), 60L, 40L, 100L, "1000",
            ValidDocIdsType.SNAPSHOT, 10000L, System.currentTimeMillis() - 1000L)));

    validDocIdsMetadataMap.put(segment2.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(segment2.getSegmentName(), 70L, 30L, 100L, "2000",
            ValidDocIdsType.SNAPSHOT, 12000L, System.currentTimeMillis() - 500L)));

    validDocIdsMetadataMap.put(segment3.getSegmentName(), Arrays.asList(
        new ValidDocIdsMetadataInfo(segment3.getSegmentName(), 50L, 50L, 100L, "3000",
            ValidDocIdsType.SNAPSHOT, 11000L, System.currentTimeMillis() - 200L)));

    Set<String> alreadyMergedSegments = Collections.emptySet();

    // Process the metadata
    UpsertCompactMergeTaskGenerator.SegmentSelectionResult result =
        UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
            tableNameWithType, taskConfigs, candidateSegmentsMap, validDocIdsMetadataMap, alreadyMergedSegments);

        // Verify results - segments should be split into multiple groups due to record limit
    Assert.assertNotNull(result);
    Assert.assertTrue(result.getSegmentsForDeletion().isEmpty());

    Map<Integer, List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>>>
        segmentsForCompactMerge = result.getSegmentsForCompactMergeByPartition();
    Assert.assertNotNull(segmentsForCompactMerge);

    // Due to maxRecordsPerTask=150, and each segment having 100 total docs,
    // only one segment can fit per group, so all groups have 1 segment each
    // Groups with only 1 segment are filtered out, so no partitions should be present
    Assert.assertEquals(segmentsForCompactMerge.size(), 0);
  }

  @Test
  public void testProcessValidDocIdsMetadataMaxNumSegmentsPerTaskLimit() {
    String tableNameWithType = "testTable_REALTIME";
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "1000000");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_RECORDS_PER_TASK_KEY, "5000000");
    taskConfigs.put(MinionConstants.UpsertCompactMergeTask.MAX_NUM_SEGMENTS_PER_TASK_KEY, "2"); // Low limit

    // Create 4 test segments
    List<SegmentZKMetadata> segments = new ArrayList<>();
    Map<String, SegmentZKMetadata> candidateSegmentsMap = new HashMap<>();
    Map<String, List<ValidDocIdsMetadataInfo>> validDocIdsMetadataMap = new HashMap<>();

    for (int i = 0; i < 4; i++) {
      SegmentZKMetadata segment = new SegmentZKMetadata("testTable__0__" + i + "__20240101T000" + i + "Z");
      segment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segment.setDownloadUrl("fs://testTable__0__" + i + "__20240101T000" + i + "Z");
      segment.setCrc(1000L + i);
      segment.setCreationTime(System.currentTimeMillis() - 1000L + (i * 100));
      segments.add(segment);
      candidateSegmentsMap.put(segment.getSegmentName(), segment);

      validDocIdsMetadataMap.put(segment.getSegmentName(), Arrays.asList(
          new ValidDocIdsMetadataInfo(segment.getSegmentName(), 50L, 50L, 100L, String.valueOf(1000 + i),
              ValidDocIdsType.SNAPSHOT, 10000L, System.currentTimeMillis() - 1000L + (i * 100))));
    }

    Set<String> alreadyMergedSegments = Collections.emptySet();

    // Process the metadata
    UpsertCompactMergeTaskGenerator.SegmentSelectionResult result =
        UpsertCompactMergeTaskGenerator.processValidDocIdsMetadata(
            tableNameWithType, taskConfigs, candidateSegmentsMap, validDocIdsMetadataMap, alreadyMergedSegments);

    // Verify results - segments should be grouped with max 2 segments per group
    Assert.assertNotNull(result);
    Assert.assertTrue(result.getSegmentsForDeletion().isEmpty());

    Map<Integer, List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>>>
        segmentsForCompactMerge = result.getSegmentsForCompactMergeByPartition();
    Assert.assertNotNull(segmentsForCompactMerge);
    Assert.assertEquals(segmentsForCompactMerge.size(), 1); // One partition

    List<List<UpsertCompactMergeTaskGenerator.SegmentMergerMetadata>> groups =
        segmentsForCompactMerge.get(0);
    Assert.assertNotNull(groups);

    // Should have 2 groups, each with 2 segments (due to maxNumSegmentsPerTask=2)
    Assert.assertEquals(groups.size(), 2);
    Assert.assertEquals(groups.get(0).size(), 2);
    Assert.assertEquals(groups.get(1).size(), 2);
  }
}
