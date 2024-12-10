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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class UpsertCompactMergeTaskGeneratorTest {

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private UpsertCompactMergeTaskGenerator _taskGenerator;
  private TableConfig _tableConfig;
  private SegmentZKMetadata _completedSegment;
  private SegmentZKMetadata _completedSegment2;
  private Map<String, SegmentZKMetadata> _completedSegmentsMap;

  @BeforeClass
  public void setUp() {
    _taskGenerator = new UpsertCompactMergeTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    tableTaskConfigs.put(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, compactionConfigs);
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableSnapshot(true);
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setUpsertConfig(upsertConfig)
            .setTaskConfig(new TableTaskConfig(tableTaskConfigs)).build();

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
  public void testUpsertCompactMergeTaskConfig() {

    // check with OFFLINE table
    Map<String, String> upsertCompactMergeTaskConfig =
        ImmutableMap.of("bufferTimePeriod", "5d");
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTaskConfig(
            new TableTaskConfig(ImmutableMap.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE,
            upsertCompactMergeTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(offlineTableConfig, upsertCompactMergeTaskConfig));

    // check with non-upsert REALTIME table
    TableConfig nonUpsertRealtimetableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTaskConfig(new TableTaskConfig(ImmutableMap.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE,
            upsertCompactMergeTaskConfig)))
        .build();

    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(nonUpsertRealtimetableConfig, upsertCompactMergeTaskConfig));

    // check with snapshot disabled
    TableConfig disabledSnapshotTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(ImmutableMap.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE,
            upsertCompactMergeTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> _taskGenerator.validateTaskConfigs(disabledSnapshotTableConfig, upsertCompactMergeTaskConfig));

    // valid table configs
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableSnapshot(true);
    TableConfig validTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setTaskConfig(new TableTaskConfig(ImmutableMap.of(MinionConstants.UpsertCompactMergeTask.TASK_TYPE,
            upsertCompactMergeTaskConfig)))
        .build();
    _taskGenerator.validateTaskConfigs(validTableConfig, upsertCompactMergeTaskConfig);

    // invalid buffer time period
    Map<String, String> upsertCompactMergeTaskConfig1 =
        ImmutableMap.of("bufferTimePeriod", "5hd");
    Assert.assertThrows(IllegalArgumentException.class,
        () -> _taskGenerator.validateTaskConfigs(validTableConfig, upsertCompactMergeTaskConfig1));
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
        List.of(new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10));
    Assert.assertEquals(_taskGenerator.getDownloadUrl(segmentMergerMetadataList), "fs://testTable__0");

    // multiple segments
    segmentMergerMetadataList = Arrays.asList(
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10),
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment2, 200, 20)
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
        List.of(new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10));
    Assert.assertEquals(_taskGenerator.getSegmentCrcList(segmentMergerMetadataList), "1000");

    // multiple segments
    segmentMergerMetadataList = Arrays.asList(
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment, 100, 10),
        new UpsertCompactMergeTaskGenerator.SegmentMergerMetadata(_completedSegment2, 200, 20)
    );
    Assert.assertEquals(_taskGenerator.getSegmentCrcList(segmentMergerMetadataList), "1000,2000");
  }
}
