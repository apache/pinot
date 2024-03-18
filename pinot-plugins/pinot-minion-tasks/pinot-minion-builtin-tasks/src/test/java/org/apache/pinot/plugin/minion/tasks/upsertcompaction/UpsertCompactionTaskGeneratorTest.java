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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsMetadataInfo;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
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
  private UpsertCompactionTaskGenerator _taskGenerator;
  private TableConfig _tableConfig;
  private ClusterInfoAccessor _mockClusterInfoAccessor;
  private SegmentZKMetadata _completedSegment;
  private SegmentZKMetadata _completedSegment2;
  private Map<String, SegmentZKMetadata> _completedSegmentsMap;

  @BeforeClass
  public void setUp() {
    _taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
            .setTaskConfig(new TableTaskConfig(tableTaskConfigs)).build();
    _mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);

    _completedSegment = new SegmentZKMetadata("testTable__0");
    _completedSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("2d"));
    _completedSegment.setEndTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("1d"));
    _completedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment.setTotalDocs(100L);

    _completedSegment2 = new SegmentZKMetadata("testTable__1");
    _completedSegment2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    _completedSegment2.setStartTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("1d"));
    _completedSegment2.setEndTime(System.currentTimeMillis());
    _completedSegment2.setTimeUnit(TimeUnit.MILLISECONDS);
    _completedSegment2.setTotalDocs(10L);

    _completedSegmentsMap = new HashMap<>();
    _completedSegmentsMap.put(_completedSegment.getSegmentName(), _completedSegment);
    _completedSegmentsMap.put(_completedSegment2.getSegmentName(), _completedSegment2);
  }

  @Test
  public void testValidate() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .build();
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfig));

    TableConfigBuilder tableConfigBuilder =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME);
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build()));

    tableConfigBuilder = tableConfigBuilder.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL));
    assertTrue(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build()));
  }

  @Test
  public void testGenerateTasksValidatesTableConfigs() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .build();
    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN_NAME)
            .build();
    pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(realtimeTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());
  }

  @Test
  public void testGenerateTasksWithNoSegments() {
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(
        Lists.newArrayList(Collections.emptyList()));
    when(_mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(
        getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList(Collections.emptyList())));

    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithConsumingSegment() {
    SegmentZKMetadata consumingSegment = new SegmentZKMetadata("testTable__0");
    consumingSegment.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(
        Lists.newArrayList(consumingSegment));
    when(_mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(
        getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList("testTable__0")));

    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithNewlyCompletedSegment() {
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(
        Lists.newArrayList(_completedSegment));
    when(_mockClusterInfoAccessor.getIdealState(REALTIME_TABLE_NAME)).thenReturn(
        getIdealState(REALTIME_TABLE_NAME, Lists.newArrayList(_completedSegment.getSegmentName())));

    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGetMaxTasks() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.TABLE_MAX_NUM_TASKS_KEY, "10");

    int maxTasks =
        UpsertCompactionTaskGenerator.getMaxTasks(UpsertCompactionTask.TASK_TYPE, REALTIME_TABLE_NAME, taskConfigs);

    assertEquals(maxTasks, 10);
  }

  @Test
  public void testGetCompletedSegments() {
    long currentTimeInMillis = System.currentTimeMillis();
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "1d");

    SegmentZKMetadata metadata1 = new SegmentZKMetadata("testTable");
    metadata1.setEndTime(1694198844776L);
    metadata1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata1.setTimeUnit(TimeUnit.MILLISECONDS);
    SegmentZKMetadata metadata2 = new SegmentZKMetadata("testTable");
    metadata2.setEndTime(1699639830678L);
    metadata2.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata2.setTimeUnit(TimeUnit.MILLISECONDS);

    SegmentZKMetadata metadata3 = new SegmentZKMetadata("testTable");
    metadata3.setEndTime(currentTimeInMillis);
    metadata3.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata3.setTimeUnit(TimeUnit.MILLISECONDS);

    List<SegmentZKMetadata> segmentZKMetadataList = new ArrayList<>();
    segmentZKMetadataList.add(metadata1);
    segmentZKMetadataList.add(metadata2);
    segmentZKMetadataList.add(metadata3);

    List<SegmentZKMetadata> result =
        UpsertCompactionTaskGenerator.getCompletedSegments(taskConfigs, segmentZKMetadataList, currentTimeInMillis);
    Assert.assertEquals(result.size(), 2);

    SegmentZKMetadata metadata4 = new SegmentZKMetadata("testTable");
    metadata4.setEndTime(currentTimeInMillis - TimeUtils.convertPeriodToMillis("2d") + 1);
    metadata4.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    metadata4.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadataList.add(metadata4);

    result =
        UpsertCompactionTaskGenerator.getCompletedSegments(taskConfigs, segmentZKMetadataList, currentTimeInMillis);
    Assert.assertEquals(result.size(), 3);

    // Check the boundary condition for buffer time period based filtering
    taskConfigs.put(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, "2d");
    result =
        UpsertCompactionTaskGenerator.getCompletedSegments(taskConfigs, segmentZKMetadataList, currentTimeInMillis);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testProcessValidDocIdsMetadata()
      throws IOException {
    Map<String, String> compactionConfigs = getCompactionConfigs("1", "10");
    String json = "[{" + "\"totalValidDocs\" : 50," + "\"totalInvalidDocs\" : 50," + "\"segmentName\" : \""
        + _completedSegment.getSegmentName() + "\"," + "\"totalDocs\" : 100" + ", \"segmentCrc\": \""
        + _completedSegment.getCrc() + "\"}," + "{" + "\"totalValidDocs\" : 0," + "\"totalInvalidDocs\" : 10,"
        + "\"segmentName\" : \"" + _completedSegment2.getSegmentName() + "\", " + "\"segmentCrc\" : \""
        + _completedSegment2.getCrc() + "\"," + "\"totalDocs\" : 10" + "}]";

    List<ValidDocIdsMetadataInfo> validDocIdsMetadataInfo =
        JsonUtils.stringToObject(json, new TypeReference<ArrayList<ValidDocIdsMetadataInfo>>() {
        });

    UpsertCompactionTaskGenerator.SegmentSelectionResult segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, new HashMap<>(),
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 0);

    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // test with a higher invalidRecordsThresholdPercent
    compactionConfigs = getCompactionConfigs("60", "10");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertTrue(segmentSelectionResult.getSegmentsForCompaction().isEmpty());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // test without an invalidRecordsThresholdPercent
    compactionConfigs = getCompactionConfigs("0", "10");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // test without a invalidRecordsThresholdCount
    compactionConfigs = getCompactionConfigs("30", "0");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // Test the case where the completedSegment from api has different crc than segment from zk metadata.
    json = "[{" + "\"totalValidDocs\" : 50," + "\"totalInvalidDocs\" : 50," + "\"segmentName\" : \""
        + _completedSegment.getSegmentName() + "\"," + "\"totalDocs\" : 100" + ", \"segmentCrc\": \""
        + "1234567890" + "\"}," + "{" + "\"totalValidDocs\" : 0," + "\"totalInvalidDocs\" : 10,"
        + "\"segmentName\" : \"" + _completedSegment2.getSegmentName() + "\", " + "\"segmentCrc\" : \""
        + _completedSegment2.getCrc() + "\","
        + "\"totalDocs\" : 10" + "}]";
    validDocIdsMetadataInfo = JsonUtils.stringToObject(json, new TypeReference<ArrayList<ValidDocIdsMetadataInfo>>() {
    });
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);

    // completedSegment is supposed to be filtered out
    Assert.assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 0);

    // completedSegment2 is still supposed to be deleted
    Assert.assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 1);
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0),
        _completedSegment2.getSegmentName());

    // check if both the candidates for compaction are coming in sorted descending order
    json = "[{" + "\"totalValidDocs\" : 50," + "\"totalInvalidDocs\" : 50," + "\"segmentName\" : \""
        + _completedSegment.getSegmentName() + "\"," + "\"totalDocs\" : 100" + ", \"segmentCrc\": \""
        + _completedSegment.getCrc() + "\"}," + "{" + "\"totalValidDocs\" : 10," + "\"totalInvalidDocs\" : 40,"
        + "\"segmentName\" : \"" + _completedSegment2.getSegmentName() + "\", " + "\"segmentCrc\" : \""
        + _completedSegment2.getCrc() + "\"," + "\"totalDocs\" : 50" + "}]";
    validDocIdsMetadataInfo = JsonUtils.stringToObject(json, new TypeReference<ArrayList<ValidDocIdsMetadataInfo>>() {
    });
    compactionConfigs = getCompactionConfigs("30", "0");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdsMetadata(compactionConfigs, _completedSegmentsMap,
            validDocIdsMetadataInfo);
    Assert.assertEquals(segmentSelectionResult.getSegmentsForCompaction().size(), 2);
    Assert.assertEquals(segmentSelectionResult.getSegmentsForDeletion().size(), 0);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(1).getSegmentName(),
        _completedSegment2.getSegmentName());
  }

  private Map<String, String> getCompactionConfigs(String invalidRecordsThresholdPercent,
      String invalidRecordsThresholdCount) {
    Map<String, String> compactionConfigs = new HashMap<>();
    if (!invalidRecordsThresholdPercent.equals("0")) {
      compactionConfigs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT, invalidRecordsThresholdPercent);
    }
    if (!invalidRecordsThresholdCount.equals("0")) {
      compactionConfigs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT, invalidRecordsThresholdCount);
    }
    return compactionConfigs;
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
