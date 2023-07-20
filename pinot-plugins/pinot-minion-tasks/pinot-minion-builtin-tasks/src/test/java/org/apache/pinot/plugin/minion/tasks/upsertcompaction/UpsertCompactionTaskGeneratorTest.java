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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
    _tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .build();
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
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .build();
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfig));

    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME);
    assertFalse(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build()));

    tableConfigBuilder = tableConfigBuilder
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL));
    assertTrue(UpsertCompactionTaskGenerator.validate(tableConfigBuilder.build()));
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
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(Collections.emptyList()));
    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithConsumingSegment() {
    SegmentZKMetadata consumingSegment = new SegmentZKMetadata("testTable__0");
    consumingSegment.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(consumingSegment));
    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithNewlyCompletedSegment() {
    when(_mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(_completedSegment));
    _taskGenerator.init(_mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = _taskGenerator.generateTasks(Lists.newArrayList(_tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGetValidDocIdMetadataUrls()
    throws URISyntaxException {
    Map<String, List<String>> serverToSegments = new HashMap<>();
    serverToSegments.put("server1",
        Lists.newArrayList(_completedSegment.getSegmentName(), _completedSegment2.getSegmentName()));
    serverToSegments.put("server2", Lists.newArrayList("consumingSegment"));
    BiMap<String, String> serverToEndpoints = HashBiMap.create(1);
    serverToEndpoints.put("server1", "http://endpoint1");
    serverToEndpoints.put("server2", "http://endpoint2");
    Set<String> completedSegments = new HashSet<>();
    completedSegments.add(_completedSegment.getSegmentName());
    completedSegments.add(_completedSegment2.getSegmentName());

    List<String> validDocIdUrls =
        UpsertCompactionTaskGenerator.getValidDocIdMetadataUrls(
            serverToSegments, serverToEndpoints, REALTIME_TABLE_NAME, completedSegments);

    String expectedUrl = String.format("%s/tables/%s/validDocIdMetadata?segmentNames=%s&segmentNames=%s",
        "http://endpoint1", REALTIME_TABLE_NAME, _completedSegment.getSegmentName(),
        _completedSegment2.getSegmentName());
    assertEquals(validDocIdUrls.get(0), expectedUrl);
    assertEquals(validDocIdUrls.size(), 1);
  }

  @Test
  public void testGetValidDocIdMetadataUrlsWithReplicatedSegments()
      throws URISyntaxException {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server1",
        Lists.newArrayList(_completedSegment.getSegmentName(), _completedSegment2.getSegmentName()));
    serverToSegments.put("server2",
        Lists.newArrayList(_completedSegment.getSegmentName(), _completedSegment2.getSegmentName()));
    BiMap<String, String> serverToEndpoints = HashBiMap.create(1);
    serverToEndpoints.put("server1", "http://endpoint1");
    serverToEndpoints.put("server2", "http://endpoint2");
    Set<String> completedSegments = new HashSet<>();
    completedSegments.add(_completedSegment.getSegmentName());
    completedSegments.add(_completedSegment2.getSegmentName());

    List<String> validDocIdUrls = UpsertCompactionTaskGenerator.getValidDocIdMetadataUrls(
        serverToSegments, serverToEndpoints, REALTIME_TABLE_NAME, completedSegments);

    String expectedUrl = String.format("%s/tables/%s/validDocIdMetadata?segmentNames=%s&segmentNames=%s",
        "http://endpoint1", REALTIME_TABLE_NAME, _completedSegment.getSegmentName(),
        _completedSegment2.getSegmentName());
        assertEquals(validDocIdUrls.get(0), expectedUrl);
    assertEquals(validDocIdUrls.size(), 1);
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
  public void testProcessValidDocIdMetadata() {
    Map<String, String> compactionConfigs = getCompactionConfigs("1", "10");
    Set<Map.Entry<String, String>> responseSet = new HashSet<>();
    String json =
         "[{"
          + "\"totalValidDocs\" : 50,"
          + "\"totalInvalidDocs\" : 50,"
          + "\"segmentName\" : \"" + _completedSegment.getSegmentName() + "\","
          + "\"totalDocs\" : 100"
        + "},"
        + "{"
          + "\"totalValidDocs\" : 0,"
          + "\"totalInvalidDocs\" : 10,"
          + "\"segmentName\" : \"" + _completedSegment2.getSegmentName() + "\","
          + "\"totalDocs\" : 10"
        + "}]";
    responseSet.add(new AbstractMap.SimpleEntry<>("", json));
    UpsertCompactionTaskGenerator.SegmentSelectionResult segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdMetadata(compactionConfigs, _completedSegmentsMap, responseSet);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
    assertEquals(segmentSelectionResult.getSegmentsForDeletion().get(0), _completedSegment2.getSegmentName());

    // test with a higher invalidRecordsThresholdPercent
    compactionConfigs = getCompactionConfigs("60", "10");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdMetadata(compactionConfigs, _completedSegmentsMap, responseSet);
    assertTrue(segmentSelectionResult.getSegmentsForCompaction().isEmpty());

    // test without an invalidRecordsThresholdPercent
    compactionConfigs = getCompactionConfigs("0", "10");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdMetadata(compactionConfigs, _completedSegmentsMap, responseSet);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());

    // test without a minRecordCount
    compactionConfigs = getCompactionConfigs("30", "0");
    segmentSelectionResult =
        UpsertCompactionTaskGenerator.processValidDocIdMetadata(compactionConfigs, _completedSegmentsMap, responseSet);
    assertEquals(segmentSelectionResult.getSegmentsForCompaction().get(0).getSegmentName(),
        _completedSegment.getSegmentName());
  }

  private Map<String, String> getCompactionConfigs(String invalidRecordsThresholdPercent, String minRecordCount) {
    Map<String, String> compactionConfigs = new HashMap<>();
    if (!invalidRecordsThresholdPercent.equals("0")) {
      compactionConfigs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT, invalidRecordsThresholdPercent);
    }
    if (!minRecordCount.equals("0")) {
      compactionConfigs.put(UpsertCompactionTask.MIN_RECORD_COUNT, minRecordCount);
    }
    return compactionConfigs;
  }
}
