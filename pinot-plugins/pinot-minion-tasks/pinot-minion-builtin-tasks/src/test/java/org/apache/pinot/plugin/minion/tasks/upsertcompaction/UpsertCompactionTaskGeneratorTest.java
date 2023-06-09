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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    Long endTime = System.currentTimeMillis();
    Long startTime = endTime - TimeUtils.convertPeriodToMillis("1d");
    _completedSegment.setStartTime(startTime);
    _completedSegment.setEndTime(endTime);
    _completedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
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
  public void testGetUrlToSegmentMappings()
      throws URISyntaxException {
    List<SegmentZKMetadata> completedSegments = Lists.newArrayList(_completedSegment);
    Map<String, String> segmentToServer = new HashMap<>();
    segmentToServer.put(_completedSegment.getSegmentName(), "server1");
    BiMap<String, String> serverToEndpoints = HashBiMap.create(1);
    serverToEndpoints.put("server1", "http://endpoint1");

    Map<String, SegmentZKMetadata> urlToSegment =
        UpsertCompactionTaskGenerator.getUrlToSegmentMappings(
            REALTIME_TABLE_NAME, completedSegments, segmentToServer, serverToEndpoints);

    String expectedUrl = String.format("%s/tables/%s/segments/%s/validDocIdMetadata",
        "http://endpoint1", REALTIME_TABLE_NAME, _completedSegment.getSegmentName());
    SegmentZKMetadata seg = urlToSegment.get(expectedUrl);
    assertEquals(seg.getSegmentName(), _completedSegment.getSegmentName());
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
  public void testGetSegmentToServer() {
    Map<String, List<String>> serverToSegments = new HashMap<>();
    serverToSegments.put("server1", Lists.newArrayList(_completedSegment.getSegmentName()));

    Map<String, String> segmentToServer = UpsertCompactionTaskGenerator.getSegmentToServer(serverToSegments);

    assertEquals(segmentToServer.get(_completedSegment.getSegmentName()), "server1");
  }
}
