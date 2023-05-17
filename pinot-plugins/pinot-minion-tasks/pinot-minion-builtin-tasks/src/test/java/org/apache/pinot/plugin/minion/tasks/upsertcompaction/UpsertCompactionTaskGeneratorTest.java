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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(Collections.emptyList()));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithConsumingSegment() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    SegmentZKMetadata consumingSegment = new SegmentZKMetadata("testTable__0");
    consumingSegment.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(consumingSegment));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }

  @Test
  public void testGenerateTasksWithNewlyCompletedSegment() {
    UpsertCompactionTaskGenerator taskGenerator = new UpsertCompactionTaskGenerator();
    Map<String, Map<String, String>> tableTaskConfigs = new HashMap<>();
    Map<String, String> compactionConfigs = new HashMap<>();
    tableTaskConfigs.put(UpsertCompactionTask.TASK_TYPE, compactionConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(tableTaskConfigs))
        .build();
    ClusterInfoAccessor mockClusterInfoAccessor = mock(ClusterInfoAccessor.class);
    SegmentZKMetadata completedSegment = new SegmentZKMetadata("testTable__0");
    completedSegment.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    Long endTime = System.currentTimeMillis();
    Long startTime = endTime - TimeUtils.convertPeriodToMillis("1d");
    completedSegment.setStartTime(startTime);
    completedSegment.setEndTime(endTime);
    completedSegment.setTimeUnit(TimeUnit.MILLISECONDS);
    when(mockClusterInfoAccessor.getSegmentsZKMetadata(REALTIME_TABLE_NAME))
        .thenReturn(Lists.newArrayList(completedSegment));
    taskGenerator.init(mockClusterInfoAccessor);

    List<PinotTaskConfig> pinotTaskConfigs = taskGenerator.generateTasks(Lists.newArrayList(tableConfig));

    assertEquals(pinotTaskConfigs.size(), 0);
  }
}
