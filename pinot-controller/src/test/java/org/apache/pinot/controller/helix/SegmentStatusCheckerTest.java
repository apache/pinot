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
package org.apache.pinot.controller.helix;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.SegmentStatusInfo;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("unchecked")
public class SegmentStatusCheckerTest {
  private static final String RAW_TABLE_NAME = "myTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  // Intentionally not reset the metrics to test all metrics being refreshed.
  private final ControllerMetrics _controllerMetrics =
      new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());

  @Test
  public void offlineBasicTest() {
    // Intentionally set the replication number to 2 to test the metrics.
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(2).build();

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot3", "OFFLINE");
    idealState.setPartitionState("myTable_3", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_3", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_3", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_4", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_4", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_4", "pinot3", "ONLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot1", "ERROR");
    externalView.setState("myTable_1", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot3", "ERROR");
    externalView.setState("myTable_3", "pinot1", "ERROR");
    externalView.setState("myTable_3", "pinot2", "ONLINE");
    externalView.setState("myTable_3", "pinot3", "ONLINE");
    externalView.setState("myTable_4", "pinot1", "ONLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    mockSegmentsZKMetadataForAllSegments(resourceManager, OFFLINE_TABLE_NAME, idealState, segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    // Based on the lineage entries: {myTable_1 -> myTable_3, COMPLETED}, {myTable_3 -> myTable_4, IN_PROGRESS},
    // myTable_1 and myTable_4 will be skipped for the metrics.
    SegmentLineage segmentLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(List.of("myTable_1"), List.of("myTable_3"), LineageEntryState.COMPLETED, 11111L));
    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(List.of("myTable_3"), List.of("myTable_4"), LineageEntryState.IN_PROGRESS, 11111L));
    when(
        propertyStore.get(eq("/SEGMENT_LINEAGE/" + OFFLINE_TABLE_NAME), any(), eq(AccessOption.PERSISTENT))).thenReturn(
        segmentLineage.toZNRecord());

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(OFFLINE_TABLE_NAME, 2, 5, 3, 2, 66, 1, 100, 2, 2468);
    // The metadata and the znode stats must come from batched reads: a per-segment read here costs two blocking
    // ZooKeeper round trips per segment, which takes minutes on tables with hundreds of thousands of segments.
    verify(resourceManager, never()).getSegmentZKMetadata(anyString(), anyString());
    verify(propertyStore, never()).getStat(anyString(), anyInt());
  }

  private SegmentZKMetadata mockPushedSegmentZKMetadata(long sizeInBytes, long pushTimeMs) {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getStatus()).thenReturn(Status.UPLOADED);
    when(segmentZKMetadata.getSizeInBytes()).thenReturn(sizeInBytes);
    when(segmentZKMetadata.getPushTime()).thenReturn(pushTimeMs);
    return segmentZKMetadata;
  }

  private void runSegmentStatusChecker(PinotHelixResourceManager resourceManager, int waitForPushTimeInSeconds) {
    runSegmentStatusChecker(resourceManager, waitForPushTimeInSeconds, mock(TableSizeReader.class));
  }

  private void runSegmentStatusChecker(PinotHelixResourceManager resourceManager, int waitForPushTimeInSeconds,
      TableSizeReader tableSizeReader) {
    runSegmentStatusChecker(buildSegmentStatusChecker(resourceManager, waitForPushTimeInSeconds, tableSizeReader));
  }

  private void runSegmentStatusChecker(SegmentStatusChecker segmentStatusChecker) {
    segmentStatusChecker.start();
    segmentStatusChecker.run();
  }

  @DataProvider(name = "compressionMetricLifecycle")
  public Object[][] compressionMetricLifecycle() {
    return new Object[][]{
        {TableType.OFFLINE, true, true, false},
        {TableType.REALTIME, true, true, false},
        {TableType.OFFLINE, false, true, false},
        {TableType.REALTIME, false, true, false},
        {TableType.OFFLINE, false, true, true},
        {TableType.REALTIME, true, false, true},
        {TableType.OFFLINE, true, true, true}
    };
  }

  @Test(dataProvider = "compressionMetricLifecycle")
  public void testCompressionMetricLifecycle(TableType tableType, boolean enabled, boolean tableSizePresent,
      boolean subtypeMissing)
      throws Exception {
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(tableType).setTableName(RAW_TABLE_NAME)
        .setCompressionStatsEnabled(enabled);
    if (tableType == TableType.REALTIME) {
      tableConfigBuilder.setTimeColumnName("timeColumn").setStreamConfigs(getStreamConfigMap());
    }
    TableConfig tableConfig = tableConfigBuilder.build();
    String tableNameWithType = tableType == TableType.OFFLINE ? OFFLINE_TABLE_NAME : REALTIME_TABLE_NAME;

    TableSizeReader.TableSubTypeSizeDetails subtypeDetails = new TableSizeReader.TableSubTypeSizeDetails();
    TableSizeReader.TableSizeDetails tableSizeDetails = null;
    if (tableSizePresent) {
      tableSizeDetails = new TableSizeReader.TableSizeDetails(RAW_TABLE_NAME);
      if (!subtypeMissing) {
        if (tableType == TableType.OFFLINE) {
          tableSizeDetails._offlineSegments = subtypeDetails;
        } else {
          tableSizeDetails._realtimeSegments = subtypeDetails;
        }
      }
    }

    TableSizeReader tableSizeReader = mock(TableSizeReader.class);
    when(tableSizeReader.getTableSizeDetails(tableNameWithType, 30_000, true,
        TableSizeReader.CompressionStatsMode.AGGREGATE_SUMMARY)).thenReturn(tableSizeDetails);
    SegmentStatusChecker checker = new SegmentStatusChecker(mock(PinotHelixResourceManager.class),
        mock(LeadControllerManager.class), mock(ControllerConf.class), _controllerMetrics, tableSizeReader);

    checker.updateTableSizeMetrics(tableNameWithType, tableConfig);

    verify(tableSizeReader).getTableSizeDetails(tableNameWithType, 30_000, true,
        TableSizeReader.CompressionStatsMode.AGGREGATE_SUMMARY);
    if (enabled && tableSizePresent && !subtypeMissing) {
      verify(tableSizeReader).updateCompressionMetrics(tableNameWithType, subtypeDetails);
      verify(tableSizeReader, never()).clearCompressionMetrics(tableNameWithType);
    } else {
      verify(tableSizeReader).clearCompressionMetrics(tableNameWithType);
      verify(tableSizeReader, never()).updateCompressionMetrics(anyString(), any());
    }
  }

  private void verifyControllerMetrics(String tableNameWithType, int expectedReplicationFromConfig,
      int expectedNumSegmentsIncludingReplaced, int expectedNumSegment, int expectedNumReplicas,
      int expectedPercentOfReplicas, int expectedSegmentsInErrorState, int expectedPercentSegmentsAvailable,
      int expectedSegmentsWithLessReplicas, int expectedTableCompressedSize) {
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.REPLICATION_FROM_CONFIG), expectedReplicationFromConfig);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.SEGMENT_COUNT_INCLUDING_REPLACED), expectedNumSegmentsIncludingReplaced);
    assertEquals(
        MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.SEGMENT_COUNT),
        expectedNumSegment);
    assertEquals(
        MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS),
        expectedNumReplicas);
    assertEquals(
        MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS),
        expectedPercentOfReplicas);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), expectedSegmentsInErrorState);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), expectedPercentSegmentsAvailable);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS), expectedSegmentsWithLessReplicas);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSED_SIZE), expectedTableCompressedSize);
  }

  @Test
  public void realtimeBasicTest() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap()).build();

    String seg1 = new LLCSegmentName(RAW_TABLE_NAME, 1, 0, System.currentTimeMillis()).getSegmentName();
    String seg2 = new LLCSegmentName(RAW_TABLE_NAME, 1, 1, System.currentTimeMillis()).getSegmentName();
    String seg3 = new LLCSegmentName(RAW_TABLE_NAME, 2, 1, System.currentTimeMillis()).getSegmentName();
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(seg1, "pinot1", "ONLINE");
    idealState.setPartitionState(seg1, "pinot2", "ONLINE");
    idealState.setPartitionState(seg1, "pinot3", "ONLINE");

    idealState.setPartitionState(seg2, "pinot1", "ONLINE");
    idealState.setPartitionState(seg2, "pinot2", "ONLINE");
    idealState.setPartitionState(seg2, "pinot3", "ONLINE");

    idealState.setPartitionState(seg3, "pinot1", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot2", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot3", "OFFLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(seg1, "pinot1", "ONLINE");
    externalView.setState(seg1, "pinot2", "ONLINE");
    externalView.setState(seg1, "pinot3", "ONLINE");

    externalView.setState(seg2, "pinot1", "CONSUMING");
    externalView.setState(seg2, "pinot2", "ONLINE");
    externalView.setState(seg2, "pinot3", "CONSUMING");

    externalView.setState(seg3, "pinot1", "CONSUMING");
    externalView.setState(seg3, "pinot2", "CONSUMING");
    externalView.setState(seg3, "pinot3", "OFFLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committedSegmentZKMetadata = mockCommittedSegmentZKMetadata();
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME,
        Map.of(seg1, committedSegmentZKMetadata, seg2, committedSegmentZKMetadata, seg3, consumingSegmentZKMetadata));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = new ZNRecord("0");
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
    when(propertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(REALTIME_TABLE_NAME, 3, 3, 3, 2, 66, 0, 100, 0, 0);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.MISSING_CONSUMING_SEGMENT_TOTAL_COUNT), 2);
  }

  @Test
  public void realtimeMutableSegmentHasLessReplicaTest() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap())
            .build();

    String seg1 = new LLCSegmentName(RAW_TABLE_NAME, 1, 0, System.currentTimeMillis()).getSegmentName();
    String seg2 = new LLCSegmentName(RAW_TABLE_NAME, 1, 1, System.currentTimeMillis()).getSegmentName();
    String seg3 = new LLCSegmentName(RAW_TABLE_NAME, 2, 1, System.currentTimeMillis()).getSegmentName();
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(seg1, "pinot1", "ONLINE");
    idealState.setPartitionState(seg1, "pinot2", "ONLINE");
    idealState.setPartitionState(seg1, "pinot3", "ONLINE");

    idealState.setPartitionState(seg2, "pinot1", "ONLINE");
    idealState.setPartitionState(seg2, "pinot2", "ONLINE");
    idealState.setPartitionState(seg2, "pinot3", "ONLINE");

    idealState.setPartitionState(seg3, "pinot1", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot2", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot3", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot4", "OFFLINE");

    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(seg1, "pinot1", "ONLINE");
    externalView.setState(seg1, "pinot2", "ONLINE");
    externalView.setState(seg1, "pinot3", "ONLINE");

    externalView.setState(seg2, "pinot1", "CONSUMING");
    externalView.setState(seg2, "pinot2", "ONLINE");
    externalView.setState(seg2, "pinot3", "CONSUMING");
    externalView.setState(seg2, "pinot4", "CONSUMING");

    externalView.setState(seg3, "pinot1", "CONSUMING");
    externalView.setState(seg3, "pinot2", "CONSUMING");
    externalView.setState(seg3, "pinot3", "CONSUMING");
    externalView.setState(seg3, "pinot4", "OFFLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committedSegmentZKMetadata = mockCommittedSegmentZKMetadata();
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME,
        Map.of(seg1, committedSegmentZKMetadata, seg2, committedSegmentZKMetadata, seg3, consumingSegmentZKMetadata));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = new ZNRecord("0");
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
    when(propertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(REALTIME_TABLE_NAME, 3, 3, 3, 3, 75, 0, 100, 0, 0);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.MISSING_CONSUMING_SEGMENT_TOTAL_COUNT), 2);
  }

  @Test
  public void realtimeServerNotQueryableTest() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap())
            .build();

    String seg1 = new LLCSegmentName(RAW_TABLE_NAME, 1, 0, System.currentTimeMillis()).getSegmentName();
    String seg2 = new LLCSegmentName(RAW_TABLE_NAME, 1, 1, System.currentTimeMillis()).getSegmentName();
    String seg3 = new LLCSegmentName(RAW_TABLE_NAME, 2, 1, System.currentTimeMillis()).getSegmentName();
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(seg1, "Server_pinot1", "ONLINE");
    idealState.setPartitionState(seg1, "Server_pinot2", "ONLINE");
    idealState.setPartitionState(seg1, "Server_pinot3", "ONLINE");

    idealState.setPartitionState(seg2, "Server_pinot1", "ONLINE");
    idealState.setPartitionState(seg2, "Server_pinot2", "ONLINE");
    idealState.setPartitionState(seg2, "Server_pinot3", "ONLINE");

    idealState.setPartitionState(seg3, "Server_pinot1", "CONSUMING");
    idealState.setPartitionState(seg3, "Server_pinot2", "CONSUMING");
    idealState.setPartitionState(seg3, "Server_pinot3", "CONSUMING");
    idealState.setPartitionState(seg3, "Server_pinot4", "OFFLINE");

    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(seg1, "Server_pinot1", "ONLINE");
    externalView.setState(seg1, "Server_pinot2", "ONLINE");
    externalView.setState(seg1, "Server_pinot3", "ONLINE");

    externalView.setState(seg2, "Server_pinot1", "CONSUMING");
    externalView.setState(seg2, "Server_pinot2", "ONLINE");
    externalView.setState(seg2, "Server_pinot3", "CONSUMING");
    externalView.setState(seg2, "Server_pinot4", "CONSUMING");

    externalView.setState(seg3, "Server_pinot1", "CONSUMING");
    externalView.setState(seg3, "Server_pinot2", "CONSUMING");
    externalView.setState(seg3, "Server_pinot3", "CONSUMING");
    externalView.setState(seg3, "Server_pinot4", "OFFLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig("Server_pinot1")).
        thenReturn(newQueryDisabledInstanceConfig("Server_pinot1"));
    when(resourceManager.getHelixInstanceConfig("Server_pinot2")).
        thenReturn(newShutdownInProgressInstanceConfig("Server_pinot2"));
    when(resourceManager.getHelixInstanceConfig("Server_pinot3")).
        thenReturn(newQuerableInstanceConfig("Server_pinot3"));
    when(resourceManager.getHelixInstanceConfig("Server_pinot4")).
        thenReturn(newQuerableInstanceConfig("Server_pinot4"));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committedSegmentZKMetadata = mockCommittedSegmentZKMetadata();
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME,
        Map.of(seg1, committedSegmentZKMetadata, seg2, committedSegmentZKMetadata, seg3, consumingSegmentZKMetadata));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = new ZNRecord("0");
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
    when(propertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(REALTIME_TABLE_NAME, 3, 3, 3, 1, 25, 0, 100, 3, 0);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.MISSING_CONSUMING_SEGMENT_TOTAL_COUNT), 2);
  }

  private InstanceConfig newQueryDisabledInstanceConfig(String instanceName) {
    ZNRecord znRecord = new ZNRecord(instanceName);
    znRecord.setBooleanField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), true);
    znRecord.setBooleanField(CommonConstants.Helix.QUERIES_DISABLED, true);
    return new InstanceConfig(znRecord);
  }

  private InstanceConfig newShutdownInProgressInstanceConfig(String instanceName) {
    ZNRecord znRecord = new ZNRecord(instanceName);
    znRecord.setBooleanField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), true);
    znRecord.setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
    return new InstanceConfig(znRecord);
  }

  private InstanceConfig newQuerableInstanceConfig(String instanceName) {
    ZNRecord znRecord = new ZNRecord(instanceName);
    znRecord.setBooleanField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), true);
    return new InstanceConfig(znRecord);
  }

  @Test
  public void realtimeImmutableSegmentHasLessReplicaTest() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap())
            .build();

    String seg1 = new LLCSegmentName(RAW_TABLE_NAME, 1, 0, System.currentTimeMillis()).getSegmentName();
    String seg2 = new LLCSegmentName(RAW_TABLE_NAME, 1, 1, System.currentTimeMillis()).getSegmentName();
    String seg3 = new LLCSegmentName(RAW_TABLE_NAME, 2, 1, System.currentTimeMillis()).getSegmentName();
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(seg1, "pinot1", "ONLINE");
    idealState.setPartitionState(seg1, "pinot2", "ONLINE");
    idealState.setPartitionState(seg1, "pinot3", "ONLINE");

    idealState.setPartitionState(seg2, "pinot1", "ONLINE");
    idealState.setPartitionState(seg2, "pinot2", "ONLINE");
    idealState.setPartitionState(seg2, "pinot3", "ONLINE");

    idealState.setPartitionState(seg3, "pinot1", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot2", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot3", "CONSUMING");
    idealState.setPartitionState(seg3, "pinot4", "OFFLINE");

    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(seg1, "pinot1", "ONLINE");
    externalView.setState(seg1, "pinot2", "ONLINE");
    externalView.setState(seg1, "pinot3", "OFFLINE");

    externalView.setState(seg2, "pinot1", "CONSUMING");
    externalView.setState(seg2, "pinot2", "ONLINE");
    externalView.setState(seg2, "pinot3", "CONSUMING");
    externalView.setState(seg2, "pinot4", "CONSUMING");

    externalView.setState(seg3, "pinot1", "CONSUMING");
    externalView.setState(seg3, "pinot2", "CONSUMING");
    externalView.setState(seg3, "pinot3", "CONSUMING");
    externalView.setState(seg3, "pinot4", "OFFLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committedSegmentZKMetadata = mockCommittedSegmentZKMetadata();
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME,
        Map.of(seg1, committedSegmentZKMetadata, seg2, committedSegmentZKMetadata, seg3, consumingSegmentZKMetadata));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = new ZNRecord("0");
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
    when(propertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(REALTIME_TABLE_NAME, 3, 3, 3, 2, 66, 0, 100, 1, 0);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.MISSING_CONSUMING_SEGMENT_TOTAL_COUNT), 2);
  }

  private Map<String, String> getStreamConfigMap() {
    return Map.of("streamType", "kafka", "stream.kafka.topic.name", "test", "stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaAvroMessageDecoder", "stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory");
  }

  private SegmentZKMetadata mockCommittedSegmentZKMetadata() {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getStatus()).thenReturn(Status.DONE);
    when(segmentZKMetadata.getSizeInBytes()).thenReturn(-1L);
    when(segmentZKMetadata.getPushTime()).thenReturn(Long.MIN_VALUE);
    return segmentZKMetadata;
  }

  private SegmentZKMetadata mockConsumingSegmentZKMetadata(long creationTimeMs) {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getStatus()).thenReturn(Status.IN_PROGRESS);
    when(segmentZKMetadata.getSizeInBytes()).thenReturn(-1L);
    when(segmentZKMetadata.getCreationTime()).thenReturn(creationTimeMs);
    return segmentZKMetadata;
  }

  // A pauseless COMMITTING segment: done consuming but its immutable segment is still being built/loaded on the
  // replicas. The grace check keys off the segment znode's modification time (mtime), which the test drives via
  // mockSegmentsZKMetadata(...); the metadata here only supplies status and size.
  private SegmentZKMetadata mockCommittingSegmentZKMetadata() {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getStatus()).thenReturn(Status.COMMITTING);
    when(segmentZKMetadata.getSizeInBytes()).thenReturn(-1L);
    return segmentZKMetadata;
  }

  // A ZK Stat whose modification time (mtime) is set to the given epoch millis, used to drive the grace-window check.
  private Stat mockStatWithMTime(long mTimeMs) {
    Stat stat = new Stat();
    stat.setMtime(mTimeMs);
    return stat;
  }

  /// Stubs the single batched segment read with the metadata of `segmentZKMetadataMap` (segment name -> metadata) and
  /// the znode modification times of `segmentZNodeMTimesMs` (segment name -> mtime in epoch millis), preserving the
  /// index alignment with the requested segment names that [SegmentStatusChecker] relies on. A segment missing from
  /// `segmentZKMetadataMap` reads back as having no ZK metadata; one missing from `segmentZNodeMTimesMs` reads back
  /// with no znode stat, which makes the checker fall back to the metadata's creation time.
  private void mockSegmentsZKMetadata(PinotHelixResourceManager resourceManager, String tableNameWithType,
      Map<String, SegmentZKMetadata> segmentZKMetadataMap, Map<String, Long> segmentZNodeMTimesMs) {
    when(resourceManager.getSegmentsZKMetadata(eq(tableNameWithType), any(), any())).thenAnswer(
        invocation -> {
          List<String> segmentNames = invocation.getArgument(1);
          List<Stat> stats = invocation.getArgument(2);
          List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>(segmentNames.size());
          for (String segmentName : segmentNames) {
            segmentsZKMetadata.add(segmentZKMetadataMap.get(segmentName));
            if (stats != null) {
              Long mTimeMs = segmentZNodeMTimesMs.get(segmentName);
              stats.add(mTimeMs != null ? mockStatWithMTime(mTimeMs) : null);
            }
          }
          return segmentsZKMetadata;
        });
  }

  /// Stubs the single batched segment read with metadata only, so that every segment reads back without a znode stat
  /// and the checker falls back to the metadata's creation time
  /// (see [#mockSegmentsZKMetadata(PinotHelixResourceManager, String, Map, Map)]).
  private void mockSegmentsZKMetadata(PinotHelixResourceManager resourceManager, String tableNameWithType,
      Map<String, SegmentZKMetadata> segmentZKMetadataMap) {
    mockSegmentsZKMetadata(resourceManager, tableNameWithType, segmentZKMetadataMap, Map.of());
  }

  /// Stubs the single batched segment read so that every segment of `idealState` resolves to `segmentZKMetadata`
  /// (see [#mockSegmentsZKMetadata(PinotHelixResourceManager, String, Map)]).
  private void mockSegmentsZKMetadataForAllSegments(PinotHelixResourceManager resourceManager,
      String tableNameWithType, IdealState idealState, SegmentZKMetadata segmentZKMetadata) {
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (String segmentName : idealState.getPartitionSet()) {
      segmentZKMetadataMap.put(segmentName, segmentZKMetadata);
    }
    mockSegmentsZKMetadata(resourceManager, tableNameWithType, segmentZKMetadataMap);
  }

  /// A pauseless COMMITTING segment whose replicas are still building (only 1/3 ONLINE in the external view) must not
  /// be counted as under-replicated while it is within the grace window, so percentOfReplicas stays at 100. Regression
  /// test for the SegmentReplicasCriticallyLowForHATable false positive on pauseless tables.
  @Test
  public void realtimeCommittingSegmentWithinGraceNotUnderReplicated() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap()).build();

    String seg = new LLCSegmentName(RAW_TABLE_NAME, 1, 5, System.currentTimeMillis()).getSegmentName();
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(seg, "pinot1", "ONLINE");
    idealState.setPartitionState(seg, "pinot2", "ONLINE");
    idealState.setPartitionState(seg, "pinot3", "ONLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    // Just committed: only 1 of 3 replicas ONLINE, the other two still building the immutable segment.
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(seg, "pinot1", "ONLINE");
    externalView.setState(seg, "pinot2", "OFFLINE");
    externalView.setState(seg, "pinot3", "OFFLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committingSegmentZKMetadata = mockCommittingSegmentZKMetadata();
    // Just committed: znode mtime is now, within the grace window.
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME, Map.of(seg, committingSegmentZKMetadata),
        Map.of(seg, System.currentTimeMillis()));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = new ZNRecord("0");
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
    when(propertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);

    // 1h grace window; the segment was just created, so it must be skipped and the table stays fully replicated.
    runSegmentStatusChecker(resourceManager, 3600);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS), 0);
  }

  /// When a segment's znode stat is unavailable, the grace window falls back to the metadata's creation time. A freshly
  /// created CONSUMING segment (mtime == creation time) must therefore still be graced, so a lost stat cannot turn into
  /// a false under-replication alert.
  @Test
  public void realtimeConsumingSegmentWithoutZNodeStatFallsBackToCreationTime() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap()).build();

    String seg = new LLCSegmentName(RAW_TABLE_NAME, 1, 5, System.currentTimeMillis()).getSegmentName();
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(seg, "pinot1", "CONSUMING");
    idealState.setPartitionState(seg, "pinot2", "CONSUMING");
    idealState.setPartitionState(seg, "pinot3", "CONSUMING");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    // Just created: only 1 of 3 replicas has started consuming.
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(seg, "pinot1", "CONSUMING");
    externalView.setState(seg, "pinot2", "OFFLINE");
    externalView.setState(seg, "pinot3", "OFFLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    // Creation time is now, and no znode stat is available -> the fallback must keep the segment within the grace
    // window
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(System.currentTimeMillis());
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME, Map.of(seg, consumingSegmentZKMetadata));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = new ZNRecord("0");
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
    when(propertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);

    // 1h grace window; the segment was just created, so it must be skipped and the table stays fully replicated.
    runSegmentStatusChecker(resourceManager, 3600);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS), 0);
  }

  @DataProvider(name = "segmentMetadataBatchSizes")
  public Object[][] segmentMetadataBatchSizes() {
    // Batch size (null leaves the production default, under which the whole table fits in one batch) and the number of
    // reads the 7 segments must then take. 3 does not divide 7 evenly, so the last batch is a partial one.
    return new Object[][]{
        {null, 1},
        {3, 3}
    };
  }

  /// The metadata and znode stats come back from batched reads, so each segment must be matched to its own metadata
  /// (by name) and its own stat (by position) rather than to whichever entry happens to sit at its index, whether the
  /// table is read in one batch or in several. Every segment is under-replicated and carries a distinct size, exactly
  /// one segment was pushed recently enough to be graced, and the last segment is the only one with 4 replicas so it
  /// alone determines PERCENT_OF_REPLICAS. Together the gauges pin that every segment was examined under its own name,
  /// that the grace window applied to exactly the segment whose znode stat is recent, and that the sizes accumulate.
  /// One segment has no ZK metadata, which shifts the alignment if the pairing gets it wrong.
  @Test(dataProvider = "segmentMetadataBatchSizes")
  public void segmentsStayAlignedWithTheirBatchedMetadata(Integer segmentMetadataBatchSize, int expectedNumBatches) {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(2).build();

    int numSegments = 7;
    // Distinct positions, none of them the last segment, which carries its own marker below. With a batch size of 3 the
    // segment without ZK metadata also starts a batch, so a boundary that shifted the pairing drops the wrong segment.
    int segmentWithoutZKMetadata = 3;
    int recentlyPushedSegment = 1;
    // The last segment is the marker that pins name-to-metadata pairing: it is the only one whose replica ratio is 1/4
    // rather than 1/2, so PERCENT_OF_REPLICAS drops to 25 only if this exact segment was examined.
    int lowReplicaSegment = numSegments - 1;
    long oldPushTimeMs = 11111L;

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    Map<String, Long> segmentZNodeMTimesMs = new HashMap<>();
    long expectedTableCompressedSize = 0;
    for (int i = 0; i < numSegments; i++) {
      String segment = "myTable_" + i;
      int numReplicas = i == lowReplicaSegment ? 4 : 2;
      // Every segment is under-replicated, so any segment that is examined and not graced must be counted
      for (int replica = 1; replica <= numReplicas; replica++) {
        idealState.setPartitionState(segment, "pinot" + replica, "ONLINE");
        externalView.setState(segment, "pinot" + replica, replica == 1 ? "ONLINE" : "OFFLINE");
      }
      if (i == segmentWithoutZKMetadata) {
        continue;
      }
      // Distinct size per segment so that the total pins which metadata was attributed to which segment
      long sizeInBytes = 1000L + i;
      segmentZKMetadataMap.put(segment, mockPushedSegmentZKMetadata(sizeInBytes, oldPushTimeMs));
      segmentZNodeMTimesMs.put(segment, i == recentlyPushedSegment ? System.currentTimeMillis() : oldPushTimeMs);
      expectedTableCompressedSize += sizeInBytes;
    }
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    mockSegmentsZKMetadata(resourceManager, OFFLINE_TABLE_NAME, segmentZKMetadataMap, segmentZNodeMTimesMs);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    // 10min grace window, so only the recently pushed segment is skipped
    SegmentStatusChecker segmentStatusChecker =
        buildSegmentStatusChecker(resourceManager, 600, mock(TableSizeReader.class));
    if (segmentMetadataBatchSize != null) {
      segmentStatusChecker._segmentMetadataBatchSize = segmentMetadataBatchSize;
    }
    runSegmentStatusChecker(segmentStatusChecker);

    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENT_COUNT), numSegments);
    // Every segment except the one without ZK metadata and the graced one must be counted. A segment paired with the
    // wrong metadata or the wrong znode stat drops out of, or into, this count.
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS), numSegments - 2);
    // Only the last segment has a 1-of-4 ratio, so this is 25 only if that segment was examined under its own name
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.PERCENT_OF_REPLICAS), 25);
    // The sizes accumulate over exactly the segments that have metadata (including the graced one, whose size is
    // counted before the grace check)
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.TABLE_COMPRESSED_SIZE), expectedTableCompressedSize);

    // The reads must be batched, metadata and znode stats together: one request per batch and no more, otherwise the
    // per-segment reads crept back in some form
    ArgumentCaptor<List<String>> segmentNamesCaptor = ArgumentCaptor.forClass(List.class);
    verify(resourceManager, times(expectedNumBatches)).getSegmentsZKMetadata(eq(OFFLINE_TABLE_NAME),
        segmentNamesCaptor.capture(), any());
    verify(propertyStore, never()).getStats(any(), anyInt());
    // Every segment is requested exactly once, in batches of at most the batch size
    List<String> requestedSegments = new ArrayList<>();
    for (List<String> batch : segmentNamesCaptor.getAllValues()) {
      assertTrue(segmentMetadataBatchSize == null || batch.size() <= segmentMetadataBatchSize,
          "batch of " + batch.size() + " segments");
      requestedSegments.addAll(batch);
    }
    assertEquals(requestedSegments.size(), numSegments);
    assertEquals(new HashSet<>(requestedSegments), idealState.getPartitionSet());
  }

  /// When not a single segment's ZK metadata can be read the table's gauges must be left alone rather than reset to
  /// all-green values, because an all-green gauge silences the alerts that a stale one would still fire. Regression
  /// test for a whole-table ZK read failure being reported as a perfectly healthy table. This supersedes the former
  /// `noSegmentZKMetadataTest`, which expected the all-green gauges for the same scenario.
  @Test
  public void tableWithoutAnyReadableSegmentZKMetadataKeepsItsGauges() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(2).build();

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    for (int i = 0; i < 3; i++) {
      String segment = "myTable_" + i;
      idealState.setPartitionState(segment, "pinot1", "ONLINE");
      idealState.setPartitionState(segment, "pinot2", "ONLINE");
      // Every segment is under-replicated, so a metric update that went ahead would be visibly wrong
      externalView.setState(segment, "pinot1", "ONLINE");
      externalView.setState(segment, "pinot2", "OFFLINE");
    }
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    // No segment resolves to metadata, as if every znode read had failed
    mockSegmentsZKMetadata(resourceManager, OFFLINE_TABLE_NAME, Map.of());

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    // Sentinels standing in for what a previous, successful cycle had published. None of them is a value this table
    // could legitimately produce, so any of them being overwritten means the checker went ahead on unreadable metadata.
    List<ControllerGauge> segmentHealthGauges =
        List.of(ControllerGauge.PERCENT_OF_REPLICAS, ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS,
            ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, ControllerGauge.SEGMENTS_IN_ERROR_STATE,
            ControllerGauge.TABLE_COMPRESSED_SIZE);
    for (ControllerGauge gauge : segmentHealthGauges) {
      _controllerMetrics.setValueOfTableGauge(OFFLINE_TABLE_NAME, gauge, -1);
    }

    runSegmentStatusChecker(resourceManager, 600);

    // SEGMENT_COUNT is published from the ideal state before the read, so it is still updated
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENT_COUNT), 3);
    for (ControllerGauge gauge : segmentHealthGauges) {
      assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME, gauge), -1,
          gauge.getGaugeName());
    }

    // The metrics are shared by every test in this class, so do not leave the sentinels behind
    for (ControllerGauge gauge : segmentHealthGauges) {
      _controllerMetrics.removeTableGauge(OFFLINE_TABLE_NAME, gauge);
    }
  }

  /// A COMMITTING segment that has been under-replicated for longer than the grace window is a genuinely stuck commit
  /// and must still be flagged (percentOfReplicas drops), so the grace does not mask real problems.
  @Test
  public void realtimeCommittingSegmentBeyondGraceUnderReplicated() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap()).build();

    String seg = new LLCSegmentName(RAW_TABLE_NAME, 1, 5, System.currentTimeMillis()).getSegmentName();
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState(seg, "pinot1", "ONLINE");
    idealState.setPartitionState(seg, "pinot2", "ONLINE");
    idealState.setPartitionState(seg, "pinot3", "ONLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    externalView.setState(seg, "pinot1", "ONLINE");
    externalView.setState(seg, "pinot2", "OFFLINE");
    externalView.setState(seg, "pinot3", "OFFLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committingSegmentZKMetadata = mockCommittingSegmentZKMetadata();
    // Committed 2h ago (znode mtime), still under-replicated -> a stuck commit, must not be graced.
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME, Map.of(seg, committingSegmentZKMetadata),
        Map.of(seg, System.currentTimeMillis() - 7200000L));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = new ZNRecord("0");
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
    when(propertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);

    // 1h grace window; the segment is 2h old and still 1/3 replicas up, so it must be flagged (33%).
    runSegmentStatusChecker(resourceManager, 3600);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.PERCENT_OF_REPLICAS), 33);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS), 1);
  }

  @Test
  public void missingEVPartitionTest() {
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot3", "OFFLINE");
    idealState.setPartitionState("myTable_3", "pinot3", "ONLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot1", "ERROR");
    externalView.setState("myTable_1", "pinot2", "ONLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    mockSegmentsZKMetadataForAllSegments(resourceManager, OFFLINE_TABLE_NAME, idealState, segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(OFFLINE_TABLE_NAME, 0, 4, 4, 0, 0, 1, 75, 2, 3702);
  }

  @Test
  public void missingEVTest() {
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot3", "ONLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    mockSegmentsZKMetadataForAllSegments(resourceManager, OFFLINE_TABLE_NAME, idealState, segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(OFFLINE_TABLE_NAME, 0, 2, 2, 0, 0, 0, 0, 0, 2468);
  }

  @Test
  public void missingIdealTest() {
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetricsNotExist();
  }

  private void verifyControllerMetricsNotExist() {
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.REPLICATION_FROM_CONFIG), 0);
    assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENT_COUNT_INCLUDING_REPLACED));
    assertFalse(
        MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME, ControllerGauge.SEGMENT_COUNT));
    assertFalse(
        MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME, ControllerGauge.NUMBER_OF_REPLICAS));
    assertFalse(
        MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME, ControllerGauge.PERCENT_OF_REPLICAS));
    assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE));
    assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE));
    assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS));
    assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.TABLE_COMPRESSED_SIZE));
  }

  @Test
  public void missingEVPartitionPushTest() {
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot2", "ONLINE");
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot1", "ONLINE");
    externalView.setState("myTable_1", "pinot2", "ONLINE");
    // myTable_2 is push in-progress and only one replica has been downloaded by servers. It will be skipped for
    // the segment status check.
    externalView.setState("myTable_2", "pinot1", "ONLINE");

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata01 = mockPushedSegmentZKMetadata(1234, 11111L);
    SegmentZKMetadata segmentZKMetadata2 = mockPushedSegmentZKMetadata(1234, System.currentTimeMillis());
    // myTable_2 was just pushed (znode mtime is now) so it is within the grace window and skipped; the others were
    // pushed long ago.
    mockSegmentsZKMetadata(resourceManager, OFFLINE_TABLE_NAME,
        Map.of("myTable_0", segmentZKMetadata01, "myTable_1", segmentZKMetadata01, "myTable_2", segmentZKMetadata2),
        Map.of("myTable_0", 11111L, "myTable_1", 11111L, "myTable_2", System.currentTimeMillis()));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 600);
    verifyControllerMetrics(OFFLINE_TABLE_NAME, 0, 3, 3, 2, 100, 0, 100, 0, 3702);
  }

  @Test
  public void missingEVUploadedConsumingTest() {
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "CONSUMING");
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    SegmentZKMetadata updatedSegmentZKMetadata = mockPushedSegmentZKMetadata(1234, System.currentTimeMillis());
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(System.currentTimeMillis());
    // Both segments were just updated/created (znode mtime is now), so they are within the grace window and skipped.
    mockSegmentsZKMetadata(resourceManager, REALTIME_TABLE_NAME,
        Map.of("myTable_0", updatedSegmentZKMetadata, "myTable_1", consumingSegmentZKMetadata),
        Map.of("myTable_0", System.currentTimeMillis(), "myTable_1", System.currentTimeMillis()));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 600);
    verifyControllerMetrics(REALTIME_TABLE_NAME, 0, 2, 2, 1, 100, 0, 100, 0, 1234);
  }

  @Test
  public void noReplicaTest() {
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "OFFLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "OFFLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "OFFLINE");
    idealState.setReplicas("0");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(null);
    SegmentZKMetadata segmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    mockSegmentsZKMetadataForAllSegments(resourceManager, REALTIME_TABLE_NAME, idealState, segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(REALTIME_TABLE_NAME, 0, 1, 1, 1, 100, 0, 100, 0, 0);
  }

  @Test
  public void disabledTableTest()
      throws Exception {
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    // disable table in idealstate
    idealState.enable(false);
    idealState.setPartitionState("myTable_OFFLINE", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_OFFLINE", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_OFFLINE", "pinot3", "ONLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);

    TableSizeReader tableSizeReader = mock(TableSizeReader.class);
    runSegmentStatusChecker(resourceManager, 0, tableSizeReader);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerMetrics, ControllerGauge.DISABLED_TABLE_COUNT), 1);
    verifyControllerMetricsNotExist();
    verify(tableSizeReader, never()).getTableSizeDetails(anyString(), anyInt(), anyBoolean(),
        any(TableSizeReader.CompressionStatsMode.class));
  }

  @Test
  public void noSegmentTest() {
    noSegmentTest(0);
    noSegmentTest(5);
    noSegmentTest(-1);
  }

  public void noSegmentTest(int numReplicas) {
    String numReplicasStr = numReplicas >= 0 ? Integer.toString(numReplicas) : "abc";
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setReplicas(numReplicasStr);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);

    runSegmentStatusChecker(resourceManager, 0);
    int expectedNumReplicas = Math.max(numReplicas, 1);
    verifyControllerMetrics(OFFLINE_TABLE_NAME, 0, 0, 0, expectedNumReplicas, 100, 0, 100, 0, 0);
  }

  @Test
  public void lessThanOnePercentSegmentsUnavailableTest() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME).setNumReplicas(1).build();

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    int numSegments = 200;
    for (int i = 0; i < numSegments; i++) {
      idealState.setPartitionState("myTable_" + i, "pinot1", "ONLINE");
    }
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState("myTable_0", "pinot1", "OFFLINE");
    for (int i = 1; i < numSegments; i++) {
      externalView.setState("myTable_" + i, "pinot1", "ONLINE");
    }

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getHelixInstanceConfig(any())).thenReturn(newQuerableInstanceConfig("any"));
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    mockSegmentsZKMetadataForAllSegments(resourceManager, OFFLINE_TABLE_NAME, idealState, segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(OFFLINE_TABLE_NAME, 1, numSegments, numSegments, 0, 0, 0, 99, 0, 246800);
  }

  @Test
  public void testAllSegmentsGoodOnlineOfflineTable() {
    TableViews.TableView tableViewExternal = new TableViews.TableView();
    TableViews.TableView tableViewIdeal = new TableViews.TableView();
    Map<String, Map<String, String>> tableViewExternalOffline = new TreeMap<>();
    Map<String, Map<String, String>> tableViewIdealOffline = new TreeMap<>();
    Map<String, String> testSegment1MapExternal = new LinkedHashMap<>();
    testSegment1MapExternal.put("Server1", "ONLINE");
    tableViewExternalOffline.put("TestSegment1", testSegment1MapExternal);
    tableViewExternalOffline.put("TestSegment2", testSegment1MapExternal);
    Map<String, String> testSegment1MapIdeal = new LinkedHashMap<>();
    testSegment1MapIdeal.put("Server1", "ONLINE");
    tableViewIdealOffline.put("TestSegment1", testSegment1MapIdeal);
    tableViewIdealOffline.put("TestSegment2", testSegment1MapIdeal);
    tableViewExternal._offline = tableViewExternalOffline;
    tableViewIdeal._offline = tableViewIdealOffline;
    TableViews tableviews = new TableViews();
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(
        tableviews.getStateMap(tableViewExternal), tableviews.getStateMap(tableViewIdeal));
    assertEquals(segmentStatusInfos.get(0).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD);
    assertEquals(segmentStatusInfos.get(1).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD);
  }

  @Test
  public void testAllSegmentsGoodConsumingOfflineTable() {
    TableViews.TableView tableViewExternal = new TableViews.TableView();
    TableViews.TableView tableViewIdeal = new TableViews.TableView();
    Map<String, Map<String, String>> tableViewExternalOffline = new TreeMap<>();
    Map<String, Map<String, String>> tableViewIdealOffline = new TreeMap<>();
    Map<String, String> testSegment1MapExternal = new LinkedHashMap<>();
    testSegment1MapExternal.put("Server1", "CONSUMING");
    tableViewExternalOffline.put("TestSegment1", testSegment1MapExternal);
    tableViewExternalOffline.put("TestSegment2", testSegment1MapExternal);
    Map<String, String> testSegment1MapIdeal = new LinkedHashMap<>();
    testSegment1MapIdeal.put("Server1", "CONSUMING");
    tableViewIdealOffline.put("TestSegment1", testSegment1MapIdeal);
    tableViewIdealOffline.put("TestSegment2", testSegment1MapIdeal);
    tableViewExternal._offline = tableViewExternalOffline;
    tableViewIdeal._offline = tableViewIdealOffline;
    TableViews tableviews = new TableViews();
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(
        tableviews.getStateMap(tableViewExternal), tableviews.getStateMap(tableViewIdeal));
    assertEquals(segmentStatusInfos.get(0).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD);
    assertEquals(segmentStatusInfos.get(1).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD);
  }

  @Test
  public void testAllSegmentsBadOfflineTable() {
    TableViews.TableView tableViewExternal = new TableViews.TableView();
    TableViews.TableView tableViewIdeal = new TableViews.TableView();
    Map<String, Map<String, String>> tableViewExternalOffline = new TreeMap<>();
    Map<String, Map<String, String>> tableViewIdealOffline = new TreeMap<>();
    Map<String, String> testSegment1MapExternal = new LinkedHashMap<>();
    testSegment1MapExternal.put("Server1", "ERROR");
    tableViewExternalOffline.put("TestSegment1", testSegment1MapExternal);
    tableViewExternalOffline.put("TestSegment2", testSegment1MapExternal);
    Map<String, String> testSegment1MapIdeal = new LinkedHashMap<>();
    testSegment1MapIdeal.put("Server1", "ONLINE");
    tableViewIdealOffline.put("TestSegment1", testSegment1MapIdeal);
    tableViewIdealOffline.put("TestSegment2", testSegment1MapIdeal);
    tableViewExternal._offline = tableViewExternalOffline;
    tableViewIdeal._offline = tableViewIdealOffline;
    TableViews tableviews = new TableViews();
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(
        tableviews.getStateMap(tableViewExternal), tableviews.getStateMap(tableViewIdeal));
    assertEquals(segmentStatusInfos.get(0).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD);
    assertEquals(segmentStatusInfos.get(1).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD);
  }

  @Test
  public void testAllSegmentsUpdatingOfflineTable() {
    TableViews.TableView tableViewExternal = new TableViews.TableView();
    TableViews.TableView tableViewIdeal = new TableViews.TableView();
    Map<String, Map<String, String>> tableViewExternalOffline = new TreeMap<>();
    Map<String, Map<String, String>> tableViewIdealOffline = new TreeMap<>();
    Map<String, String> testSegment1MapExternal = new LinkedHashMap<>();
    testSegment1MapExternal.put("Server1", "OFFLINE");
    tableViewExternalOffline.put("TestSegment1", testSegment1MapExternal);
    tableViewExternalOffline.put("TestSegment2", testSegment1MapExternal);
    Map<String, String> testSegment1MapIdeal = new LinkedHashMap<>();
    testSegment1MapIdeal.put("Server1", "ONLINE");
    tableViewIdealOffline.put("TestSegment1", testSegment1MapIdeal);
    tableViewIdealOffline.put("TestSegment2", testSegment1MapIdeal);
    tableViewExternal._offline = tableViewExternalOffline;
    tableViewIdeal._offline = tableViewIdealOffline;
    TableViews tableviews = new TableViews();
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(
        tableviews.getStateMap(tableViewExternal), tableviews.getStateMap(tableViewIdeal));
    assertEquals(segmentStatusInfos.get(0).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING);
    assertEquals(segmentStatusInfos.get(1).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING);
  }

  @Test
  public void testAllSegmentsGoodBadOfflineTable() {
    TableViews.TableView tableViewExternal = new TableViews.TableView();
    TableViews.TableView tableViewIdeal = new TableViews.TableView();
    Map<String, Map<String, String>> tableViewExternalOffline = new TreeMap<>();
    Map<String, Map<String, String>> tableViewIdealOffline = new TreeMap<>();
    Map<String, String> testSegment1MapExternal = new LinkedHashMap<>();
    Map<String, String> testSegment2MapExternal = new LinkedHashMap<>();
    testSegment1MapExternal.put("Server1", "OFFLINE");
    testSegment2MapExternal.put("Server2", "ERROR");
    tableViewExternalOffline.put("TestSegment1", testSegment1MapExternal);
    tableViewExternalOffline.put("TestSegment2", testSegment2MapExternal);
    Map<String, String> testSegment1MapIdeal = new LinkedHashMap<>();
    testSegment1MapIdeal.put("Server1", "OFFLINE");
    Map<String, String> testSegment2MapIdeal = new LinkedHashMap<>();
    testSegment2MapIdeal.put("Server2", "ERROR");
    tableViewIdealOffline.put("TestSegment1", testSegment1MapIdeal);
    tableViewIdealOffline.put("TestSegment2", testSegment2MapIdeal);
    tableViewExternal._offline = tableViewExternalOffline;
    tableViewIdeal._offline = tableViewIdealOffline;
    TableViews tableviews = new TableViews();
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(
        tableviews.getStateMap(tableViewExternal), tableviews.getStateMap(tableViewIdeal));
    assertEquals(segmentStatusInfos.get(0).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD);
    assertEquals(segmentStatusInfos.get(1).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD);
  }

  @Test
  public void testJsonDeserializationSegmentStatusInfo()
      throws Exception {
    // JSON string representing SchemaInfo
    String json = "[\n" + "  {\n" + "    \"segmentStatus\": \"GOOD\",\n"
        + "    \"segmentName\": \"airlineStats_OFFLINE_16071_16071_0\"\n" + "  },\n" + "  {\n"
        + "    \"segmentStatus\": \"BAD\",\n" + "    \"segmentName\": \"airlineStats_OFFLINE_16072_16072_0\"\n"
        + "  },\n" + "  {\n" + "    \"segmentStatus\": \"UPDATING\",\n"
        + "    \"segmentName\": \"airlineStats_OFFLINE_16073_16073_0\"\n" + "  }\n" + "]";
    JsonNode jsonNode = JsonUtils.stringToJsonNode(json);
    List<SegmentStatusInfo> segmentStatusInfos =
        JsonUtils.jsonNodeToObject(jsonNode, new TypeReference<List<SegmentStatusInfo>>() {
        });
    // Assertions
    assertEquals(segmentStatusInfos.size(), 3);
    assertEquals(segmentStatusInfos.get(0).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD);
    assertEquals(segmentStatusInfos.get(0).getSegmentName(), "airlineStats_OFFLINE_16071_16071_0");
    assertEquals(segmentStatusInfos.get(1).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD);
    assertEquals(segmentStatusInfos.get(1).getSegmentName(), "airlineStats_OFFLINE_16072_16072_0");
    assertEquals(segmentStatusInfos.get(2).getSegmentStatus(),
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING);
    assertEquals(segmentStatusInfos.get(2).getSegmentName(), "airlineStats_OFFLINE_16073_16073_0");
  }

  @Test
  public void testJsonSerializationSegmentStatusInfo()
      throws Exception {
    SegmentStatusInfo statusInfo1 = new SegmentStatusInfo("airlineStats_OFFLINE_16071_16071_0",
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.GOOD);
    SegmentStatusInfo statusInfo2 = new SegmentStatusInfo("airlineStats_OFFLINE_16072_16072_0",
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.BAD);
    SegmentStatusInfo statusInfo3 = new SegmentStatusInfo("airlineStats_OFFLINE_16073_16073_0",
        CommonConstants.Helix.StateModel.DisplaySegmentStatus.UPDATING);
    List<SegmentStatusInfo> segmentStatusInfoList = new ArrayList<>();
    segmentStatusInfoList.add(statusInfo1);
    segmentStatusInfoList.add(statusInfo2);
    segmentStatusInfoList.add(statusInfo3);
    String json =
        "[ {\n" + "  \"segmentName\" : \"airlineStats_OFFLINE_16071_16071_0\",\n" + "  \"segmentStatus\" : \"GOOD\"\n"
            + "}, {\n" + "  \"segmentName\" : \"airlineStats_OFFLINE_16072_16072_0\",\n"
            + "  \"segmentStatus\" : \"BAD\"\n" + "}, {\n"
            + "  \"segmentName\" : \"airlineStats_OFFLINE_16073_16073_0\",\n" + "  \"segmentStatus\" : \"UPDATING\"\n"
            + "} ]";
    String jsonString = JsonUtils.objectToPrettyString(segmentStatusInfoList);
    assertEquals(jsonString, json);
  }

  @Test
  public void testInvalidSegmentStartEndTime() {
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "ONLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_0", "pinot3", "ONLINE");

    ZNRecord znRecord = new ZNRecord("myTable_0");
    znRecord.setLongField(CommonConstants.Segment.START_TIME, TimeUtils.VALID_MIN_TIME_MILLIS - 1);
    znRecord.setLongField(CommonConstants.Segment.END_TIME, TimeUtils.VALID_MAX_TIME_MILLIS + 1);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    when(segmentZKMetadata.getStartTimeMs()).thenReturn(TimeUtils.VALID_MIN_TIME_MILLIS - 1);
    when(segmentZKMetadata.getEndTimeMs()).thenReturn(TimeUtils.VALID_MAX_TIME_MILLIS + 1);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    mockSegmentsZKMetadataForAllSegments(resourceManager, OFFLINE_TABLE_NAME, idealState, segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_INVALID_START_TIME), 1);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_INVALID_END_TIME), 1);
  }

  @Test
  public void tableTenantInfoGaugeNamedTenantTest() {
    String serverTenant = "myTenant";
    String brokerTenant = "myBroker";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setServerTenant(serverTenant)
            .setBrokerTenant(brokerTenant).build();

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);

    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "server." + serverTenant, ControllerGauge.TABLE_TENANT_INFO), 1);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "broker." + brokerTenant, ControllerGauge.TABLE_TENANT_INFO), 1);
  }

  @Test
  public void tableTenantInfoGaugeDefaultTenantFallbackTest() {
    // No tenant configured — both server and broker should fall back to "DefaultTenant".
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);

    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "server.DefaultTenant", ControllerGauge.TABLE_TENANT_INFO), 1);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "broker.DefaultTenant", ControllerGauge.TABLE_TENANT_INFO), 1);
  }

  @Test
  public void tableTenantInfoGaugeTierTenantTest() {
    // Table with a tier config — tier server tenant should be extracted from the server tag and emitted.
    TierConfig tierConfig = new TierConfig("coldTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, "tierTenant_OFFLINE", null, null);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setServerTenant("myTenant")
            .setTierConfigList(List.of(tierConfig)).build();

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);

    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "server.myTenant", ControllerGauge.TABLE_TENANT_INFO), 1);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "tier.tierTenant", ControllerGauge.TABLE_TENANT_INFO), 1);
  }

  @Test
  public void tableTenantInfoGaugeTenantChangeCleansStaleGaugeTest() {
    String firstTenant = "tenantA";
    String secondTenant = "tenantB";

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    // First run: table on firstTenant.
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setServerTenant(firstTenant).build());
    SegmentStatusChecker checker = buildSegmentStatusChecker(resourceManager, 0);
    checker.start();
    checker.run();
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "server." + firstTenant, ControllerGauge.TABLE_TENANT_INFO), 1);

    // Second run: table moves to secondTenant — stale gauge for firstTenant must be removed.
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setServerTenant(secondTenant).build());
    checker.run();
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "server." + secondTenant, ControllerGauge.TABLE_TENANT_INFO), 1);
    assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME, "server." + firstTenant,
        ControllerGauge.TABLE_TENANT_INFO), "stale server firstTenant gauge must be removed after tenant change");
  }

  @Test
  public void tableTenantInfoGaugeTableRemovedCleansUpTest() {
    String serverTenant = "myTenant";

    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setServerTenant(serverTenant).build());
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    SegmentStatusChecker checker = buildSegmentStatusChecker(resourceManager, 0);
    checker.start();
    checker.run();
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        "server." + serverTenant, ControllerGauge.TABLE_TENANT_INFO), 1);

    // Table disappears from Helix — nonLeaderCleanup triggers removeMetricsForTable.
    checker.nonLeaderCleanup(List.of(OFFLINE_TABLE_NAME));
    assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, OFFLINE_TABLE_NAME, "server." + serverTenant,
        ControllerGauge.TABLE_TENANT_INFO), "tenant gauge must be removed when table is cleaned up");
  }

  @Test
  public void tableTenantInfoGaugeRealtimeTableTest() {
    String serverTenant = "realtimeTenant";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setServerTenant(serverTenant)
            .setTimeColumnName("timeColumn").setStreamConfigs(getStreamConfigMap()).build();

    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);

    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, REALTIME_TABLE_NAME,
        "server." + serverTenant, ControllerGauge.TABLE_TENANT_INFO), 1);
  }

  private SegmentStatusChecker buildSegmentStatusChecker(PinotHelixResourceManager resourceManager,
      int waitForPushTimeInSeconds) {
    return buildSegmentStatusChecker(resourceManager, waitForPushTimeInSeconds, mock(TableSizeReader.class));
  }

  private SegmentStatusChecker buildSegmentStatusChecker(PinotHelixResourceManager resourceManager,
      int waitForPushTimeInSeconds, TableSizeReader tableSizeReader) {
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    ControllerConf controllerConf = mock(ControllerConf.class);
    when(controllerConf.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(waitForPushTimeInSeconds);
    return new SegmentStatusChecker(resourceManager, leadControllerManager, controllerConf, _controllerMetrics,
        tableSizeReader);
  }
}
