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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
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
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.SegmentStatusInfo;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


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
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    when(resourceManager.getSegmentZKMetadata(eq(OFFLINE_TABLE_NAME), anyString())).thenReturn(segmentZKMetadata);

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
  }

  private SegmentZKMetadata mockPushedSegmentZKMetadata(long sizeInBytes, long pushTimeMs) {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getStatus()).thenReturn(Status.UPLOADED);
    when(segmentZKMetadata.getSizeInBytes()).thenReturn(sizeInBytes);
    when(segmentZKMetadata.getPushTime()).thenReturn(pushTimeMs);
    return segmentZKMetadata;
  }

  private void runSegmentStatusChecker(PinotHelixResourceManager resourceManager, int waitForPushTimeInSeconds) {
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    ControllerConf controllerConf = mock(ControllerConf.class);
    when(controllerConf.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(waitForPushTimeInSeconds);
    TableSizeReader tableSizeReader = mock(TableSizeReader.class);
    SegmentStatusChecker segmentStatusChecker =
        new SegmentStatusChecker(resourceManager, leadControllerManager, controllerConf, _controllerMetrics,
            tableSizeReader);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
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
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committedSegmentZKMetadata = mockCommittedSegmentZKMetadata();
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg1)).thenReturn(committedSegmentZKMetadata);
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg2)).thenReturn(committedSegmentZKMetadata);
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg3)).thenReturn(consumingSegmentZKMetadata);

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
  public void realtimeMutableSegmnetHasLessReplicaTest() {
        InstanceAssignmentConfig instanceAssignmentConfig =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", true, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(true, 4, 4, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap())
            .setInstanceAssignmentConfigMap(
                Map.of("CONSUMING", instanceAssignmentConfig)
            )
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
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committedSegmentZKMetadata = mockCommittedSegmentZKMetadata();
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg1)).thenReturn(committedSegmentZKMetadata);
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg2)).thenReturn(committedSegmentZKMetadata);
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg3)).thenReturn(consumingSegmentZKMetadata);

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
  public void realtimeImmutableSegmnetHasLessReplicaTest() {
    InstanceAssignmentConfig instanceAssignmentConfig =
        new InstanceAssignmentConfig(new InstanceTagPoolConfig("DefaultTenant", true, 0, null), null,
            new InstanceReplicaGroupPartitionConfig(true, 4, 4, 0, 0, 0, false, null), null, false);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap())
            .setInstanceAssignmentConfigMap(
                Map.of("CONSUMING", instanceAssignmentConfig)
            )
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
    when(resourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getAllTables()).thenReturn(List.of(REALTIME_TABLE_NAME));
    when(resourceManager.getTableIdealState(REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(REALTIME_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata committedSegmentZKMetadata = mockCommittedSegmentZKMetadata();
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg1)).thenReturn(committedSegmentZKMetadata);
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg2)).thenReturn(committedSegmentZKMetadata);
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(11111L);
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, seg3)).thenReturn(consumingSegmentZKMetadata);

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
    return Map.of("streamType", "kafka", "stream.kafka.consumer.type", "simple", "stream.kafka.topic.name", "test",
        "stream.kafka.decoder.class.name", "org.apache.pinot.plugin.stream.kafka.KafkaAvroMessageDecoder",
        "stream.kafka.consumer.factory.class.name",
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
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    when(resourceManager.getSegmentZKMetadata(eq(OFFLINE_TABLE_NAME), anyString())).thenReturn(segmentZKMetadata);

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
    when(resourceManager.getSegmentZKMetadata(eq(OFFLINE_TABLE_NAME), anyString())).thenReturn(segmentZKMetadata);

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
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata01 = mockPushedSegmentZKMetadata(1234, 11111L);
    when(resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, "myTable_0")).thenReturn(segmentZKMetadata01);
    when(resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, "myTable_1")).thenReturn(segmentZKMetadata01);
    SegmentZKMetadata segmentZKMetadata2 = mockPushedSegmentZKMetadata(1234, System.currentTimeMillis());
    when(resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, "myTable_2")).thenReturn(segmentZKMetadata2);

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
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, "myTable_0")).thenReturn(updatedSegmentZKMetadata);
    SegmentZKMetadata consumingSegmentZKMetadata = mockConsumingSegmentZKMetadata(System.currentTimeMillis());
    when(resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, "myTable_1")).thenReturn(consumingSegmentZKMetadata);

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
    when(resourceManager.getSegmentZKMetadata(eq(REALTIME_TABLE_NAME), anyString())).thenReturn(segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(REALTIME_TABLE_NAME, 0, 1, 1, 1, 100, 0, 100, 0, 0);
  }

  @Test
  public void noSegmentZKMetadataTest() {
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    verifyControllerMetrics(OFFLINE_TABLE_NAME, 0, 1, 1, 1, 100, 0, 100, 0, 0);
  }

  @Test
  public void disabledTableTest() {
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

    runSegmentStatusChecker(resourceManager, 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerMetrics, ControllerGauge.DISABLED_TABLE_COUNT), 1);
    verifyControllerMetricsNotExist();
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
    when(resourceManager.getAllTables()).thenReturn(List.of(OFFLINE_TABLE_NAME));
    when(resourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(resourceManager.getTableIdealState(OFFLINE_TABLE_NAME)).thenReturn(idealState);
    when(resourceManager.getTableExternalView(OFFLINE_TABLE_NAME)).thenReturn(externalView);
    SegmentZKMetadata segmentZKMetadata = mockPushedSegmentZKMetadata(1234, 11111L);
    when(resourceManager.getSegmentZKMetadata(eq(OFFLINE_TABLE_NAME), anyString())).thenReturn(segmentZKMetadata);

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
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(tableViewExternal, tableViewIdeal);
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
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(tableViewExternal, tableViewIdeal);
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
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(tableViewExternal, tableViewIdeal);
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
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(tableViewExternal, tableViewIdeal);
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
    List<SegmentStatusInfo> segmentStatusInfos = tableviews.getSegmentStatuses(tableViewExternal, tableViewIdeal);
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
    when(resourceManager.getSegmentZKMetadata(eq(OFFLINE_TABLE_NAME), anyString())).thenReturn(segmentZKMetadata);

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    runSegmentStatusChecker(resourceManager, 0);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_INVALID_START_TIME), 1);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, OFFLINE_TABLE_NAME,
        ControllerGauge.SEGMENTS_WITH_INVALID_END_TIME), 1);
  }
}
