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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class RealtimeConsumerMonitorTest {

  @Test
  public void realtimeBasicTest()
      throws Exception {
    String rawTableName = "myTable";
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(rawTableName).setTimeColumnName("timeColumn")
            .setNumReplicas(2).setStreamConfigs(getStreamConfigMap()).build();

    LLCSegmentName segmentPartition1Seq0 = new LLCSegmentName(rawTableName, 1, 0, System.currentTimeMillis());
    LLCSegmentName segmentPartition1Seq1 = new LLCSegmentName(rawTableName, 1, 1, System.currentTimeMillis());
    LLCSegmentName segmentPartition2Seq0 = new LLCSegmentName(rawTableName, 2, 0, System.currentTimeMillis());
    IdealState idealState = new IdealState(realtimeTableName);
    idealState.setPartitionState(segmentPartition1Seq0.getSegmentName(), "pinot1", "ONLINE");
    idealState.setPartitionState(segmentPartition1Seq0.getSegmentName(), "pinot2", "ONLINE");
    idealState.setPartitionState(segmentPartition1Seq1.getSegmentName(), "pinot1", "CONSUMING");
    idealState.setPartitionState(segmentPartition1Seq1.getSegmentName(), "pinot2", "CONSUMING");
    idealState.setPartitionState(segmentPartition2Seq0.getSegmentName(), "pinot1", "CONSUMING");
    idealState.setPartitionState(segmentPartition2Seq0.getSegmentName(), "pinot2", "CONSUMING");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(realtimeTableName);
    externalView.setState(segmentPartition1Seq0.getSegmentName(), "pinot1", "ONLINE");
    externalView.setState(segmentPartition1Seq0.getSegmentName(), "pinot2", "ONLINE");
    externalView.setState(segmentPartition1Seq1.getSegmentName(), "pinot1", "CONSUMING");
    externalView.setState(segmentPartition1Seq1.getSegmentName(), "pinot2", "CONSUMING");
    externalView.setState(segmentPartition2Seq0.getSegmentName(), "pinot1", "CONSUMING");
    externalView.setState(segmentPartition2Seq0.getSegmentName(), "pinot2", "CONSUMING");

    PinotHelixResourceManager helixResourceManager;
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      ZkHelixPropertyStore<ZNRecord> helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(helixResourceManager.getTableConfig(realtimeTableName)).thenReturn(tableConfig);
      when(helixResourceManager.getPropertyStore()).thenReturn(helixPropertyStore);
      when(helixResourceManager.getAllTables()).thenReturn(List.of(realtimeTableName));
      when(helixResourceManager.getTableIdealState(realtimeTableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(realtimeTableName)).thenReturn(externalView);
      ZNRecord znRecord = new ZNRecord("0");
      znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
      when(helixPropertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);
    }
    ControllerConf config;
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    LeadControllerManager leadControllerManager;
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    ControllerMetrics controllerMetrics = new ControllerMetrics(metricsRegistry);

    // server 1 caught up on partition-1 and partition-2
    // server 2 lags for partition-2 and caught up on partition-1
    // So, the consumer monitor should show: 1. partition-1 has 0 lag; partition-2 has some non-zero lag.
    // Segment 1 in replicas:
    TreeMap<String, List<ConsumingSegmentInfoReader.ConsumingSegmentInfo>> response = new TreeMap<>();
    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> part1ServerConsumingSegmentInfo =
        List.of(getConsumingSegmentInfoForServer("pinot1", "1", "100", "100", "0"),
            getConsumingSegmentInfoForServer("pinot2", "1", "100", "100", "0"));
    response.put(segmentPartition1Seq1.getSegmentName(), part1ServerConsumingSegmentInfo);

    // Segment 2 in replicas
    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> part2ServerConsumingSegmentInfo =
        List.of(getConsumingSegmentInfoForServer("pinot1", "2", "120", "120", "0"),
            getConsumingSegmentInfoForServer("pinot2", "2", "80", "120", "60000"));
    response.put(segmentPartition2Seq0.getSegmentName(), part2ServerConsumingSegmentInfo);

    ConsumingSegmentInfoReader consumingSegmentReader = mock(ConsumingSegmentInfoReader.class);
    when(consumingSegmentReader.getConsumingSegmentsInfo(realtimeTableName, 10000)).thenReturn(
        new ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap(response, 0, 0));
    RealtimeConsumerMonitor realtimeConsumerMonitor =
        new RealtimeConsumerMonitor(config, helixResourceManager, leadControllerManager, controllerMetrics,
            consumingSegmentReader);
    realtimeConsumerMonitor.start();
    realtimeConsumerMonitor.run();

    assertEquals(MetricValueUtils.getPartitionGaugeValue(controllerMetrics, realtimeTableName, 1,
        ControllerGauge.MAX_RECORDS_LAG), 0);
    assertEquals(MetricValueUtils.getPartitionGaugeValue(controllerMetrics, realtimeTableName, 2,
        ControllerGauge.MAX_RECORDS_LAG), 40);
    assertEquals(MetricValueUtils.getPartitionGaugeValue(controllerMetrics, realtimeTableName, 1,
        ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS), 0);
    assertEquals(MetricValueUtils.getPartitionGaugeValue(controllerMetrics, realtimeTableName, 2,
        ControllerGauge.MAX_RECORD_AVAILABILITY_LAG_MS), 60000);
  }

  ConsumingSegmentInfoReader.ConsumingSegmentInfo getConsumingSegmentInfoForServer(String serverName,
      String partitionId, String currentOffset, String upstreamLatestOffset, String availabilityLagMs) {
    Map<String, String> currentOffsetMap = Map.of(partitionId, currentOffset);
    Map<String, String> latestUpstreamOffsetMap = Map.of(partitionId, upstreamLatestOffset);
    Map<String, String> recordsLagMap =
        Map.of(partitionId, String.valueOf(Long.parseLong(upstreamLatestOffset) - Long.parseLong(currentOffset)));
    Map<String, String> availabilityLagMsMap = Map.of(partitionId, availabilityLagMs);

    ConsumingSegmentInfoReader.PartitionOffsetInfo partitionOffsetInfo =
        new ConsumingSegmentInfoReader.PartitionOffsetInfo(currentOffsetMap, latestUpstreamOffsetMap, recordsLagMap,
            availabilityLagMsMap);
    return new ConsumingSegmentInfoReader.ConsumingSegmentInfo(serverName, "CONSUMING", -1, currentOffsetMap,
        partitionOffsetInfo);
  }

  Map<String, String> getStreamConfigMap() {
    return Map.of("streamType", "kafka", "stream.kafka.topic.name", "test", "stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaAvroMessageDecoder", "stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory");
  }
}
