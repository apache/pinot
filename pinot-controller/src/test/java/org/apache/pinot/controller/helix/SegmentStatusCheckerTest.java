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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentStatusCheckerTest {
  private SegmentStatusChecker _segmentStatusChecker;
  private PinotHelixResourceManager _helixResourceManager;
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  private LeadControllerManager _leadControllerManager;
  private PinotMetricsRegistry _metricsRegistry;
  private ControllerMetrics _controllerMetrics;
  private ControllerConf _config;
  private TableSizeReader _tableSizeReader;
  private ExecutorService _executorService = Executors.newFixedThreadPool(1);

  @Test
  public void offlineBasicTest()
      throws Exception {
    final String tableName = "myTable_OFFLINE";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setNumReplicas(2).build();

    IdealState idealState = new IdealState(tableName);
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
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(tableName);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot1", "ERROR");
    externalView.setState("myTable_1", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot3", "ERROR");
    externalView.setState("myTable_3", "pinot1", "ERROR");
    externalView.setState("myTable_3", "pinot2", "ONLINE");
    externalView.setState("myTable_3", "pinot3", "ONLINE");
    externalView.setState("myTable_4", "pinot1", "ONLINE");

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(externalView);
    }
    {
      _helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(_helixResourceManager.getPropertyStore()).thenReturn(_helixPropertyStore);
      // Based on the lineage entries: {myTable_1 -> myTable_3, COMPLETED}, {myTable_3 -> myTable_4, IN_PROGRESS},
      // myTable_1 and myTable_4 will be skipped for the metrics.
      SegmentLineage segmentLineage = new SegmentLineage(tableName);
      segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
          new LineageEntry(Collections.singletonList("myTable_1"), Collections.singletonList("myTable_3"),
              LineageEntryState.COMPLETED, 11111L));
      segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
          new LineageEntry(Collections.singletonList("myTable_3"), Collections.singletonList("myTable_4"),
              LineageEntryState.IN_PROGRESS, 11111L));
      when(_helixPropertyStore.get(eq("/SEGMENT_LINEAGE/" + tableName), any(), eq(AccessOption.PERSISTENT)))
          .thenReturn(segmentLineage.toZNRecord());
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
        ControllerGauge.REPLICATION_FROM_CONFIG), 2);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.SEGMENT_COUNT), 3);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.SEGMENT_COUNT_INCLUDING_REPLACED), 5);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.SEGMENTS_IN_ERROR_STATE), 1);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.NUMBER_OF_REPLICAS), 2);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_OF_REPLICAS), 66);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.TABLE_COMPRESSED_SIZE), 0);
  }

  @Test
  public void realtimeBasicTest()
      throws Exception {
    final String tableName = "myTable_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setTimeColumnName("timeColumn")
            .setNumReplicas(3).setStreamConfigs(getStreamConfigMap()).build();
    final LLCSegmentName seg1 = new LLCSegmentName(rawTableName, 1, 0, System.currentTimeMillis());
    final LLCSegmentName seg2 = new LLCSegmentName(rawTableName, 1, 1, System.currentTimeMillis());
    final LLCSegmentName seg3 = new LLCSegmentName(rawTableName, 2, 1, System.currentTimeMillis());
    IdealState idealState = new IdealState(tableName);
    idealState.setPartitionState(seg1.getSegmentName(), "pinot1", "ONLINE");
    idealState.setPartitionState(seg1.getSegmentName(), "pinot2", "ONLINE");
    idealState.setPartitionState(seg1.getSegmentName(), "pinot3", "ONLINE");
    idealState.setPartitionState(seg2.getSegmentName(), "pinot1", "ONLINE");
    idealState.setPartitionState(seg2.getSegmentName(), "pinot2", "ONLINE");
    idealState.setPartitionState(seg2.getSegmentName(), "pinot3", "ONLINE");
    idealState.setPartitionState(seg3.getSegmentName(), "pinot1", "CONSUMING");
    idealState.setPartitionState(seg3.getSegmentName(), "pinot2", "CONSUMING");
    idealState.setPartitionState(seg3.getSegmentName(), "pinot3", "OFFLINE");
    idealState.setReplicas("3");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(tableName);
    externalView.setState(seg1.getSegmentName(), "pinot1", "ONLINE");
    externalView.setState(seg1.getSegmentName(), "pinot2", "ONLINE");
    externalView.setState(seg1.getSegmentName(), "pinot3", "ONLINE");
    externalView.setState(seg2.getSegmentName(), "pinot1", "CONSUMING");
    externalView.setState(seg2.getSegmentName(), "pinot2", "ONLINE");
    externalView.setState(seg2.getSegmentName(), "pinot3", "CONSUMING");
    externalView.setState(seg3.getSegmentName(), "pinot1", "CONSUMING");
    externalView.setState(seg3.getSegmentName(), "pinot2", "CONSUMING");
    externalView.setState(seg3.getSegmentName(), "pinot3", "OFFLINE");

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      _helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(_helixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
      when(_helixResourceManager.getPropertyStore()).thenReturn(_helixPropertyStore);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(externalView);
      ZNRecord znRecord = new ZNRecord("0");
      znRecord.setSimpleField(CommonConstants.Segment.Realtime.END_OFFSET, "10000");
      when(_helixPropertyStore.get(anyString(), any(), anyInt())).thenReturn(znRecord);
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.REPLICATION_FROM_CONFIG), 3);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.NUMBER_OF_REPLICAS), 3);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.MISSING_CONSUMING_SEGMENT_TOTAL_COUNT), 2);
  }

  Map<String, String> getStreamConfigMap() {
    return ImmutableMap.of(
        "streamType", "kafka",
        "stream.kafka.consumer.type", "simple",
        "stream.kafka.topic.name", "test",
        "stream.kafka.decoder.class.name", "org.apache.pinot.plugin.stream.kafka.KafkaAvroMessageDecoder",
        "stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory");
  }

  @Test
  public void missingEVPartitionTest()
      throws Exception {
    String offlineTableName = "myTable_OFFLINE";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(offlineTableName);
    IdealState idealState = new IdealState(offlineTableName);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot3", "OFFLINE");
    idealState.setPartitionState("myTable_3", "pinot3", "ONLINE");
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(offlineTableName);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot1", "ERROR");
    externalView.setState("myTable_1", "pinot2", "ONLINE");

    ZNRecord znrecord = new ZNRecord("myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    znrecord.setLongField(CommonConstants.Segment.START_TIME, 1000);
    znrecord.setLongField(CommonConstants.Segment.END_TIME, 2000);
    znrecord.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    znrecord.setLongField(CommonConstants.Segment.TOTAL_DOCS, 10000);
    znrecord.setLongField(CommonConstants.Segment.CRC, 1234);
    znrecord.setLongField(CommonConstants.Segment.CREATION_TIME, 3000);
    znrecord.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "http://localhost:8000/myTable_0");
    znrecord.setLongField(CommonConstants.Segment.PUSH_TIME, System.currentTimeMillis());
    znrecord.setLongField(CommonConstants.Segment.REFRESH_TIME, System.currentTimeMillis());
    znrecord.setLongField(CommonConstants.Segment.SIZE_IN_BYTES, 1111);

    ZkHelixPropertyStore<ZNRecord> propertyStore;
    {
      propertyStore = (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
      when(propertyStore.get("/SEGMENTS/myTable_OFFLINE/myTable_3", null, AccessOption.PERSISTENT))
          .thenReturn(znrecord);
    }

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      _helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(_helixResourceManager.getPropertyStore()).thenReturn(_helixPropertyStore);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(offlineTableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(offlineTableName)).thenReturn(externalView);
      when(_helixResourceManager.getSegmentZKMetadata(offlineTableName, "myTable_3"))
          .thenReturn(new SegmentZKMetadata(znrecord));
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(0);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.SEGMENTS_IN_ERROR_STATE), 1);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.NUMBER_OF_REPLICAS), 0);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 75);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.TABLE_COMPRESSED_SIZE), 1111);
  }

  @Test
  public void missingEVTest()
      throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    IdealState idealState = new IdealState(tableName);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot3", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot3", "OFFLINE");
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      _helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(_helixResourceManager.getPropertyStore()).thenReturn(_helixPropertyStore);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.NUMBER_OF_REPLICAS), 0);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.TABLE_COMPRESSED_SIZE), 0);
  }

  @Test
  public void missingIdealTest()
      throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<>();
    allTableNames.add(tableName);

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(null);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.SEGMENTS_IN_ERROR_STATE), Long.MIN_VALUE);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.NUMBER_OF_REPLICAS), Long.MIN_VALUE);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.PERCENT_OF_REPLICAS), Long.MIN_VALUE);
    Assert.assertFalse(MetricValueUtils.tableGaugeExists(_controllerMetrics, tableName,
            ControllerGauge.TABLE_COMPRESSED_SIZE));
  }

  @Test
  public void missingEVPartitionPushTest()
      throws Exception {
    String offlineTableName = "myTable_OFFLINE";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(offlineTableName);
    IdealState idealState = new IdealState(offlineTableName);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_2", "pinot2", "ONLINE");
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(offlineTableName);
    externalView.setState("myTable_1", "pinot1", "ONLINE");
    externalView.setState("myTable_1", "pinot2", "ONLINE");
    // myTable_2 is push in-progress and only one replica has been downloaded by servers. It will be skipped for
    // the segment status check.
    externalView.setState("myTable_2", "pinot1", "ONLINE");

    ZNRecord znrecord = new ZNRecord("myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    znrecord.setLongField(CommonConstants.Segment.START_TIME, 1000);
    znrecord.setLongField(CommonConstants.Segment.END_TIME, 2000);
    znrecord.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    znrecord.setLongField(CommonConstants.Segment.TOTAL_DOCS, 10000);
    znrecord.setLongField(CommonConstants.Segment.CRC, 1234);
    znrecord.setLongField(CommonConstants.Segment.CREATION_TIME, 3000);
    znrecord.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "http://localhost:8000/myTable_0");
    znrecord.setLongField(CommonConstants.Segment.PUSH_TIME, System.currentTimeMillis());
    znrecord.setLongField(CommonConstants.Segment.REFRESH_TIME, System.currentTimeMillis());
    znrecord.setLongField(CommonConstants.Segment.SIZE_IN_BYTES, 1111);

    ZNRecord znrecord2 = new ZNRecord("myTable_2");
    znrecord2.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    znrecord2.setLongField(CommonConstants.Segment.START_TIME, 1000);
    znrecord2.setLongField(CommonConstants.Segment.END_TIME, 2000);
    znrecord2.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    znrecord2.setLongField(CommonConstants.Segment.TOTAL_DOCS, 10000);
    znrecord2.setLongField(CommonConstants.Segment.CRC, 1235);
    znrecord2.setLongField(CommonConstants.Segment.CREATION_TIME, 3000);
    znrecord2.setSimpleField(CommonConstants.Segment.DOWNLOAD_URL, "http://localhost:8000/myTable_2");
    znrecord2.setLongField(CommonConstants.Segment.PUSH_TIME, System.currentTimeMillis());
    znrecord2.setLongField(CommonConstants.Segment.REFRESH_TIME, System.currentTimeMillis());
    znrecord.setLongField(CommonConstants.Segment.SIZE_IN_BYTES, 1111);

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      _helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(_helixResourceManager.getPropertyStore()).thenReturn(_helixPropertyStore);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(offlineTableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(offlineTableName)).thenReturn(externalView);
      when(_helixResourceManager.getSegmentZKMetadata(offlineTableName, "myTable_0"))
          .thenReturn(new SegmentZKMetadata(znrecord));
      when(_helixResourceManager.getSegmentZKMetadata(offlineTableName, "myTable_2"))
          .thenReturn(new SegmentZKMetadata(znrecord2));
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.NUMBER_OF_REPLICAS), 2);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.TABLE_COMPRESSED_SIZE), 0);
  }

  @Test
  public void noReplicas()
      throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    IdealState idealState = new IdealState(tableName);
    idealState.setPartitionState("myTable_0", "pinot1", "OFFLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "OFFLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "OFFLINE");
    idealState.setReplicas("0");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      _helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(_helixResourceManager.getPropertyStore()).thenReturn(_helixPropertyStore);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.NUMBER_OF_REPLICAS), 1);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
            ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
  }

  @Test
  public void disabledTableTest()
      throws Exception {

    final String tableName = "myTable_OFFLINE";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    IdealState idealState = new IdealState(tableName);
    // disable table in idealstate
    idealState.enable(false);
    idealState.setPartitionState("myTable_OFFLINE", "pinot1", "OFFLINE");
    idealState.setPartitionState("myTable_OFFLINE", "pinot2", "OFFLINE");
    idealState.setPartitionState("myTable_OFFLINE", "pinot3", "OFFLINE");
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    // verify state before test
    Assert.assertEquals(
        MetricValueUtils.getGlobalGaugeValue(_controllerMetrics, ControllerGauge.DISABLED_TABLE_COUNT), 0);
    // update metrics
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(
        MetricValueUtils.getGlobalGaugeValue(_controllerMetrics, ControllerGauge.DISABLED_TABLE_COUNT), 1);
  }

  @Test
  public void disabledEmptyTableTest()
      throws Exception {

    final String tableName = "myTable_OFFLINE";
    List<String> allTableNames = Lists.newArrayList(tableName);
    IdealState idealState = new IdealState(tableName);
    // disable table in idealstate
    idealState.enable(false);
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    // verify state before test
    Assert.assertFalse(
        MetricValueUtils.globalGaugeExists(_controllerMetrics, ControllerGauge.DISABLED_TABLE_COUNT));
    // update metrics
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(
        MetricValueUtils.getGlobalGaugeValue(_controllerMetrics, ControllerGauge.DISABLED_TABLE_COUNT), 1);
  }

  @Test
  public void noSegments()
      throws Exception {
    noSegmentsInternal(0);
    noSegmentsInternal(5);
    noSegmentsInternal(-1);
  }

  @Test
  public void lessThanOnePercentSegmentsUnavailableTest()
          throws Exception {
    String tableName = "myTable_OFFLINE";
    int numSegments = 200;
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    TableConfig tableConfig =
            new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setNumReplicas(1).build();

    IdealState idealState = new IdealState(tableName);
    for (int i = 0; i < numSegments; i++) {
      idealState.setPartitionState("myTable_" + i, "pinot1", "ONLINE");
    }
    idealState.setReplicas("1");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(tableName);
    externalView.setState("myTable_0", "pinot1", "OFFLINE");
    for (int i = 1; i < numSegments; i++) {
      externalView.setState("myTable_" + i, "pinot1", "ONLINE");
    }

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(externalView);
    }
    {
      _helixPropertyStore = mock(ZkHelixPropertyStore.class);
      when(_helixResourceManager.getPropertyStore()).thenReturn(_helixPropertyStore);
      SegmentLineage segmentLineage = new SegmentLineage(tableName);
      when(_helixPropertyStore.get(eq("/SEGMENT_LINEAGE/" + tableName), any(), eq(AccessOption.PERSISTENT)))
              .thenReturn(segmentLineage.toZNRecord());
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
            new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
                    _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, externalView.getId(),
            ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 99);
  }

  public void noSegmentsInternal(final int nReplicas)
      throws Exception {
    final String tableName = "myTable_REALTIME";
    String nReplicasStr = Integer.toString(nReplicas);
    int nReplicasExpectedValue = nReplicas;
    if (nReplicas < 0) {
      nReplicasStr = "abc";
      nReplicasExpectedValue = 1;
    }
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    IdealState idealState = new IdealState(tableName);
    idealState.setReplicas(nReplicasStr);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    {
      _helixResourceManager = mock(PinotHelixResourceManager.class);
      when(_helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(_helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(_helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      _config = mock(ControllerConf.class);
      when(_config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(_config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      _leadControllerManager = mock(LeadControllerManager.class);
      when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    {
      _tableSizeReader = mock(TableSizeReader.class);
      when(_tableSizeReader.getTableSizeDetails(anyString(), anyInt())).thenReturn(null);
    }
    PinotMetricUtils.cleanUp();
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _executorService);
    _segmentStatusChecker.setTableSizeReader(_tableSizeReader);
    _segmentStatusChecker.start();
    _segmentStatusChecker.run();

    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), Long.MIN_VALUE);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
        ControllerGauge.NUMBER_OF_REPLICAS), nReplicasExpectedValue);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableName,
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
  }
}
