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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentStatusCheckerTest {
  private SegmentStatusChecker segmentStatusChecker;
  private PinotHelixResourceManager helixResourceManager;
  private LeadControllerManager leadControllerManager;
  private PinotMetricsRegistry metricsRegistry;
  private ControllerMetrics controllerMetrics;
  private ControllerConf config;

  @Test
  public void offlineBasicTest()
      throws Exception {
    final String tableName = "myTable_OFFLINE";
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

    ExternalView externalView = new ExternalView(tableName);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot1", "ERROR");
    externalView.setState("myTable_1", "pinot2", "ONLINE");

    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(externalView);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.SEGMENTS_IN_ERROR_STATE), 1);
    Assert
        .assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.NUMBER_OF_REPLICAS),
            1);
    Assert
        .assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.PERCENT_OF_REPLICAS),
            33);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
  }

  @Test
  public void realtimeBasicTest()
      throws Exception {
    final String tableName = "myTable_REALTIME";
    final String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
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
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(externalView);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert
        .assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.NUMBER_OF_REPLICAS),
            3);
    Assert
        .assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.PERCENT_OF_REPLICAS),
            100);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
  }

  @Test
  public void missingEVPartitionTest()
      throws Exception {
    final String tableName = "myTable_OFFLINE";
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
    idealState.setPartitionState("myTable_3", "pinot3", "ONLINE");
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(tableName);
    externalView.setState("myTable_0", "pinot1", "ONLINE");
    externalView.setState("myTable_0", "pinot2", "ONLINE");
    externalView.setState("myTable_1", "pinot1", "ERROR");
    externalView.setState("myTable_1", "pinot2", "ONLINE");

    ZNRecord znrecord = new ZNRecord("myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, "myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.TABLE_NAME, "myTable_OFFLINE");
    znrecord.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    znrecord.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.OFFLINE);
    znrecord.setLongField(CommonConstants.Segment.START_TIME, 1000);
    znrecord.setLongField(CommonConstants.Segment.END_TIME, 2000);
    znrecord.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    znrecord.setLongField(CommonConstants.Segment.TOTAL_DOCS, 10000);
    znrecord.setLongField(CommonConstants.Segment.CRC, 1234);
    znrecord.setLongField(CommonConstants.Segment.CREATION_TIME, 3000);
    znrecord.setSimpleField(CommonConstants.Segment.Offline.DOWNLOAD_URL, "http://localhost:8000/myTable_0");
    znrecord.setLongField(CommonConstants.Segment.Offline.PUSH_TIME, System.currentTimeMillis());
    znrecord.setLongField(CommonConstants.Segment.Offline.REFRESH_TIME, System.currentTimeMillis());

    ZkHelixPropertyStore<ZNRecord> propertyStore;
    {
      propertyStore = (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
      when(propertyStore.get("/SEGMENTS/myTable_OFFLINE/myTable_3", null, AccessOption.PERSISTENT))
          .thenReturn(znrecord);
    }

    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(externalView);
      when(helixResourceManager.getOfflineSegmentZKMetadata(tableName, "myTable_3"))
          .thenReturn(new OfflineSegmentZKMetadata(znrecord));
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(0);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.SEGMENTS_IN_ERROR_STATE), 1);
    Assert
        .assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.NUMBER_OF_REPLICAS),
            0);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 75);
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
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS), 0);
  }

  @Test
  public void missingIdealTest()
      throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<>();
    allTableNames.add(tableName);

    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(null);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE),
        Long.MIN_VALUE);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS),
        Long.MIN_VALUE);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS),
        Long.MIN_VALUE);
  }

  @Test
  public void missingEVPartitionPushTest()
      throws Exception {
    final String tableName = "myTable_OFFLINE";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    IdealState idealState = new IdealState(tableName);
    idealState.setPartitionState("myTable_0", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot1", "ONLINE");
    idealState.setPartitionState("myTable_1", "pinot2", "ONLINE");
    idealState.setReplicas("2");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    ExternalView externalView = new ExternalView(tableName);
    externalView.setState("myTable_1", "pinot1", "ONLINE");
    externalView.setState("myTable_1", "pinot2", "ONLINE");

    ZNRecord znrecord = new ZNRecord("myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, "myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.TABLE_NAME, "myTable_OFFLINE");
    znrecord.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    znrecord.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.OFFLINE);
    znrecord.setLongField(CommonConstants.Segment.START_TIME, 1000);
    znrecord.setLongField(CommonConstants.Segment.END_TIME, 2000);
    znrecord.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    znrecord.setLongField(CommonConstants.Segment.TOTAL_DOCS, 10000);
    znrecord.setLongField(CommonConstants.Segment.CRC, 1234);
    znrecord.setLongField(CommonConstants.Segment.CREATION_TIME, 3000);
    znrecord.setSimpleField(CommonConstants.Segment.Offline.DOWNLOAD_URL, "http://localhost:8000/myTable_0");
    znrecord.setLongField(CommonConstants.Segment.Offline.PUSH_TIME, System.currentTimeMillis());
    znrecord.setLongField(CommonConstants.Segment.Offline.REFRESH_TIME, System.currentTimeMillis());

    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(externalView);
      when(helixResourceManager.getOfflineSegmentZKMetadata(tableName, "myTable_0"))
          .thenReturn(new OfflineSegmentZKMetadata(znrecord));
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert
        .assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.NUMBER_OF_REPLICAS),
            2);
    Assert
        .assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.PERCENT_OF_REPLICAS),
            100);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(externalView.getId(), ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
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
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS), 1);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE),
        100);
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
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    // verify state before test
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT), 0);
    // update metrics
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT), 1);
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
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    // verify state before test
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT), 0);
    // update metrics
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT), 1);
  }

  @Test
  public void noSegments()
      throws Exception {
    noSegmentsInternal(0);
    noSegmentsInternal(5);
    noSegmentsInternal(-1);
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
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getTableIdealState(tableName)).thenReturn(idealState);
      when(helixResourceManager.getTableExternalView(tableName)).thenReturn(null);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    {
      leadControllerManager = mock(LeadControllerManager.class);
      when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    }
    metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker =
        new SegmentStatusChecker(helixResourceManager, leadControllerManager, config, controllerMetrics);
    segmentStatusChecker.start();
    segmentStatusChecker.run();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE),
        Long.MIN_VALUE);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS),
        nReplicasExpectedValue);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE),
        100);
  }
}
