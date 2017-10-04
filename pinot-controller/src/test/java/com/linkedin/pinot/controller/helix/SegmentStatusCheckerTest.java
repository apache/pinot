/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentStatusCheckerTest {
  private SegmentStatusChecker segmentStatusChecker;
  private PinotHelixResourceManager helixResourceManager;
  private MetricsRegistry metricsRegistry;
  private ControllerMetrics controllerMetrics;
  private ControllerConf config;

  @BeforeSuite
  public void setUp() throws Exception {
  }

  @AfterSuite
  public void tearDown() {
  }

  @BeforeMethod
  public void beforeMethod() {
  }

  @Test
  public void offlineBasicTest() throws Exception {
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
    externalView.setState("myTable_0","pinot1","ONLINE");
    externalView.setState("myTable_0","pinot2","ONLINE");
    externalView.setState("myTable_1","pinot1","ERROR");
    externalView.setState("myTable_1","pinot2","ONLINE");

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(externalView);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 1);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.NUMBER_OF_REPLICAS), 1);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.PERCENT_OF_REPLICAS), 33);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    segmentStatusChecker.stop();
  }

  @Test
  public void realtimeBasicTest() throws Exception {
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
    externalView.setState(seg1.getSegmentName(),"pinot1","ONLINE");
    externalView.setState(seg1.getSegmentName(), "pinot2","ONLINE");
    externalView.setState(seg1.getSegmentName(),"pinot3","ONLINE");
    externalView.setState(seg2.getSegmentName(),"pinot1","CONSUMING");
    externalView.setState(seg2.getSegmentName(),"pinot2","ONLINE");
    externalView.setState(seg2.getSegmentName(),"pinot3","CONSUMING");
    externalView.setState(seg3.getSegmentName(),"pinot1","CONSUMING");
    externalView.setState(seg3.getSegmentName(),"pinot2","CONSUMING");
    externalView.setState(seg3.getSegmentName(),"pinot3","OFFLINE");

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(externalView);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.NUMBER_OF_REPLICAS), 3);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    segmentStatusChecker.stop();
  }

  @Test
  public void nonLeaderTest() throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(false);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.NUMBER_OF_REPLICAS), 0);
    segmentStatusChecker.stop();
  }

  @Test
  public void missingEVPartitionTest() throws Exception {
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
    externalView.setState("myTable_0","pinot1","ONLINE");
    externalView.setState("myTable_0","pinot2","ONLINE");
    externalView.setState("myTable_1","pinot1","ERROR");
    externalView.setState("myTable_1","pinot2","ONLINE");

    ZNRecord znrecord =  new ZNRecord("myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.SEGMENT_NAME,"myTable_0");
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
    znrecord.setLongField(CommonConstants.Segment.Offline.REFRESH_TIME,System.currentTimeMillis());

    ZkHelixPropertyStore<ZNRecord> propertyStore;
    {
      propertyStore = (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
      when(propertyStore.get("/SEGMENTS/myTable_OFFLINE/myTable_3",null, AccessOption.PERSISTENT)).thenReturn(znrecord);
    }

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(externalView);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
      when(helixResourceManager.getPropertyStore()).thenReturn(propertyStore);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(0);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 1);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.NUMBER_OF_REPLICAS), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 75);
    segmentStatusChecker.stop();
  }

  @Test
  public void missingEVTest() throws Exception {
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

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(null);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.NUMBER_OF_REPLICAS), 0);
    segmentStatusChecker.stop();
  }

  @Test
  public void missingIdealTest() throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(null);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(null);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.NUMBER_OF_REPLICAS), 1);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    segmentStatusChecker.stop();
  }

  @Test
  public void missingEVPartitionPushTest() throws Exception {
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
    externalView.setState("myTable_1","pinot1","ONLINE");
    externalView.setState("myTable_1","pinot2","ONLINE");

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker","myTable_OFFLINE")).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker","myTable_OFFLINE")).thenReturn(externalView);
    }
    ZNRecord znrecord =  new ZNRecord("myTable_0");
    znrecord.setSimpleField(CommonConstants.Segment.SEGMENT_NAME,"myTable_0");
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
    znrecord.setLongField(CommonConstants.Segment.Offline.REFRESH_TIME,System.currentTimeMillis());

    ZkHelixPropertyStore<ZNRecord> propertyStore;
    {
      propertyStore = (ZkHelixPropertyStore<ZNRecord>) mock(ZkHelixPropertyStore.class);
      when(propertyStore.get("/SEGMENTS/myTable_OFFLINE/myTable_0",null, AccessOption.PERSISTENT)).thenReturn(znrecord);
    }

    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
      when(helixResourceManager.getPropertyStore()).thenReturn(propertyStore);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.NUMBER_OF_REPLICAS), 2);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(externalView.getId(),
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    segmentStatusChecker.stop();
  }

  @Test
  public void noReplicas() throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    IdealState idealState = new IdealState(tableName);
    idealState.setPartitionState("myTable_0", "pinot1", "OFFLINE");
    idealState.setPartitionState("myTable_0", "pinot2", "OFFLINE");
    idealState.setPartitionState("myTable_0", "pinot3", "OFFLINE");
    idealState.setReplicas("0");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(null);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.NUMBER_OF_REPLICAS), 1);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    segmentStatusChecker.stop();
  }

  @Test
  public void noIdealState() throws Exception {
    final String tableName = "myTable_REALTIME";
    List<String> allTableNames = new ArrayList<String>();
    allTableNames.add(tableName);
    IdealState idealState = null;
    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(null);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.NUMBER_OF_REPLICAS), 1);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    segmentStatusChecker.stop();
  }

  @Test
  public void noSegments() throws Exception {
    noSegmentsInternal(0);
    noSegmentsInternal(5);
    noSegmentsInternal(-1);
  }

  public void noSegmentsInternal(final int nReplicas) throws Exception {
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

    HelixAdmin helixAdmin;
    {
      helixAdmin = mock(HelixAdmin.class);
      when(helixAdmin.getResourceIdealState("StatusChecker",tableName)).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker",tableName)).thenReturn(null);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusCheckerFrequencyInSeconds()).thenReturn(300);
      when(config.getStatusCheckerWaitForPushTimeInSeconds()).thenReturn(300);
    }
    metricsRegistry = new MetricsRegistry();
    controllerMetrics = new ControllerMetrics(metricsRegistry);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    segmentStatusChecker.setMetricsRegistry(controllerMetrics);
    segmentStatusChecker.runSegmentMetrics();
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.NUMBER_OF_REPLICAS), nReplicasExpectedValue);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName,
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);
    segmentStatusChecker.stop();
  }
}
