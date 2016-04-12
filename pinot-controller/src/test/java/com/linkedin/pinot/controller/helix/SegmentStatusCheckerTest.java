/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import static org.mockito.Mockito.*;


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
  public void basicTest() throws Exception {
    final String tableName = "myTable";
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
      when(helixAdmin.getResourceIdealState("StatusChecker","myTable")).thenReturn(idealState);
      when(helixAdmin.getResourceExternalView("StatusChecker","myTable")).thenReturn(externalView);
    }
    {
      helixResourceManager = mock(PinotHelixResourceManager.class);
      when(helixResourceManager.isLeader()).thenReturn(true);
      when(helixResourceManager.getAllPinotTableNames()).thenReturn(allTableNames);
      when(helixResourceManager.getHelixClusterName()).thenReturn("StatusChecker");
      when(helixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    }
    {
      config = mock(ControllerConf.class);
      when(config.getStatusControllerFrequencyInSeconds()).thenReturn(300);
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
    segmentStatusChecker.stop();
  }

}
