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
package org.apache.pinot.controller.helix.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class SegmentResetTest {

  private final String _tableName = "myTable_OFFLINE";
  private final String _segmentName = "segment_1";
  private final String _instance1 = "instance_1";
  private final String _instance2 = "instance_2";

  private class MockPinotHelixResourceManager extends PinotHelixResourceManager {

    private final Set<String> _instanceToFail;

    public MockPinotHelixResourceManager(ControllerConf controllerConf) {
      super(controllerConf);
      _instanceToFail = new HashSet<>();
    }

    @Override
    public IdealState getTableIdealState(String tableNameWithType) {
      IdealState idealState = Mockito.mock(IdealState.class);
      when(idealState.getInstanceSet(_segmentName)).thenReturn(new HashSet<>(Arrays.asList(_instance1, _instance2)));
      when(idealState.getPartitionSet()).thenReturn(new HashSet<>(Collections.singletonList(_segmentName)));
      return idealState;
    }

    @Override
    public ExternalView getTableExternalView(String tableNameWithType) {
      ExternalView externalView = Mockito.mock(ExternalView.class);
      Map<String, String> segmentStateMap = new HashMap<>();
      segmentStateMap.put(_instance1, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      segmentStateMap.put(_instance2, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      when(externalView.getStateMap(_segmentName)).thenReturn(segmentStateMap);
      return externalView;
    }

    @Override
    public void resetPartitionAllState(String instanceName, String resourceName, Set<String> resetPartitionNames) {
      if (_instanceToFail.contains(instanceName)) {
        throw new RuntimeException("Test: Fail reset for " + instanceName);
      }
    }

    public void addInstanceToFail(String instanceName) {
      _instanceToFail.add(instanceName);
    }
  }

  @Test
  public void testResetSegmentOneFailureOthersStillInvoked() {
    ControllerConf cfg = new ControllerConf();
    cfg.setZkStr("localhost:2181");
    cfg.setHelixClusterName("cluster01");
    MockPinotHelixResourceManager pinotHelixManager = new MockPinotHelixResourceManager(cfg);
    pinotHelixManager.addInstanceToFail(_instance1);

    RuntimeException runtimeException = Assert.expectThrows(RuntimeException.class,
        () -> pinotHelixManager.resetSegment(_tableName, _segmentName, null));
    Assert.assertEquals(runtimeException.getMessage(),
        "Reset segment failed for table: myTable_OFFLINE, segment: segment_1, instances: [instance_1]");

    runtimeException =
        Assert.expectThrows(RuntimeException.class, () -> pinotHelixManager.resetSegments(_tableName, null, false));
    Assert.assertEquals(runtimeException.getMessage(),
        "Reset segment failed for table: myTable_OFFLINE, instances: [instance_1]");
  }

  @Test
  public void testResetSegmentNoFailure() {
    ControllerConf cfg = new ControllerConf();
    cfg.setZkStr("localhost:2181");
    cfg.setHelixClusterName("cluster01");
    MockPinotHelixResourceManager pinotHelixManager = new MockPinotHelixResourceManager(cfg);

    pinotHelixManager.resetSegment(_tableName, _segmentName, null);
    pinotHelixManager.resetSegments(_tableName, null, false);
  }
}
