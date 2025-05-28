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

  String tableName = "myTable_OFFLINE";
  String segmentName = "segment_1";
  String instance1 = "instance_1";
  String instance2 = "instance_2";

  private class MockPinotHelixResourceManager extends PinotHelixResourceManager {

    private final Set<String> _instanceToFail;

    public MockPinotHelixResourceManager(ControllerConf controllerConf) {
      super(controllerConf);
      _instanceToFail = new HashSet<>();
    }

    @Override
    public IdealState getTableIdealState(String tableNameWithType) {
      IdealState idealState = Mockito.mock(IdealState.class);
      when(idealState.getInstanceSet(segmentName)).thenReturn(new HashSet<>(Arrays.asList(instance1, instance2)));
      when(idealState.getPartitionSet()).thenReturn(new HashSet<>(Collections.singletonList(segmentName)));
      return idealState;
    }

    @Override
    public ExternalView getTableExternalView(String tableNameWithType) {
      ExternalView externalView = Mockito.mock(ExternalView.class);
      Map<String, String> segmentStateMap = new HashMap<>();
      segmentStateMap.put(instance1, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      segmentStateMap.put(instance2, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      when(externalView.getStateMap(segmentName)).thenReturn(segmentStateMap);
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
  public void testResetSegment_oneFailure_othersStillInvoked() {
    ControllerConf cfg = new ControllerConf();
    cfg.setZkStr("localhost:2181");
    cfg.setHelixClusterName("cluster01");
    MockPinotHelixResourceManager pinotHelixManager = new MockPinotHelixResourceManager(cfg);
    pinotHelixManager.addInstanceToFail(instance1);

    RuntimeException runtimeException =
        Assert.expectThrows(RuntimeException.class, () -> pinotHelixManager.resetSegment(tableName, segmentName, null));
    Assert.assertEquals(runtimeException.getMessage(),
        "Reset segment failed for table: myTable_OFFLINE, segment: segment_1, instances: [instance_1]");

    runtimeException =
        Assert.expectThrows(RuntimeException.class, () -> pinotHelixManager.resetSegments(tableName, null, false));
    Assert.assertEquals(runtimeException.getMessage(),
        "Reset segment failed for table: myTable_OFFLINE, instances: [instance_1]");
  }

  @Test
  public void testResetSegmentNoFailure() {
    ControllerConf cfg = new ControllerConf();
    cfg.setZkStr("localhost:2181");
    cfg.setHelixClusterName("cluster01");
    MockPinotHelixResourceManager pinotHelixManager = new MockPinotHelixResourceManager(cfg);

    pinotHelixManager.resetSegment(tableName, segmentName, null);
    pinotHelixManager.resetSegments(tableName,null, false);
  }
}
