/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.relocation;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class RealtimeSegmentRelocatorTest {

  private TestRealtimeSegmentRelocator _realtimeSegmentRelocator;
  private HelixManager _mockHelixManager;

  private String[] serverNames;
  private String[] consumingServerNames;

  private List<String> getInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= serverNames.length);
    String[] instanceArray = Arrays.copyOf(serverNames, nServers);
    return Lists.newArrayList(instanceArray);
  }

  private List<String> getConsumingInstanceList(final int nServers) {
    Assert.assertTrue(nServers <= consumingServerNames.length);
    String[] instanceArray = Arrays.copyOf(consumingServerNames, nServers);
    return Lists.newArrayList(instanceArray);
  }

  @BeforeClass
  public void setup() {
    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    _mockHelixManager = mock(HelixManager.class);
    when(mockPinotHelixResourceManager.getHelixZkManager()).thenReturn(_mockHelixManager);
    ControllerConf controllerConfig = new ControllerConf();
    _realtimeSegmentRelocator = new TestRealtimeSegmentRelocator(mockPinotHelixResourceManager, controllerConfig);

    final int maxInstances = 20;
    serverNames = new String[maxInstances];
    consumingServerNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      serverNames[i] = "Server_" + i;
      consumingServerNames[i] = "ConsumingServer_" + i;
    }
  }

  /**
   * Tests the relocation of segments from consuming server to completed server, once all replicas become ONLINE
   */
  @Test
  public void testRelocationOfConsumingSegments() {
    String tableName = "aRealtimeTable_REALTIME";
    int nReplicas = 3;
    String serverTenantConsuming = "aServerTenant_REALTIME_CONSUMING";
    String serverTenantCompleted = "aServerTenant_REALTIME_COMPLETED";
    List<String> consumingInstanceList = getConsumingInstanceList(3);
    List<String> completedInstanceList = getInstanceList(3);
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableName);
    customModeIdealStateBuilder.setStateModel(
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0)
        .setNumReplica(nReplicas)
        .setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(tableName);

    RealtimeTagConfig realtimeTagConfig = mock(RealtimeTagConfig.class);
    when(realtimeTagConfig.getConsumingServerTag()).thenReturn(serverTenantConsuming);
    when(realtimeTagConfig.getCompletedServerTag()).thenReturn(serverTenantCompleted);

    Map<String, Integer> segmentNameToExpectedNumCompletedInstances = new HashMap<>(1);
    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();

    boolean exception = false;

    // no completed instances found
    _realtimeSegmentRelocator.setTagToInstance(serverTenantConsuming, consumingInstanceList);
    _realtimeSegmentRelocator.setTagToInstance(serverTenantCompleted, new ArrayList<>());
    try {
      _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // empty ideal state
    _realtimeSegmentRelocator.setTagToInstance(serverTenantConsuming, consumingInstanceList);
    _realtimeSegmentRelocator.setTagToInstance(serverTenantCompleted, completedInstanceList);
    IdealState prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertEquals(idealState, prevIdealState);

    // no move required, 1 segment all replicas in CONSUMING
    Map<String, String> instanceStateMap0 = new HashMap<>(3);
    instanceStateMap0.put(consumingInstanceList.get(0), "CONSUMING");
    instanceStateMap0.put(consumingInstanceList.get(1), "CONSUMING");
    instanceStateMap0.put(consumingInstanceList.get(2), "CONSUMING");
    idealState.setInstanceStateMap("segment0", instanceStateMap0);
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertEquals(idealState, prevIdealState);

    // no move necessary, 1 replica ONLINE on consuming, others CONSUMING
    instanceStateMap0.put(consumingInstanceList.get(0), "ONLINE");
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertEquals(idealState, prevIdealState);

    // no move necessary, 2 replicas ONLINE on consuming, 1 CONSUMING
    instanceStateMap0.put(consumingInstanceList.get(1), "ONLINE");
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertEquals(idealState, prevIdealState);

    // all replicas ONLINE on consuming servers. relocate 1 replica from consuming to completed
    instanceStateMap0.put(consumingInstanceList.get(2), "ONLINE");
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertNotSame(idealState, prevIdealState);
    segmentNameToExpectedNumCompletedInstances.put("segment0", 1);
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, nReplicas, segmentNameToExpectedNumCompletedInstances);

    // 2 replicas ONLINE on consuming servers, and 1 already relocated. relocate 1 replica from consuming to completed
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertNotSame(idealState, prevIdealState);
    segmentNameToExpectedNumCompletedInstances.put("segment0", 2);
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, nReplicas, segmentNameToExpectedNumCompletedInstances);

    // 1 replica ONLINE on consuming, 2 already relocated. relocate the 3rd replica.
    // However, only 2 completed servers, which is less than num replicas
    completedInstanceList = getInstanceList(2);
    _realtimeSegmentRelocator.setTagToInstance(serverTenantCompleted, completedInstanceList);
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    exception = false;
    try {
      _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // Revise list of completed servers to 3. Now we have enough completed servers for all replicas
    completedInstanceList = getInstanceList(3);
    _realtimeSegmentRelocator.setTagToInstance(serverTenantCompleted, completedInstanceList);
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertNotSame(idealState, prevIdealState);
    segmentNameToExpectedNumCompletedInstances.put("segment0", 3);
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, nReplicas, segmentNameToExpectedNumCompletedInstances);

    // new segment, all CONSUMING, no move necessary
    Map<String, String> instanceStateMap1 = new HashMap<>(3);
    instanceStateMap1.put(consumingInstanceList.get(0), "CONSUMING");
    instanceStateMap1.put(consumingInstanceList.get(1), "CONSUMING");
    instanceStateMap1.put(consumingInstanceList.get(2), "CONSUMING");
    idealState.setInstanceStateMap("segment1", instanceStateMap1);
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertEquals(idealState, prevIdealState);

    // 2 segments, both in ONLINE on consuming. move 1 replica for each of 2 segments
    instanceStateMap1.put(consumingInstanceList.get(0), "ONLINE");
    instanceStateMap1.put(consumingInstanceList.get(1), "ONLINE");
    instanceStateMap1.put(consumingInstanceList.get(2), "ONLINE");
    Map<String, String> instanceStateMap2 = new HashMap<>(3);
    instanceStateMap2.put(consumingInstanceList.get(0), "ONLINE");
    instanceStateMap2.put(consumingInstanceList.get(1), "ONLINE");
    instanceStateMap2.put(consumingInstanceList.get(2), "ONLINE");
    idealState.setInstanceStateMap("segment2", instanceStateMap2);
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertNotSame(idealState, prevIdealState);
    segmentNameToExpectedNumCompletedInstances.put("segment1", 1);
    segmentNameToExpectedNumCompletedInstances.put("segment2", 1);
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, nReplicas, segmentNameToExpectedNumCompletedInstances);

    // a segment with instances that are not consuming tagged instances. Relocate them as well
    Map<String, String> instanceStateMap3 = new HashMap<>(3);
    instanceStateMap3.put("notAConsumingServer_0", "ONLINE");
    instanceStateMap3.put("notAConsumingServer_1", "ONLINE");
    instanceStateMap3.put("notAConsumingServer_2", "ONLINE");
    idealState.setInstanceStateMap("segment3", instanceStateMap3);
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertNotSame(idealState, prevIdealState);
    segmentNameToExpectedNumCompletedInstances.put("segment1", 2);
    segmentNameToExpectedNumCompletedInstances.put("segment2", 2);
    segmentNameToExpectedNumCompletedInstances.put("segment3", 1);
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, nReplicas, segmentNameToExpectedNumCompletedInstances);
  }

  private void verifySegmentAssignment(IdealState updatedIdealState, IdealState prevIdealState,
      List<String> completedInstanceList, int nReplicas, Map<String, Integer> segmentNameToNumCompletedInstances) {
    Assert.assertEquals(updatedIdealState.getPartitionSet().size(), prevIdealState.getPartitionSet().size());
    Assert.assertTrue(prevIdealState.getPartitionSet().containsAll(updatedIdealState.getPartitionSet()));
    for (String segmentName : updatedIdealState.getPartitionSet()) {
      Map<String, String> newInstanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
      int onCompleted = 0;
      int notOnCompleted = 0;
      for (String instance : newInstanceStateMap.keySet()) {
        if (completedInstanceList.contains(instance)) {
          onCompleted++;
        } else {
          notOnCompleted++;
        }
      }
      int expectedOnCompletedServers = segmentNameToNumCompletedInstances.get(segmentName).intValue();
      Assert.assertEquals(onCompleted, expectedOnCompletedServers);
      Assert.assertEquals(notOnCompleted, nReplicas - expectedOnCompletedServers);
    }
  }

  private class TestRealtimeSegmentRelocator extends RealtimeSegmentRelocator {

    private Map<String, List<String>> tagToInstances;

    public TestRealtimeSegmentRelocator(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf config) {
      super(pinotHelixResourceManager, config);
      tagToInstances = new HashedMap();
    }

    @Override
    protected List<String> getInstancesWithTag(HelixManager helixManager, String instanceTag) {
      return tagToInstances.get(instanceTag);
    }

    void setTagToInstance(String tag, List<String> instances) {
      tagToInstances.put(tag, instances);
    }
  }
}
