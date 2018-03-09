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
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class RealtimeSegmentRelocatorTest {

  private RealtimeSegmentRelocator _realtimeSegmentRelocator;
  private HelixManager _mockHelixManager;
  private MockHelixAdmin _mockHelixAdmin;

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
    _mockHelixAdmin = new MockHelixAdmin(null);
    when(mockPinotHelixResourceManager.getHelixZkManager()).thenReturn(_mockHelixManager);
    when(mockPinotHelixResourceManager.getHelixAdmin()).thenReturn(_mockHelixAdmin);
    ControllerConf controllerConfig = new ControllerConf();
    _realtimeSegmentRelocator =
        new RealtimeSegmentRelocator(mockPinotHelixResourceManager, controllerConfig);

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
    List<String> completedInstanceList = getInstanceList(6);
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

    // no consuming instances found
    _mockHelixAdmin.setInstanceInClusterWithTag(serverTenantConsuming, new ArrayList<String>());
    _mockHelixAdmin.setInstanceInClusterWithTag(serverTenantCompleted, completedInstanceList);
    boolean exception = false;
    try {
      _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);
    exception = false;

    // no completed instances found
    _mockHelixAdmin.setInstanceInClusterWithTag(serverTenantConsuming, consumingInstanceList);
    _mockHelixAdmin.setInstanceInClusterWithTag(serverTenantCompleted, new ArrayList<String>());
    try {
      _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // empty ideal state
    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();
    _mockHelixAdmin.setInstanceInClusterWithTag(serverTenantConsuming, consumingInstanceList);
    _mockHelixAdmin.setInstanceInClusterWithTag(serverTenantCompleted, completedInstanceList);
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
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, consumingInstanceList,
        nReplicas, segmentNameToExpectedNumCompletedInstances);

    // 2 replicas ONLINE on consuming servers, and 1 already relocated. relocate 1 replica from consuming to completed
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertNotSame(idealState, prevIdealState);
    segmentNameToExpectedNumCompletedInstances.put("segment0", 2);
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, consumingInstanceList,
        nReplicas, segmentNameToExpectedNumCompletedInstances);

    // 1 replica ONLINE on consuming, 2 already relocated. relocate the 3rd replica.
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertNotSame(idealState, prevIdealState);
    segmentNameToExpectedNumCompletedInstances.put("segment0", 3);
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, consumingInstanceList,
        nReplicas, segmentNameToExpectedNumCompletedInstances);

    // no move necessary
    Map<String, String> instanceStateMap1 = new HashMap<>(3);
    instanceStateMap1.put(consumingInstanceList.get(0), "CONSUMING");
    instanceStateMap1.put(consumingInstanceList.get(1), "CONSUMING");
    instanceStateMap1.put(consumingInstanceList.get(2), "CONSUMING");
    idealState.setInstanceStateMap("segment1", instanceStateMap1);
    prevIdealState = new IdealState(
        (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));
    _realtimeSegmentRelocator.relocateSegments(realtimeTagConfig, idealState);
    Assert.assertEquals(idealState, prevIdealState);

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
    verifySegmentAssignment(idealState, prevIdealState, completedInstanceList, consumingInstanceList,
        nReplicas, segmentNameToExpectedNumCompletedInstances);
  }

  private void verifySegmentAssignment(IdealState updatedIdealState,
      IdealState prevIdealState, List<String> completedInstanceList, List<String> consumingInstanceList, int nReplicas,
      Map<String, Integer> segmentNameToNumCompletedInstances) {
    Assert.assertEquals(updatedIdealState.getPartitionSet().size(), prevIdealState.getPartitionSet().size());
    Assert.assertTrue(prevIdealState.getPartitionSet().containsAll(updatedIdealState.getPartitionSet()));
    for (String segmentName : updatedIdealState.getPartitionSet()) {
      Map<String, String> newInstanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
      int completed = 0;
      int consuming = 0;
      for (String instance : newInstanceStateMap.keySet()) {
        if (completedInstanceList.contains(instance)) {
          completed++;
        } else if (consumingInstanceList.contains(instance)) {
          consuming++;
        }
      }
      int expectedOnCompletedServers = segmentNameToNumCompletedInstances.get(segmentName).intValue();
      Assert.assertEquals(completed, expectedOnCompletedServers);
      Assert.assertEquals(consuming, nReplicas - expectedOnCompletedServers);
    }
  }

  private class MockHelixAdmin extends ZKHelixAdmin {

    Map<String, List<String>> tagToInstances;

    MockHelixAdmin(ZkClient zkClient) {
      super(zkClient);
      tagToInstances = new HashMap<>();
    }

    @Override
    public List<String> getInstancesInClusterWithTag(String clusterName, String tag) {
      return tagToInstances.get(tag);
    }

    void setInstanceInClusterWithTag(String tag, List<String> instances) {
      tagToInstances.put(tag, instances);
    }
  }

}
