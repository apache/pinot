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
package com.linkedin.pinot.controller.helix.core.rebalance;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class DefaultRebalanceStrategyTest {

  private HelixManager mockHelixManager = mock(HelixManager.class);
  private HelixAdmin mockHelixAdmin = mock(HelixAdmin.class);
  private InstanceConfig mockInstanceConfig = mock(InstanceConfig.class);

  private String[] serverNames;
  private String[] consumingServerNames;
  private DefaultRebalanceSegmentStrategy _rebalanceSegmentsStrategy;

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

  private void setInstanceStateMapForIdealState(IdealState idealState, int nSegments, int nReplicas,
      List<String> instances, String tableName, String state, String segmentPrefix) {
    int i = 0;
    for (int s = 0; s < nSegments; s++) {
      Map<String, String> instanceStateMap = new HashMap<>(nReplicas);
      for (int r = 0; r < nReplicas; r++) {
        instanceStateMap.put(instances.get(i++), state);
        if (i == instances.size()) {
          i = 0;
        }
      }
      idealState.setInstanceStateMap(segmentPrefix + tableName + "__" + s + "__0__1234", instanceStateMap);
    }
  }

  private void setInstanceStateMapForIdealState(IdealState idealState, PartitionAssignment partitionAssignment,
      int nSegments, int nReplicas, List<String> instances, String tableName, String state, String segmentPrefix) {
    int i = 0;
    for (int s = 0; s < nSegments; s++) {
      Map<String, String> instanceStateMap = new HashMap<>(nReplicas);
      for (int r = 0; r < nReplicas; r++) {
        instanceStateMap.put(instances.get(i++), state);
        if (i == instances.size()) {
          i = 0;
        }
      }
      idealState.setInstanceStateMap(segmentPrefix + tableName + "__" + s + "__0__1234", instanceStateMap);
      partitionAssignment.addPartition(String.valueOf(s), Lists.newArrayList(instanceStateMap.keySet()));
    }
  }

  @BeforeClass
  public void setUp() throws Exception {
    when(mockInstanceConfig.containsTag(anyString())).thenReturn(true);
    when(mockInstanceConfig.getInstanceEnabled()).thenReturn(true);
    when(mockHelixAdmin.getInstanceConfig(anyString(), anyString())).thenReturn(mockInstanceConfig);
    when(mockHelixManager.getClusterManagmentTool()).thenReturn(mockHelixAdmin);
    when(mockHelixManager.getClusterName()).thenReturn("mockClusterName");
    _rebalanceSegmentsStrategy = new DefaultRebalanceSegmentStrategy(mockHelixManager);

    final int maxInstances = 20;
    serverNames = new String[maxInstances];
    consumingServerNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      serverNames[i] = "Server_" + i;
      consumingServerNames[i] = "ConsumingServer_" + i;
    }
  }

  @Test
  public void testGetRebalancedIdealStateOffline() throws IOException, JSONException {

    String offlineTableName = "letsRebalanceThisTable_OFFLINE";
    TableConfig tableConfig;

    // start with an ideal state, i instances, r replicas, n segments, OFFLINE table
    int nReplicas = 2;
    int nSegments = 5;
    int nInstances = 6;
    List<String> instances = getInstanceList(nInstances);
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(instances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(instances);

    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(offlineTableName);
    customModeIdealStateBuilder.setStateModel(
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0)
        .setNumReplica(nReplicas)
        .setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(offlineTableName);
    setInstanceStateMapForIdealState(idealState, nSegments, nReplicas, instances, offlineTableName, "ONLINE",
        "offline");

    RebalanceUserConfig rebalanceUserConfig = new RebalanceUserConfig();
    rebalanceUserConfig.addConfig(RebalanceUserConfigProperties.DRYRUN, "true");
    rebalanceUserConfig.addConfig(RebalanceUserConfigProperties.REBALANCE_CONSUMING, "false");

    IdealState rebalancedIdealState;
    int targetNumReplicas = nReplicas;

    // rebalance with no change
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(offlineTableName)
        .setNumReplicas(targetNumReplicas)
        .build();
    rebalancedIdealState =
        testRebalance(idealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances, false);

    // increase i (i > n*r)
    instances = getInstanceList(12);
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(instances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(instances);
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            true);

    // rebalance with no change
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            false);

    // remove unused servers
    for (String segment : rebalancedIdealState.getPartitionSet()) {
      for (String server : rebalancedIdealState.getInstanceSet(segment)) {
        instances.remove(server);
      }
    }
    String serverToRemove = instances.get(0);
    instances = getInstanceList(12);
    instances.remove(serverToRemove);
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(instances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(instances);
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            false);

    // remove used servers
    instances = getInstanceList(8);
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(instances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(instances);
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            true);

    // replace servers
    String removedServer = instances.remove(0);
    instances.add(removedServer + "_replaced_server");
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(instances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(instances);
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            true);

    // reduce targetNumReplicas
    targetNumReplicas = 1;
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(offlineTableName)
        .setNumReplicas(targetNumReplicas)
        .build();
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            true);

    // increase targetNumReplicas
    targetNumReplicas = 3;
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(offlineTableName)
        .setNumReplicas(targetNumReplicas)
        .build();
    testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
        true);
  }

  @Test
  public void testGetRebalancedIdealStateRealtime() throws IOException, JSONException {

    String realtimeTableName = "letsRebalanceThisTable_REALTIME";
    TableConfig tableConfig;

    // new ideal state, i instances, r replicas, p partitions (p consuming segments, n*p completed segments), REALTIME table
    int nReplicas = 2;
    int nConsumingSegments = 4;
    int nCompletedSegments = 8;
    int nCompletedInstances = 6;
    int nConsumingInstances = 3;
    PartitionAssignment newPartitionAssignment = new PartitionAssignment(realtimeTableName);
    List<String> completedInstances = getInstanceList(nCompletedInstances);
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(completedInstances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(completedInstances);

    List<String> consumingInstances = getConsumingInstanceList(nConsumingInstances);
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(realtimeTableName);
    customModeIdealStateBuilder.setStateModel(
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0)
        .setNumReplica(nReplicas)
        .setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(realtimeTableName);
    setInstanceStateMapForIdealState(idealState, nCompletedSegments, nReplicas, completedInstances, realtimeTableName,
        "ONLINE", "completed");
    setInstanceStateMapForIdealState(idealState, newPartitionAssignment, nConsumingSegments, nReplicas,
        consumingInstances, realtimeTableName, "CONSUMING", "consuming");

    RebalanceUserConfig rebalanceUserConfig = new RebalanceUserConfig();
    rebalanceUserConfig.addConfig(RebalanceUserConfigProperties.DRYRUN, "true");
    rebalanceUserConfig.addConfig(RebalanceUserConfigProperties.REBALANCE_CONSUMING, "true");

    IdealState rebalancedIdealState;
    int targetNumReplicas = nReplicas;
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(realtimeTableName)
        .setLLC(true)
        .setNumReplicas(targetNumReplicas)
        .build();

    // no change
    rebalancedIdealState =
        testRebalanceRealtime(idealState, tableConfig, rebalanceUserConfig, newPartitionAssignment, targetNumReplicas,
            nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // reduce replicas
    targetNumReplicas = 1;
    for (Map.Entry<String, List<String>> entry : newPartitionAssignment.getPartitionToInstances().entrySet()) {
      entry.getValue().remove(1);
    }
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(realtimeTableName)
        .setLLC(true)
        .setNumReplicas(targetNumReplicas)
        .build();
    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // increase replicas
    targetNumReplicas = 2;
    setPartitionAssignment(newPartitionAssignment, targetNumReplicas, consumingInstances);
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(realtimeTableName)
        .setLLC(true)
        .setNumReplicas(targetNumReplicas)
        .build();
    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // remove completed server
    nCompletedInstances = 4;
    completedInstances = getInstanceList(nCompletedInstances);
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(completedInstances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(completedInstances);
    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // add completed server
    nCompletedInstances = 6;
    completedInstances = getInstanceList(nCompletedInstances);
    when(mockHelixAdmin.getInstancesInClusterWithTag(anyString(), anyString())).thenReturn(completedInstances);
    when(mockHelixAdmin.getInstancesInCluster(anyString())).thenReturn(completedInstances);
    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // remove consuming server
    nConsumingInstances = 2;
    consumingInstances = getConsumingInstanceList(nConsumingInstances);
    setPartitionAssignment(newPartitionAssignment, targetNumReplicas, consumingInstances);

    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // add consuming server
    nConsumingInstances = 3;
    consumingInstances = getConsumingInstanceList(nConsumingInstances);
    setPartitionAssignment(newPartitionAssignment, targetNumReplicas, consumingInstances);

    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // change partition assignment, but keep rebalanceConsuming false
    nConsumingInstances = 2;
    consumingInstances = getConsumingInstanceList(nConsumingInstances);
    setPartitionAssignment(newPartitionAssignment, targetNumReplicas, consumingInstances);
    rebalanceUserConfig.addConfig(RebalanceUserConfigProperties.REBALANCE_CONSUMING, "false");
    testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
        targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);
  }

  private void setPartitionAssignment(PartitionAssignment newPartitionAssignment, int targetNumReplicas,
      List<String> consumingInstances) {
    int instanceId = 0;
    for (String partition : newPartitionAssignment.getPartitionToInstances().keySet()) {
      List<String> newInstances = new ArrayList<>(targetNumReplicas);
      for (int i = 0; i < targetNumReplicas; i++) {
        newInstances.add(consumingInstances.get(instanceId++));
        if (instanceId == consumingInstances.size()) {
          instanceId = 0;
        }
      }
      newPartitionAssignment.addPartition(partition, newInstances);
    }
  }

  private IdealState testRebalance(IdealState idealState, TableConfig tableConfig,
      RebalanceUserConfig rebalanceUserConfig, int targetNumReplicas, int nSegments, List<String> instances,
      boolean changeExpected) {
    Map<String, Map<String, String>> prevAssignment = getPrevAssignment(idealState);
    IdealState rebalancedIdealState =
        _rebalanceSegmentsStrategy.rebalanceIdealState(idealState, tableConfig, rebalanceUserConfig, null);
    validateIdealState(rebalancedIdealState, nSegments, targetNumReplicas, instances, prevAssignment, changeExpected);
    return rebalancedIdealState;
  }

  private IdealState testRebalanceRealtime(IdealState idealState, TableConfig tableConfig,
      RebalanceUserConfig rebalanceUserConfig, PartitionAssignment newPartitionAssignment, int targetNumReplicas,
      int nSegmentsCompleted, int nSegmentsConsuming, List<String> instancesCompleted, List<String> instancesConsuming) {
    IdealState rebalancedIdealState =
        _rebalanceSegmentsStrategy.rebalanceIdealState(idealState, tableConfig, rebalanceUserConfig,
            newPartitionAssignment);
    validateIdealStateRealtime(rebalancedIdealState, nSegmentsCompleted, nSegmentsConsuming, targetNumReplicas,
        instancesCompleted, instancesConsuming, rebalanceUserConfig);
    return rebalancedIdealState;
  }

  private Map<String, Map<String, String>> getPrevAssignment(IdealState idealState) {
    Map<String, Map<String, String>> prevAssignment = new HashMap<>(1);
    for (String segment : idealState.getPartitionSet()) {
      Map<String, String> instanceMap = new HashMap<>(1);
      instanceMap.putAll(idealState.getInstanceStateMap(segment));
      prevAssignment.put(segment, instanceMap);
    }
    return prevAssignment;
  }

  private void validateIdealStateRealtime(IdealState rebalancedIdealState, int nSegmentsCompleted,
      int nSegmentsConsuming, int targetNumReplicas, List<String> instancesCompleted, List<String> instancesConsuming,
      RebalanceUserConfig rebalanceUserConfig) {
    Assert.assertEquals(rebalancedIdealState.getPartitionSet().size(), nSegmentsCompleted + nSegmentsConsuming);
    for (String segment : rebalancedIdealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = rebalancedIdealState.getInstanceStateMap(segment);
      Assert.assertEquals(instanceStateMap.size(), targetNumReplicas);
      String rebalanceConsuming = rebalanceUserConfig.getConfig(RebalanceUserConfigProperties.REBALANCE_CONSUMING);
      if (segment.contains("consuming")) {
        if (rebalanceConsuming != null && rebalanceConsuming.equals("true")) {
          Assert.assertTrue(instancesConsuming.containsAll(instanceStateMap.keySet()));
        }
      } else {
        Assert.assertTrue(instancesCompleted.containsAll(instanceStateMap.keySet()));
      }
    }
  }

  private void validateIdealState(IdealState rebalancedIdealState, int nSegments, int targetNumReplicas,
      List<String> instances, Map<String, Map<String, String>> prevAssignment, boolean changeExpected) {
    Assert.assertEquals(rebalancedIdealState.getPartitionSet().size(), nSegments);
    for (String segment : rebalancedIdealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = rebalancedIdealState.getInstanceStateMap(segment);
      Assert.assertEquals(instanceStateMap.size(), targetNumReplicas);
      Assert.assertTrue(instances.containsAll(instanceStateMap.keySet()));
    }

    boolean changed = false;
    for (String segment : prevAssignment.keySet()) {
      Map<String, String> prevInstanceMap = prevAssignment.get(segment);
      Map<String, String> instanceStateMap = rebalancedIdealState.getInstanceStateMap(segment);
      if (!changeExpected) {
        Assert.assertTrue(prevInstanceMap.keySet().containsAll(instanceStateMap.keySet()));
        Assert.assertTrue(instanceStateMap.keySet().containsAll(prevInstanceMap.keySet()));
      } else {
        if (!prevInstanceMap.keySet().containsAll(instanceStateMap.keySet()) || !instanceStateMap.keySet()
            .containsAll(prevInstanceMap.keySet())) {
          changed = true;
          break;
        }
      }
    }
    Assert.assertEquals(changeExpected, changed);
  }
}
