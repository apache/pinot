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
package com.linkedin.pinot.controller.helix.core.rebalance;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TagNameUtils;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.partition.IdealStateBuilderUtil;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.partition.StreamPartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamConfigProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class DefaultRebalanceStrategyTest {

  private class TestDefaultRebalanceStrategy extends DefaultRebalanceSegmentStrategy {

    private Map<String, List<String>> tagToInstances;

    public TestDefaultRebalanceStrategy(HelixManager helixManager) {
      super(helixManager);
      tagToInstances = new HashedMap();
    }

    void setTagToInstances(String tag, List<String> instances) {
      tagToInstances.put(tag, instances);
    }

    @Override
    protected List<String> getInstancesWithTag(String tag) {
      return tagToInstances.get(tag);
    }

    @Override
    protected List<String> getEnabledInstancesWithTag(String tag) {
      return tagToInstances.get(tag);
    }
  }

  private HelixManager _mockHelixManager;
  private String[] serverNames;
  private String[] consumingServerNames;
  private TestDefaultRebalanceStrategy _rebalanceSegmentsStrategy;

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

  private void setInstanceStateMapForIdealStateOffline(IdealState idealState, int nSegments, int nReplicas,
      List<String> instances, String tableName) {
    int i = 0;
    for (int s = 0; s < nSegments; s++) {
      Map<String, String> instanceStateMap = new HashMap<>(nReplicas);
      for (int r = 0; r < nReplicas; r++) {
        instanceStateMap.put(instances.get(i++), "ONLINE");
        if (i == instances.size()) {
          i = 0;
        }
      }
      idealState.setInstanceStateMap(tableName + "__" + s + "__0__1234", instanceStateMap);
    }
  }

  private void setInstanceStateMapForIdealStateRealtimeCompleted(IdealState idealState, int nPartitions,
      int nIterationsCompleted, int nReplicas, List<String> instances, String tableName) {
    int i = 0;
    for (int it = 0; it < nIterationsCompleted; it++) {
      for (int p = 0; p < nPartitions; p++) {
        Map<String, String> instanceStateMap = new HashMap<>(nReplicas);
        for (int r = 0; r < nReplicas; r++) {
          instanceStateMap.put(instances.get(i++), "ONLINE");
          if (i == instances.size()) {
            i = 0;
          }
        }
        idealState.setInstanceStateMap("completed" + tableName + "__" + p + "__" + it + "__1234", instanceStateMap);
      }
    }
  }

  private void setInstanceStateMapForIdealStateRealtimeConsuming(IdealState idealState,
      PartitionAssignment partitionAssignment, int nPartitions, int seqNum, int nReplicas, List<String> instances,
      String tableName) {
    int i = 0;
    for (int p = 0; p < nPartitions; p++) {
      Map<String, String> instanceStateMap = new HashMap<>(nReplicas);
      for (int r = 0; r < nReplicas; r++) {
        instanceStateMap.put(instances.get(i++), "CONSUMING");
        if (i == instances.size()) {
          i = 0;
        }
      }
      idealState.setInstanceStateMap("consuming" + tableName + "__" + p + "__" + seqNum + "__1234", instanceStateMap);
      if (partitionAssignment != null) {
        partitionAssignment.addPartition(String.valueOf(p), Lists.newArrayList(instanceStateMap.keySet()));
      }
    }
  }

  @BeforeClass
  public void setUp() throws Exception {
    _mockHelixManager = mock(HelixManager.class);
    _rebalanceSegmentsStrategy = new TestDefaultRebalanceStrategy(_mockHelixManager);

    final int maxInstances = 20;
    serverNames = new String[maxInstances];
    consumingServerNames = new String[maxInstances];
    for (int i = 0; i < maxInstances; i++) {
      serverNames[i] = "Server_" + i;
      consumingServerNames[i] = "ConsumingServer_" + i;
    }
  }

  /**
   * Tests for rebalancePartitionAssignment
   * Checks for :
   * 1. rebalance only for realtime table
   * 2. rebalance only if presence of simple consumer type
   * 3. rebalance only if includeConsuming flag true
   * 4. exception on invalid config
   * 5. gracefully handle empty ideal state
   * 6. handle capacity changes
   * 7. allow both hlc and llc
   *
   * @throws InvalidConfigException
   */
  @Test
  public void testGetRebalancedPartitionAssignment() throws InvalidConfigException {
    HelixManager mockHelixManager = mock(HelixManager.class);
    TestStreamPartitionAssignmentGenerator partitionAssignmentGenerator = new TestStreamPartitionAssignmentGenerator(mockHelixManager);
    TestRebalanceSegmentsStrategy rebalanceSegmentsStrategy = new TestRebalanceSegmentsStrategy(mockHelixManager);
    rebalanceSegmentsStrategy.setStreamPartitionAssignmentGenerator(partitionAssignmentGenerator);

    int nReplicas = 2;
    int nPartitions = 8;
    String consumerTypesCSV = "simple";
    String tableName = "anOfflineTable_OFFLINE";
    TableConfig tableConfig = makeTableConfig(tableName, nReplicas, consumerTypesCSV);
    IdealStateBuilderUtil idealStateBuilderUtil = new IdealStateBuilderUtil(tableName);
    IdealState idealState = idealStateBuilderUtil.build();
    Configuration rebalanceUserConfig = new PropertiesConfiguration();

    // not realtime table
    PartitionAssignment partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), 0);

    // does not have simple type consumer
    tableName = "aRealtimeTable_REALTIME";
    consumerTypesCSV = "highLevel";
    tableConfig = makeTableConfig(tableName, nReplicas, consumerTypesCSV);
    idealStateBuilderUtil = new IdealStateBuilderUtil(tableName);
    idealState = idealStateBuilderUtil.build();
    partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), 0);

    // includeConsuming not present - default should be false
    consumerTypesCSV = "simple";
    tableConfig = makeTableConfig(tableName, nReplicas, consumerTypesCSV);
    partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), 0);

    // include consuming false
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, false);
    partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), 0);

    //  invalid config
    rebalanceUserConfig.setProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, true);
    try {
      partitionAssignment =
          rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    } catch (InvalidConfigException e) {

    }
    Assert.assertEquals(partitionAssignment.getNumPartitions(), 0);

    // empty ideal state
    List<String> instances = getConsumingInstanceList(4);
    partitionAssignmentGenerator.setConsumingTaggedInstances(instances);
    partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), 0);

    // llc
    idealState = idealStateBuilderUtil.addConsumingSegments(nPartitions, 0, nReplicas, instances).build();
    partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), nPartitions);
    Assert.assertEquals(partitionAssignment.getAllInstances().size(), instances.size());
    Assert.assertTrue(partitionAssignment.getAllInstances().containsAll(instances));

    idealState = idealStateBuilderUtil.setSegmentState(0, 0, "ONLINE")
        .setSegmentState(1, 0, "ONLINE")
        .setSegmentState(2, 0, "ONLINE")
        .setSegmentState(3, 0, "ONLINE")
        .setSegmentState(4, 0, "ONLINE")
        .setSegmentState(5, 0, "ONLINE")
        .setSegmentState(6, 0, "ONLINE")
        .setSegmentState(7, 0, "ONLINE")
        .addConsumingSegments(nPartitions, 1, nReplicas, instances).build();
    partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), nPartitions);
    Assert.assertEquals(partitionAssignment.getAllInstances().size(), instances.size());
    Assert.assertTrue(partitionAssignment.getAllInstances().containsAll(instances));

    // instances changed
    instances = getConsumingInstanceList(7);
    instances.set(0, "replacedServer0");
    partitionAssignmentGenerator.setConsumingTaggedInstances(instances);
    partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), nPartitions);
    Assert.assertEquals(partitionAssignment.getAllInstances().size(), instances.size());
    Assert.assertTrue(partitionAssignment.getAllInstances().containsAll(instances));

    // hlc
    Map<String, String> instanceStateMap = new HashMap<>(2);
    instanceStateMap.put(instances.get(0), "ONLINE");
    instanceStateMap.put(instances.get(1), "ONLINE");
    idealState = idealStateBuilderUtil.addSegment("anHlcSegment", instanceStateMap).build();partitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    Assert.assertEquals(partitionAssignment.getNumPartitions(), nPartitions);
    Assert.assertEquals(partitionAssignment.getAllInstances().size(), instances.size());
    Assert.assertTrue(partitionAssignment.getAllInstances().containsAll(instances));

  }

  @Test
  public void testGetRebalancedIdealStateOffline() throws IOException, JSONException {

    String offlineTableName = "letsRebalanceThisTable_OFFLINE";
    TableConfig tableConfig;

    // start with an ideal state, i instances, r replicas, n segments, OFFLINE table
    int nReplicas = 2;
    int nSegments = 5;
    int nInstances = 6;
    String serverTenant = "aServerTenant";
    String offlineServerTag = TagNameUtils.getOfflineTagForTenant(serverTenant);
    List<String> instances = getInstanceList(nInstances);
    _rebalanceSegmentsStrategy.setTagToInstances(offlineServerTag, instances);

    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(offlineTableName);
    customModeIdealStateBuilder.setStateModel(
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0)
        .setNumReplica(nReplicas)
        .setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(offlineTableName);
    setInstanceStateMapForIdealStateOffline(idealState, nSegments, nReplicas, instances, offlineTableName);

    Configuration rebalanceUserConfig = new PropertiesConfiguration();
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.DRYRUN, true);
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, false);
    rebalanceUserConfig.setProperty(RebalanceUserConfigConstants.DOWNTIME, true);

    IdealState rebalancedIdealState;
    int targetNumReplicas = nReplicas;

    // rebalance with no change
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(offlineTableName)
        .setNumReplicas(targetNumReplicas).setServerTenant(serverTenant)
        .build();
    rebalancedIdealState =
        testRebalance(idealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances, false);

    // increase i (i > n*r)
    instances = getInstanceList(12);
    _rebalanceSegmentsStrategy.setTagToInstances(offlineServerTag, instances);
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
    _rebalanceSegmentsStrategy.setTagToInstances(offlineServerTag, instances);
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            false);

    // remove used servers
    instances = getInstanceList(8);
    _rebalanceSegmentsStrategy.setTagToInstances(offlineServerTag, instances);
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            true);

    // replace servers
    String removedServer = instances.remove(0);
    instances.add(removedServer + "_replaced_server");
    _rebalanceSegmentsStrategy.setTagToInstances(offlineServerTag, instances);
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            true);

    // reduce targetNumReplicas
    targetNumReplicas = 1;
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(offlineTableName)
        .setNumReplicas(targetNumReplicas).setServerTenant(serverTenant)
        .build();
    rebalancedIdealState =
        testRebalance(rebalancedIdealState, tableConfig, rebalanceUserConfig, targetNumReplicas, nSegments, instances,
            true);

    // increase targetNumReplicas
    targetNumReplicas = 3;
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(offlineTableName)
        .setNumReplicas(targetNumReplicas).setServerTenant(serverTenant)
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
    int nPartitions = 4;
    int nIterationsCompleted = 2;
    int nConsumingSegments = 4;
    int nCompletedSegments = nPartitions * nIterationsCompleted;
    int nCompletedInstances = 6;
    int nConsumingInstances = 3;
    String serverTenant = "aServerTenant";
    String realtimeTagForTenant = TagNameUtils.getRealtimeTagForTenant(serverTenant);
    PartitionAssignment newPartitionAssignment = new PartitionAssignment(realtimeTableName);
    List<String> completedInstances = getInstanceList(nCompletedInstances);
  _rebalanceSegmentsStrategy.setTagToInstances(realtimeTagForTenant, completedInstances);

    List<String> consumingInstances = getConsumingInstanceList(nConsumingInstances);
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(realtimeTableName);
    customModeIdealStateBuilder.setStateModel(
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL)
        .setNumPartitions(0)
        .setNumReplica(nReplicas)
        .setMaxPartitionsPerNode(1);
    IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(realtimeTableName);
    setInstanceStateMapForIdealStateRealtimeCompleted(idealState, nPartitions, nIterationsCompleted, nReplicas,
        completedInstances, realtimeTableName);
    setInstanceStateMapForIdealStateRealtimeConsuming(idealState, newPartitionAssignment, nConsumingSegments, 2,
        nReplicas, consumingInstances, realtimeTableName);

    Configuration rebalanceUserConfig = new PropertiesConfiguration();
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.DRYRUN, true);
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, true);

    IdealState rebalancedIdealState;
    int targetNumReplicas = nReplicas;
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(realtimeTableName)
        .setLLC(true)
        .setNumReplicas(targetNumReplicas).setServerTenant(serverTenant)
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
        .setNumReplicas(targetNumReplicas).setServerTenant(serverTenant)
        .build();
    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // increase replicas
    targetNumReplicas = 2;
    setPartitionAssignment(newPartitionAssignment, targetNumReplicas, consumingInstances);
    tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(realtimeTableName)
        .setLLC(true)
        .setNumReplicas(targetNumReplicas).setServerTenant(serverTenant)
        .build();
    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // remove completed server
    nCompletedInstances = 4;
    completedInstances = getInstanceList(nCompletedInstances);
    _rebalanceSegmentsStrategy.setTagToInstances(realtimeTagForTenant, completedInstances);
    rebalancedIdealState =
        testRebalanceRealtime(rebalancedIdealState, tableConfig, rebalanceUserConfig, newPartitionAssignment,
            targetNumReplicas, nCompletedSegments, nConsumingSegments, completedInstances, consumingInstances);

    // add completed server
    nCompletedInstances = 6;
    completedInstances = getInstanceList(nCompletedInstances);
    _rebalanceSegmentsStrategy.setTagToInstances(realtimeTagForTenant, completedInstances);
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
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, false);
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

  private IdealState testRebalance(IdealState idealState, TableConfig tableConfig, Configuration rebalanceUserConfig,
      int targetNumReplicas, int nSegments, List<String> instances, boolean changeExpected) {
    Map<String, Map<String, String>> prevAssignment = getPrevAssignment(idealState);
    IdealState rebalancedIdealState =
        _rebalanceSegmentsStrategy.getRebalancedIdealState(idealState, tableConfig, rebalanceUserConfig, null);
    validateIdealState(rebalancedIdealState, nSegments, targetNumReplicas, instances, prevAssignment, changeExpected);
    return rebalancedIdealState;
  }

  private IdealState testRebalanceRealtime(IdealState idealState, TableConfig tableConfig,
      Configuration rebalanceUserConfig, PartitionAssignment newPartitionAssignment, int targetNumReplicas,
      int nSegmentsCompleted, int nSegmentsConsuming, List<String> instancesCompleted,
      List<String> instancesConsuming) {
    IdealState rebalancedIdealState =
        _rebalanceSegmentsStrategy.getRebalancedIdealState(idealState, tableConfig, rebalanceUserConfig,
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
      Configuration rebalanceUserConfig) {
    Assert.assertEquals(rebalancedIdealState.getPartitionSet().size(), nSegmentsCompleted + nSegmentsConsuming);
    for (String segment : rebalancedIdealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = rebalancedIdealState.getInstanceStateMap(segment);
      Assert.assertEquals(instanceStateMap.size(), targetNumReplicas);
      boolean rebalanceConsuming = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.INCLUDE_CONSUMING);
      if (segment.contains("consuming")) {
        if (rebalanceConsuming) {
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

  private TableConfig makeTableConfig(String tableName, int nReplicas, String consumerTypesCSV) {
    TableConfig mockTableConfig = mock(TableConfig.class);
    when(mockTableConfig.getTableName()).thenReturn(tableName);
    SegmentsValidationAndRetentionConfig mockValidationConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(mockValidationConfig.getReplicasPerPartition()).thenReturn(Integer.toString(nReplicas));
    when(mockValidationConfig.getReplicasPerPartitionNumber()).thenReturn(nReplicas);
    when(mockTableConfig.getValidationConfig()).thenReturn(mockValidationConfig);
    CommonConstants.Helix.TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    when(mockTableConfig.getTableType()).thenReturn(tableTypeFromTableName);

    Map<String, String> streamConfigMap = new HashMap<>(1);
    String streamType = "kafka";
    String topic = "aTopic";
    String consumerFactoryClass = KafkaConsumerFactory.class.getName();
    String decoderClass = KafkaAvroMessageDecoder.class.getName();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        consumerTypesCSV);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);
    IndexingConfig mockIndexConfig = mock(IndexingConfig.class);
    when(mockIndexConfig.getStreamConfigs()).thenReturn(streamConfigMap);
    when(mockTableConfig.getIndexingConfig()).thenReturn(mockIndexConfig);

    return mockTableConfig;
  }

  /**
   * Test class for DefaultRebalanceSegmentsStrategy
   */
  private class TestRebalanceSegmentsStrategy extends DefaultRebalanceSegmentStrategy {
    HelixManager _helixManager;
    StreamPartitionAssignmentGenerator _streamPartitionAssignmentGenerator;

    public TestRebalanceSegmentsStrategy(HelixManager helixManager) {
      super(helixManager);
      _helixManager = helixManager;
    }

    @Override
    protected StreamPartitionAssignmentGenerator getStreamPartitionAssignmentGenerator() {
      return _streamPartitionAssignmentGenerator;
    }

    void setStreamPartitionAssignmentGenerator(StreamPartitionAssignmentGenerator streamPartitionAssignmentGenerator) {
      _streamPartitionAssignmentGenerator = streamPartitionAssignmentGenerator;
    }
  }

  /**
   * Test class for partition assignment generator
   */
  private class TestStreamPartitionAssignmentGenerator extends StreamPartitionAssignmentGenerator {
    private List<String> _consumingTaggedInstances = new ArrayList<>();

    public TestStreamPartitionAssignmentGenerator(HelixManager helixManager) {
      super(helixManager);
    }

    @Override
    protected List<String> getConsumingTaggedInstances(TableConfig tableConfig) {
      return _consumingTaggedInstances;
    }

    void setConsumingTaggedInstances(List<String> consumingTaggedInstances) {
      _consumingTaggedInstances = consumingTaggedInstances;
    }
  }
}
