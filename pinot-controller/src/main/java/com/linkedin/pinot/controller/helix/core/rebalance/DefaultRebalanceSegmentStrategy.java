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
import com.linkedin.pinot.common.config.TagConfig;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.controller.helix.core.realtime.partition.StreamPartitionAssignmentGenerator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic rebalance segments strategy, which rebalances offline segments using autorebalance strategy,
 * and consuming segments using the partition assignment strategy for the table
 */
public class DefaultRebalanceSegmentStrategy extends BaseRebalanceSegmentStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRebalanceSegmentStrategy.class);

  protected static final String DEFAULT_DRY_RUN = "true";
  protected static final String DEFAULT_REBALANCE_CONSUMING = "false";

  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _helixClusterName;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public DefaultRebalanceSegmentStrategy(HelixManager helixManager) {
    super(helixManager);
    _helixManager = helixManager;
    _helixAdmin = helixManager.getClusterManagmentTool();
    _helixClusterName = helixManager.getClusterName();
    _propertyStore = helixManager.getHelixPropertyStore();
  }

  @Override
  public PartitionAssignment rebalancePartitionAssignment(IdealState idealState, TableConfig tableConfig,
      RebalanceUserParams rebalanceUserParams) {
    PartitionAssignment partitionAssignment = null;

    boolean rebalanceConsuming = Boolean.valueOf(
        rebalanceUserParams.getConfig(RebalanceUserParamConstants.REBALANCE_CONSUMING,
            DEFAULT_REBALANCE_CONSUMING));
    if (rebalanceConsuming) {
      LOGGER.info("Rebalancing partition assignment for table {}", tableConfig.getTableName());

      if (tableConfig.getTableType().equals(CommonConstants.Helix.TableType.REALTIME)) {

        StreamPartitionAssignmentGenerator streamPartitionAssignmentGenerator = new StreamPartitionAssignmentGenerator(_propertyStore);
        TagConfig tagConfig = new TagConfig(tableConfig, _helixManager);
        List<String> consumingInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, tagConfig.getConsumingServerTag());
        String tableNameWithType = tableConfig.getTableName();
        // FIXME: where should this come from? from kafka metadata stream or just read from znode how many are existing?
        int numPartitions = getKafkaPartitionCount(tableConfig);

        Map<String, PartitionAssignment> consumingPartitionAssignment =
            streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig, numPartitions,
                consumingInstances, Lists.newArrayList(tableNameWithType));
        partitionAssignment = consumingPartitionAssignment.get(tableNameWithType);

        boolean dryRun =
            Boolean.valueOf(rebalanceUserParams.getConfig(RebalanceUserParamConstants.DRYRUN, DEFAULT_DRY_RUN));
        if (!dryRun) {
          LOGGER.info("Updating stream partition assignment for table {}", tableNameWithType);
          streamPartitionAssignmentGenerator.writeStreamPartitionAssignment(consumingPartitionAssignment);
        } else {
          LOGGER.info("Dry run. Skip writing stream partition assignment to property store");
          LOGGER.info("Partition assignment for table {} is {}", tableNameWithType, partitionAssignment);
        }
      }
    } else {
      LOGGER.info("rebalanceConsuming = false. No need to rebalance partition assignment for {}", tableConfig.getTableType());
    }
    return partitionAssignment;
  }

  @Override
  public IdealState rebalanceIdealState(IdealState idealState, TableConfig tableConfig,
      RebalanceUserParams rebalanceUserParams, PartitionAssignment newPartitionAssignment) {

    LOGGER.info("Rebalancing ideal state for table {}", tableConfig.getTableName());
    String tableNameWithType = tableConfig.getTableName();
    CommonConstants.Helix.TableType tableType = tableConfig.getTableType();

    // if realtime, rebalance consuming segments
    if (tableType.equals(CommonConstants.Helix.TableType.REALTIME)) {
      boolean rebalanceConsuming = Boolean.valueOf(
          rebalanceUserParams.getConfig(RebalanceUserParamConstants.REBALANCE_CONSUMING,
              DEFAULT_REBALANCE_CONSUMING));
      if (rebalanceConsuming) {
        rebalanceConsumingSegments(idealState, newPartitionAssignment);
      }
    }
    // rebalance serving segments
    int targetNumReplicas;
    if (tableType.equals(CommonConstants.Helix.TableType.REALTIME)) {
      String replicasString = tableConfig.getValidationConfig().getReplicasPerPartition();
      try {
        targetNumReplicas = Integer.parseInt(replicasString);
      } catch (Exception e) {
        throw new RuntimeException("Invalid value for replicasPerPartition:'" + replicasString + "'", e);
      }
    } else {
      targetNumReplicas = Integer.parseInt(tableConfig.getValidationConfig().getReplication());
    }
    rebalanceServing(idealState, tableConfig, targetNumReplicas);
    boolean dryRun =
        Boolean.valueOf(rebalanceUserParams.getConfig(RebalanceUserParamConstants.DRYRUN, DEFAULT_DRY_RUN));
    if (!dryRun) {
      LOGGER.info("Updating ideal state for table {}", tableNameWithType);
      updateIdealStateWithNewSegmentMapping(tableNameWithType, targetNumReplicas,
          idealState.getRecord().getMapFields());
    } else {
      LOGGER.info("Dry run. Skip writing ideal state");
    }
    return idealState;
  }



  /**
   * Rebalances serving segments based on autorebalance strategy
   * @param idealState
   * @param tableConfig
   * @param targetNumReplicas
   * @return
   */
  protected IdealState rebalanceServing(IdealState idealState, TableConfig tableConfig, int targetNumReplicas) {

    LOGGER.info("Rebalancing serving segments for table {}", tableConfig.getTableName());
    TagConfig tagConfig = new TagConfig(tableConfig, _helixManager);
    List<String> servingInstances =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, tagConfig.getCompletedServerTag());
    List<String> enabledServingInstances =
        HelixHelper.getEnabledInstancesWithTag(_helixAdmin, _helixClusterName, tagConfig.getCompletedServerTag());

    int numReplicasInIdealState = Integer.parseInt(idealState.getReplicas());

    if (numReplicasInIdealState > targetNumReplicas) { // We need to reduce the number of replicas per helix partition.

      for (String segmentId : idealState.getPartitionSet()) {
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentId);
        if (instanceStateMap.size() > targetNumReplicas) {
          Set<String> keys = instanceStateMap.keySet();
          while (instanceStateMap.size() > targetNumReplicas) {
            instanceStateMap.remove(keys.iterator().next());
          }
        } else if (instanceStateMap.size() < targetNumReplicas) {
          LOGGER.warn("Table {}, segment {} has {} replicas, less than {} (requested number of replicas",
              idealState.getResourceName(), segmentId, instanceStateMap.size(), targetNumReplicas);
        }
      }
    } else { // Number of replicas is either the same or higher, so invoke Helix rebalancer.

      final Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

      List<Map.Entry<String, Map<String, String>>> removedEntries = new LinkedList<>();
      if (tableConfig.getTableType().equals(CommonConstants.Helix.TableType.REALTIME)) {
        // FIXME: what about those in OFFLINE states?
        removeSegmentsNotBalanceable(mapFields, removedEntries);
      }

      if (mapFields.isEmpty()) {// if ideal state had only CONSUMING segments so far, mapFields will be empty
        // we do not need to do anything in that case
        LOGGER.info("No LLC segments in ONLINE state for table {}", tableConfig.getTableName());
      }
      else {

        String tableNameWithType = tableConfig.getTableName();

        LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
        List<String> partitions = Lists.newArrayList(idealState.getPartitionSet());
        states.put(RealtimeSegmentOnlineOfflineStateModel.OFFLINE, 0);
        states.put(RealtimeSegmentOnlineOfflineStateModel.ONLINE, targetNumReplicas);
        Set<String> currentHosts = new HashSet<>();
        for (String segment : mapFields.keySet()) {
          currentHosts.addAll(mapFields.get(segment).keySet());
        }
        AutoRebalanceStrategy rebalanceStrategy = new AutoRebalanceStrategy(tableNameWithType, partitions, states);

        LOGGER.info("Current nodes for table {}: {}", tableNameWithType, currentHosts);
        LOGGER.info("New nodes for table {}: {}", tableNameWithType, servingInstances);
        LOGGER.info("Enabled nodes for table: {} {}", tableNameWithType, enabledServingInstances);
        ZNRecord newZnRecord =
            rebalanceStrategy.computePartitionAssignment(servingInstances, enabledServingInstances, mapFields, new ClusterDataCache());
        final Map<String, Map<String, String>> newMapping = newZnRecord.getMapFields();
        for (Map.Entry<String, Map<String, String>> entry : newMapping.entrySet()) {
          idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
        }
      }
      // If we removed any entries, add them back here
      for (Map.Entry<String, Map<String, String>> entry : removedEntries) {
        idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
      }
    }
    idealState.setReplicas(Integer.toString(targetNumReplicas));
    return idealState;
  }

  /**
   * Rebalances consuming segments based on partition assignment of the stream partitions
   * @param idealState
   * @param newPartitionAssignment
   */
  private void rebalanceConsumingSegments(IdealState idealState, PartitionAssignment newPartitionAssignment) {
    List<String> consumingSegments = new ArrayList<>();
    LOGGER.info("Rebalancing consuming segments for table {}", idealState.getResourceName());
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
    // FIXME: is this sufficient to get all consuming segments? what is someone went into error/offline?
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      Collection<String> instanceStates = entry.getValue().values();
      if (instanceStates.contains(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
        consumingSegments.add(entry.getKey());
      }
    }
    // TODO: check that targetNumReplicas and replicas in newPartitionAssignment match
    // update ideal state of consuming segments, based on new stream partition assignment
    Map<String, List<String>> partitionToInstanceMap = newPartitionAssignment.getPartitionToInstances();
    for (String segmentName : consumingSegments) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      Map<String, String> instanceStateMap = new HashMap<>();
      for (String instance : partitionToInstanceMap.get(String.valueOf(partitionId))) {
        instanceStateMap.put(instance, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      }
      idealState.setInstanceStateMap(llcSegmentName.getSegmentName(), instanceStateMap);
    }
  }

  // Keep only those segments that are LLC and in ONLINE state.
  private void removeSegmentsNotBalanceable(Map<String, Map<String, String>> mapFields,
      List<Map.Entry<String, Map<String, String>>> removedEntries) {
    // Keep only those segments that are LLC and in ONLINE state.
    Iterator<Map.Entry<String, Map<String, String>>> it = mapFields.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Map<String, String>> entry = it.next();
      final String segmentName = entry.getKey();
      boolean keep = false;
      if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        // Check the states. If any of the instances are ONLINE, then we keep the segment for rebalancing,
        // Otherwise, we remove it, and add it back after helix re-balances the segments.
        Map<String, String> stateMap = entry.getValue();
        for (String state : stateMap.values()) {
          if (state.equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)) {
            keep = true;
            break;
          }
        }
      }
      if (!keep) {
        removedEntries.add(entry);
        it.remove();
      }
    }
  }

  protected int getKafkaPartitionCount(TableConfig tableConfig) {
    final KafkaStreamMetadata kafkaStreamMetadata =
        new KafkaStreamMetadata(tableConfig.getIndexingConfig().getStreamConfigs());
    return PinotTableIdealStateBuilder.getPartitionCount(kafkaStreamMetadata);
  }
}
