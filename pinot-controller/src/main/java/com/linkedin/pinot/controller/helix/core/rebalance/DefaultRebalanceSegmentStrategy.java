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
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import com.linkedin.pinot.controller.helix.core.realtime.partition.StreamPartitionAssignmentGenerator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
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

  private static final boolean DEFAULT_DRY_RUN = true;
  private static final boolean DEFAULT_INCLUDE_CONSUMING = false;

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

  /**
   * Rebalance partition assignment only for realtime tables, and if includeConsuming=true
   * @param idealState old ideal state
   * @param tableConfig table config of table tor rebalance
   * @param rebalanceUserConfig custom user configs for specific rebalance strategies
   * @return
   */
  @Override
  public PartitionAssignment rebalancePartitionAssignment(IdealState idealState, TableConfig tableConfig,
      Configuration rebalanceUserConfig) {
    PartitionAssignment newPartitionAssignment = new PartitionAssignment(tableConfig.getTableName());

    if (tableConfig.getTableType().equals(CommonConstants.Helix.TableType.REALTIME)) {
      LOGGER.info("Rebalancing stream partition assignment for table {}", tableConfig.getTableName());

      boolean includeConsuming =
          rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.INCLUDE_CONSUMING, DEFAULT_INCLUDE_CONSUMING);
      if (includeConsuming) {

        String tableNameWithType = tableConfig.getTableName();

        StreamPartitionAssignmentGenerator streamPartitionAssignmentGenerator =
            new StreamPartitionAssignmentGenerator(_propertyStore);
        PartitionAssignment streamPartitionAssignment =
            streamPartitionAssignmentGenerator.getStreamPartitionAssignment(tableNameWithType);
        int numPartitions = streamPartitionAssignment.getNumPartitions();

        TagConfig tagConfig = new TagConfig(tableConfig, _helixManager);
        List<String> consumingInstances =
            _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, tagConfig.getConsumingServerTag());

        Map<String, PartitionAssignment> tableNameToStreamPartitionAssignmentMap =
            streamPartitionAssignmentGenerator.generatePartitionAssignment(tableConfig, numPartitions,
                consumingInstances, Lists.newArrayList(tableNameWithType));
        newPartitionAssignment = tableNameToStreamPartitionAssignmentMap.get(tableNameWithType);

        boolean dryRun = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN, DEFAULT_DRY_RUN);
        if (!dryRun) {
          LOGGER.info("Updating stream partition assignment for table {}", tableNameWithType);
          streamPartitionAssignmentGenerator.writeStreamPartitionAssignment(tableNameToStreamPartitionAssignmentMap);
        } else {
          LOGGER.info("Dry run. Skip writing stream partition assignment to property store");
          LOGGER.info("Partition assignment for table {} is {}", tableNameWithType, newPartitionAssignment);
        }
      } else {
        LOGGER.info("includeConsuming = false. No need to rebalance partition assignment for {}",
            tableConfig.getTableType());
      }
    }
    return newPartitionAssignment;
  }

  /**
   * If realtime table and includeConsuming=true, rebalance consuming segments. NewPartitionAssignment will be used only in this case
   * Always rebalance completed (online) segments, NewPartitionAssignment unused in this case
   * @param idealState old ideal state
   * @param tableConfig table config of table tor rebalance
   * @param rebalanceUserConfig custom user configs for specific rebalance strategies
   * @param newPartitionAssignment new rebalanced partition assignments as part of the resource rebalance
   * @return
   */
  @Override
  public IdealState rebalanceIdealState(IdealState idealState, TableConfig tableConfig,
      Configuration rebalanceUserConfig, PartitionAssignment newPartitionAssignment) {

    String tableNameWithType = tableConfig.getTableName();
    CommonConstants.Helix.TableType tableType = tableConfig.getTableType();
    LOGGER.info("Rebalancing ideal state for table {}", tableNameWithType);

    // if realtime and includeConsuming, then rebalance consuming segments
    if (tableType.equals(CommonConstants.Helix.TableType.REALTIME)) {
      boolean includeConsuming =
          rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.INCLUDE_CONSUMING, DEFAULT_INCLUDE_CONSUMING);
      if (includeConsuming) {
        rebalanceConsumingSegments(idealState, newPartitionAssignment);
      }
    }

    // get target num replicas
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

    // always rebalance serving segments
    rebalanceServingSegments(idealState, tableConfig, targetNumReplicas);

    // update if not dryRun
    boolean dryRun = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN, DEFAULT_DRY_RUN);
    if (!dryRun) {
      LOGGER.info("Updating ideal state for table {}", tableNameWithType);
      updateIdealState(tableNameWithType, targetNumReplicas, idealState.getRecord().getMapFields());
    } else {
      LOGGER.info("Dry run. Skip writing ideal state");
    }
    return idealState;
  }

  /**
   * Rebalances consuming segments based on stream partition assignment
   * @param idealState
   * @param newPartitionAssignment
   */
  private void rebalanceConsumingSegments(IdealState idealState, PartitionAssignment newPartitionAssignment) {
    LOGGER.info("Rebalancing consuming segments for table {}", idealState.getResourceName());

    // for each partition, the segment with latest sequence number should be in CONSUMING
    Map<Integer, LLCSegmentName> partitionIdToLatestSegment = new HashMap<>(newPartitionAssignment.getNumPartitions());
    for (String segmentName : idealState.getPartitionSet()) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      LLCSegmentName latestSegmentForPartition = partitionIdToLatestSegment.get(partitionId);
      if (latestSegmentForPartition == null
          || llcSegmentName.getSequenceNumber() > latestSegmentForPartition.getSequenceNumber()) {
        partitionIdToLatestSegment.put(partitionId, llcSegmentName);
      }
    }

    // update ideal state of consuming segments, based on new stream partition assignment
    Map<String, List<String>> partitionToInstanceMap = newPartitionAssignment.getPartitionToInstances();
    for (LLCSegmentName llcSegmentName : partitionIdToLatestSegment.values()) {
      int partitionId = llcSegmentName.getPartitionId();
      Map<String, String> instanceStateMap = new HashMap<>();
      for (String instance : partitionToInstanceMap.get(String.valueOf(partitionId))) {
        instanceStateMap.put(instance, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
      }
      idealState.setInstanceStateMap(llcSegmentName.getSegmentName(), instanceStateMap);
    }
  }

  /**
   * Rebalances serving segments based on autorebalance strategy
   * @param idealState
   * @param tableConfig
   * @param targetNumReplicas
   * @return
   */
  protected IdealState rebalanceServingSegments(IdealState idealState, TableConfig tableConfig, int targetNumReplicas) {

    LOGGER.info("Rebalancing serving segments for table {}", tableConfig.getTableName());

    int numReplicasInIdealState = Integer.parseInt(idealState.getReplicas());

    if (numReplicasInIdealState > targetNumReplicas) {
      // We need to reduce the number of replicas per helix partition.

      for (String segmentName : idealState.getPartitionSet()) {
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
        if (instanceStateMap.size() > targetNumReplicas) {
          Set<String> keys = instanceStateMap.keySet();
          while (instanceStateMap.size() > targetNumReplicas) {
            instanceStateMap.remove(keys.iterator().next());
          }
        } else if (instanceStateMap.size() < targetNumReplicas) {
          LOGGER.warn("Table {}, segment {} has {} replicas, less than {} (requested number of replicas)",
              idealState.getResourceName(), segmentName, instanceStateMap.size(), targetNumReplicas);
        }
      }
    } else {
      // Number of replicas is either the same or higher, so invoke Helix rebalancer.

      final Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

      Map<String, Map<String, String>> removedEntries = new LinkedHashMap<>();
      if (tableConfig.getTableType().equals(CommonConstants.Helix.TableType.REALTIME)) {
        filterSegmentsForRealtimeRebalance(mapFields, removedEntries);
      }

      if (!mapFields.isEmpty()) {

        String tableNameWithType = tableConfig.getTableName();

        LinkedHashMap<String, Integer> states = new LinkedHashMap<>();
        List<String> partitions = Lists.newArrayList(idealState.getPartitionSet());
        states.put(RealtimeSegmentOnlineOfflineStateModel.OFFLINE, 0);
        states.put(RealtimeSegmentOnlineOfflineStateModel.ONLINE, targetNumReplicas);
        Set<String> currentHosts = new HashSet<>();
        for (String segment : mapFields.keySet()) {
          currentHosts.addAll(mapFields.get(segment).keySet());
        }
        TagConfig tagConfig = new TagConfig(tableConfig, _helixManager);
        List<String> servingInstances =
            _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, tagConfig.getCompletedServerTag());
        List<String> enabledServingInstances =
            HelixHelper.getEnabledInstancesWithTag(_helixAdmin, _helixClusterName, tagConfig.getCompletedServerTag());

        AutoRebalanceStrategy rebalanceStrategy = new AutoRebalanceStrategy(tableNameWithType, partitions, states);

        LOGGER.info("Current nodes for table {}: {}", tableNameWithType, currentHosts);
        LOGGER.info("New nodes for table {}: {}", tableNameWithType, servingInstances);
        LOGGER.info("Enabled nodes for table: {} {}", tableNameWithType, enabledServingInstances);
        ZNRecord newZnRecord =
            rebalanceStrategy.computePartitionAssignment(servingInstances, enabledServingInstances, mapFields,
                new ClusterDataCache());
        final Map<String, Map<String, String>> newMapping = newZnRecord.getMapFields();
        for (Map.Entry<String, Map<String, String>> entry : newMapping.entrySet()) {
          idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
        }
      }

      // If we removed any entries, add them back here
      for (Map.Entry<String, Map<String, String>> entry : removedEntries.entrySet()) {
        idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
      }
    }
    idealState.setReplicas(Integer.toString(targetNumReplicas));
    return idealState;
  }

  /**
   * Remove all segments from the ideal state, other than LLC ONLINE segments. We only want LLC ONLINE segments for this rebalance
   * We do not want to rebalance CONSUMING segments as part of this rebalance, it is done separately
   *
   * If any segments were OFFLINE instead of ONLINE, we will not rebalance them,
   * as that state was likely achieved because of some manual operation
   * @param mapFields
   * @param removedEntries
   */
  private void filterSegmentsForRealtimeRebalance(Map<String, Map<String, String>> mapFields,
      Map<String, Map<String, String>> removedEntries) {

    Iterator<Map.Entry<String, Map<String, String>>> mapFieldsIterator = mapFields.entrySet().iterator();
    while (mapFieldsIterator.hasNext()) {

      // Only keep LLC segments in ONLINE state for rebalance
      Map.Entry<String, Map<String, String>> entry = mapFieldsIterator.next();
      final String segmentName = entry.getKey();
      boolean keep = false;
      if (SegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        Map<String, String> instanceStateMap = entry.getValue();
        if (instanceStateMap.values()
            .contains(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)) {
          keep = true;
        }
      }
      if (!keep) {
        removedEntries.put(segmentName, entry.getValue());
        mapFieldsIterator.remove();
      }
    }
  }
}
