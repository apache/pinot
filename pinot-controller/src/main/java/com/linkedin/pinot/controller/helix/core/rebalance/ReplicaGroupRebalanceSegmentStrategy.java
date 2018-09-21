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

import com.google.common.base.Function;
import com.linkedin.pinot.common.config.OfflineTagConfig;
import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignment;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Replica group aware rebalance segment strategy.
 */
public class ReplicaGroupRebalanceSegmentStrategy implements RebalanceSegmentStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaGroupRebalanceSegmentStrategy.class);

  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _helixClusterName;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public ReplicaGroupRebalanceSegmentStrategy(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixAdmin = helixManager.getClusterManagmentTool();
    _helixClusterName = helixManager.getClusterName();
    _propertyStore = helixManager.getHelixPropertyStore();
  }

  /**
   * Rebalance partition assignment for replica group tables.
   * @param idealState old ideal state
   * @param tableConfig table config of table tor rebalance
   * @param rebalanceUserConfig custom user configs for specific rebalance strategies
   * @return a rebalanced replica group partition assignment
   */
  @Override
  public PartitionAssignment rebalancePartitionAssignment(IdealState idealState, TableConfig tableConfig,
      Configuration rebalanceUserConfig) throws InvalidConfigException {
    // Currently, only offline table is supported
    if (tableConfig.getTableType() == CommonConstants.Helix.TableType.REALTIME) {
      throw new InvalidConfigException("Realtime table is not supported by replica group rebalancer");
    }

    String tableNameWithType = tableConfig.getTableName();
    LOGGER.info("Rebalancing replica group partition assignment for table {}", tableNameWithType);

    ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(_propertyStore);

    // Compute new replica group partition assignment
    ReplicaGroupPartitionAssignment newPartitionAssignment =
        computeNewReplicaGroupMapping(tableConfig, partitionAssignmentGenerator);

    boolean dryRun = rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN,
        RebalanceUserConfigConstants.DEFAULT_DRY_RUN);
    if (!dryRun) {
      LOGGER.info("Updating replica group partition assignment for table {}", tableNameWithType);
      partitionAssignmentGenerator.writeReplicaGroupPartitionAssignment(newPartitionAssignment);
    } else {
      LOGGER.info("Dry run. Skip writing replica group partition assignment to property store");
    }

    return newPartitionAssignment;
  }

  /**
   * Rebalance the segments for replica group tables.
   * @param idealState old ideal state
   * @param tableConfig table config of table tor rebalance
   * @param rebalanceUserConfig custom user configs for specific rebalance strategies
   * @param newPartitionAssignment new rebalaned partition assignments as part of the resource rebalance
   * @return a rebalanced idealstate
   */
  @Override
  public IdealState getRebalancedIdealState(IdealState idealState, TableConfig tableConfig,
      Configuration rebalanceUserConfig, PartitionAssignment newPartitionAssignment) throws InvalidConfigException {
    // Currently, only offline table is supported
    if (tableConfig.getTableType() == CommonConstants.Helix.TableType.REALTIME) {
      throw new InvalidConfigException("Realtime table is not supported by replica group rebalancer");
    }
    ReplicaGroupPartitionAssignment newReplicaGroupPartitionAssignment =
        (ReplicaGroupPartitionAssignment) newPartitionAssignment;
    return rebalanceSegments(idealState, tableConfig, newReplicaGroupPartitionAssignment);
  }

  /**
   * Compute the new replica group mapping based on the new configurations
   * @param tableConfig a talbe config
   * @param partitionAssignmentGenerator partition assignment generator
   * @return new replica group partition assignment
   */
  private ReplicaGroupPartitionAssignment computeNewReplicaGroupMapping(TableConfig tableConfig,
      ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator) throws InvalidConfigException {
    ReplicaGroupStrategyConfig replicaGroupConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();

    // If no replica group config is available in table config, we cannot perform the rebalance algorithm
    if (replicaGroupConfig == null) {
      throw new InvalidConfigException("This table is not using replica group segment assignment");
    }

    // Currently, only table level replica group rebalance is supported
    if (replicaGroupConfig.getPartitionColumn() != null) {
      throw new InvalidConfigException("Partition level replica group rebalance is not supported");
    }

    // Fetch the information required for computing new replica group partition assignment
    int targetNumInstancesPerPartition = replicaGroupConfig.getNumInstancesPerPartition();
    int targetNumReplicaGroup = tableConfig.getValidationConfig().getReplicationNumber();

    OfflineTagConfig offlineTagConfig = new OfflineTagConfig(tableConfig);
    List<String> serverInstances =
        HelixHelper.getInstancesWithTag(_helixManager, offlineTagConfig.getOfflineServerTag());

    // Perform the basic validation
    if (targetNumReplicaGroup <= 0 || targetNumInstancesPerPartition <= 0
        || targetNumReplicaGroup * targetNumInstancesPerPartition > serverInstances.size()) {
      throw new InvalidConfigException(
          "Invalid input config (numReplicaGroup: " + targetNumReplicaGroup + ", " + "numInstancesPerPartition: "
              + targetNumInstancesPerPartition + ", numServers: " + serverInstances.size() + ")");
    }

    // Check that the partition assignment exists
    String tableNameWithType = tableConfig.getTableName();
    ReplicaGroupPartitionAssignment oldReplicaGroupPartitionAssignment =
        partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    if (oldReplicaGroupPartitionAssignment == null) {
      throw new InvalidConfigException(
          "Replica group partition assignment does not exist for " + tableNameWithType);
    }

    // Fetch the previous replica group configurations
    List<String> oldServerInstances = oldReplicaGroupPartitionAssignment.getAllInstances();
    int oldNumReplicaGroup = oldReplicaGroupPartitionAssignment.getNumReplicaGroups();
    int oldNumInstancesPerPartition = oldServerInstances.size() / oldNumReplicaGroup;

    // Compute added and removed servers
    List<String> addedServers = new ArrayList<>(serverInstances);
    addedServers.removeAll(oldServerInstances);
    List<String> removedServers = new ArrayList<>(oldServerInstances);
    removedServers.removeAll(serverInstances);

    // Create the new replica group partition assignment
    ReplicaGroupPartitionAssignment newReplicaGroupPartitionAssignment =
        new ReplicaGroupPartitionAssignment(oldReplicaGroupPartitionAssignment.getTableName());

    // In case of replacement, create the removed to added server mapping
    Map<String, String> oldToNewServerMapping = new HashMap<>();
    if (addedServers.size() == removedServers.size()) {
      for (int i = 0; i < addedServers.size(); i++) {
        oldToNewServerMapping.put(removedServers.get(i), addedServers.get(i));
      }
    }

    // Compute rebalance type
    ReplicaGroupRebalanceType rebalanceType =
        computeRebalanceType(oldNumInstancesPerPartition, oldNumReplicaGroup, targetNumReplicaGroup,
            targetNumInstancesPerPartition, addedServers, removedServers);
    LOGGER.info("Replica group rebalance type: " + rebalanceType);

    // For now, we don't support the rebalance for partition level replica group so "numPartitions" will always be 1.
    for (int partitionId = 0; partitionId < oldReplicaGroupPartitionAssignment.getNumPartitions(); partitionId++) {
      int currentNewReplicaGroupId = 0;
      for (int groupId = 0; groupId < oldNumReplicaGroup; groupId++) {
        List<String> oldReplicaGroup =
            oldReplicaGroupPartitionAssignment.getInstancesfromReplicaGroup(partitionId, groupId);
        List<String> newReplicaGroup = new ArrayList<>();
        boolean removeGroup = false;

        // Based on the rebalance type, compute the new replica group partition assignment accordingly
        switch (rebalanceType) {
          case REPLACE:
            // Swap the removed server with the added one.
            for (String oldServer : oldReplicaGroup) {
              if (!oldToNewServerMapping.containsKey(oldServer)) {
                newReplicaGroup.add(oldServer);
              } else {
                newReplicaGroup.add(oldToNewServerMapping.get(oldServer));
              }
            }
            break;
          case ADD_SERVER:
            newReplicaGroup.addAll(oldReplicaGroup);
            // Assign new servers to the replica group
            for (int serverIndex = 0; serverIndex < addedServers.size(); serverIndex++) {
              if (serverIndex % targetNumReplicaGroup == groupId) {
                newReplicaGroup.add(addedServers.get(serverIndex));
              }
            }
            break;
          case REMOVE_SERVER:
            // Only add the servers that are not in the removed list
            newReplicaGroup.addAll(oldReplicaGroup);
            newReplicaGroup.removeAll(removedServers);
            break;
          case ADD_REPLICA_GROUP:
            // Add all servers for original replica groups and add new replica groups later
            newReplicaGroup.addAll(oldReplicaGroup);
            break;
          case REMOVE_REPLICA_GROUP:
            newReplicaGroup.addAll(oldReplicaGroup);
            // mark the group if this is the replica group that needs to be removed
            if (removedServers.containsAll(oldReplicaGroup)) {
              removeGroup = true;
            }
            break;
          default:
            String errorMessage =
                "Not supported replica group rebalance operation. Need to check server tags and replica group config to"
                    + " make sure only one maintenance step is asked. ( oldNumInstancesPerPatition: "
                    + oldNumInstancesPerPartition + ", targetNumInstancesPerPartition: "
                    + targetNumInstancesPerPartition + ", oldNumReplicaGroup: " + oldNumReplicaGroup
                    + ", targetNumReplicaGroup: " + targetNumReplicaGroup + ", numAddedServers: " + addedServers.size()
                    + ", numRemovedServers: " + removedServers.size() + " )";
            LOGGER.info(errorMessage);
            throw new InvalidConfigException(errorMessage);
        }
        if (!removeGroup) {
          LOGGER.info("Setting new replica group ( partitionId: " + partitionId + ", replicaGroupId: " + groupId
              + ", server list: " + StringUtils.join(",", newReplicaGroup));
          newReplicaGroupPartitionAssignment.setInstancesToReplicaGroup(partitionId, currentNewReplicaGroupId++, newReplicaGroup);
        }
      }

      // Adding new replica groups if needed
      int index = 0;
      for (int newGroupId = currentNewReplicaGroupId; newGroupId < targetNumReplicaGroup; newGroupId++) {
        List<String> newReplicaGroup = new ArrayList<>();
        while (newReplicaGroup.size() < targetNumInstancesPerPartition) {
          newReplicaGroup.add(addedServers.get(index));
          index++;
        }
        newReplicaGroupPartitionAssignment.setInstancesToReplicaGroup(partitionId, newGroupId, newReplicaGroup);
      }
    }

    return newReplicaGroupPartitionAssignment;
  }

  /**
   * Given the list of added/removed servers and the old and new replica group configurations, compute the
   * type of update (e.g. replace server, add servers to each replica group, add replica groups..)
   *
   * @return the update type
   */
  private ReplicaGroupRebalanceType computeRebalanceType(int oldNumInstancesPerPartition, int oldNumReplicaGroup,
      int targetNumReplicaGroup, int targetNumInstancesPerPartition, List<String> addedServers,
      List<String> removedServers) {
    boolean sameNumInstancesPerPartition = oldNumInstancesPerPartition == targetNumInstancesPerPartition;
    boolean sameNumReplicaGroup = oldNumReplicaGroup == targetNumReplicaGroup;
    boolean isAddedServersSizeZero = addedServers.size() == 0;
    boolean isRemovedServersSizeZero = removedServers.size() == 0;

    if (sameNumInstancesPerPartition && sameNumReplicaGroup && addedServers.size() == removedServers.size()) {
      return ReplicaGroupRebalanceType.REPLACE;
    } else if (sameNumInstancesPerPartition) {
      if (oldNumReplicaGroup < targetNumReplicaGroup && !isAddedServersSizeZero && isRemovedServersSizeZero
          && addedServers.size() % targetNumInstancesPerPartition == 0) {
        return ReplicaGroupRebalanceType.ADD_REPLICA_GROUP;
      } else if (oldNumReplicaGroup > targetNumReplicaGroup && isAddedServersSizeZero && !isRemovedServersSizeZero
          && removedServers.size() % targetNumInstancesPerPartition == 0) {
        return ReplicaGroupRebalanceType.REMOVE_REPLICA_GROUP;
      }
    } else if (sameNumReplicaGroup) {
      if (oldNumInstancesPerPartition < targetNumInstancesPerPartition && isRemovedServersSizeZero
          && !isAddedServersSizeZero && addedServers.size() % targetNumReplicaGroup == 0) {
        return ReplicaGroupRebalanceType.ADD_SERVER;
      } else if (oldNumInstancesPerPartition > targetNumInstancesPerPartition && !isRemovedServersSizeZero
          && isAddedServersSizeZero && removedServers.size() % targetNumReplicaGroup == 0) {
        return ReplicaGroupRebalanceType.REMOVE_SERVER;
      }
    }
    return ReplicaGroupRebalanceType.UNSUPPORTED;
  }

  /**
   * Modifies in-memory idealstate to rebalance segments for table with replica-group based segment assignment
   * @param idealState old idealstate
   * @param tableConfig a table config
   * @param replicaGroupPartitionAssignment a replica group partition assignment
   * @return a rebalanced idealstate
   */
  private IdealState rebalanceSegments(IdealState idealState, TableConfig tableConfig,
      ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment) {

    Map<String, Map<String, String>> segmentToServerMapping = idealState.getRecord().getMapFields();
    Map<String, LinkedList<String>> serverToSegments = buildServerToSegmentMapping(segmentToServerMapping);

    List<String> oldServerInstances = new ArrayList<>(serverToSegments.keySet());
    List<String> serverInstances = replicaGroupPartitionAssignment.getAllInstances();

    // Compute added and removed servers
    List<String> addedServers = new ArrayList<>(serverInstances);
    addedServers.removeAll(oldServerInstances);
    List<String> removedServers = new ArrayList<>(oldServerInstances);
    removedServers.removeAll(serverInstances);

    // Add servers to the mapping
    for (String server : addedServers) {
      serverToSegments.put(server, new LinkedList<String>());
    }

    // Remove servers from the mapping
    for (String server : removedServers) {
      serverToSegments.remove(server);
    }

    // Check if rebalance can cause the data inconsistency
    Set<String> segmentsToCover = segmentToServerMapping.keySet();
    Set<String> coveredSegments = new HashSet<>();
    for (Map.Entry<String, LinkedList<String>> entry : serverToSegments.entrySet()) {
      coveredSegments.addAll(entry.getValue());
    }

    coveredSegments.removeAll(segmentsToCover);
    if (!coveredSegments.isEmpty()) {
      LOGGER.warn("Some segments may temporarily be unavailable during the rebalance. "
          + "This may cause incorrect answer for the query.");
    }

    // Fetch replica group configs
    ReplicaGroupStrategyConfig replicaGroupConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    boolean mirrorAssignment = replicaGroupConfig.getMirrorAssignmentAcrossReplicaGroups();
    int numPartitions = replicaGroupPartitionAssignment.getNumPartitions();
    int numReplicaGroups = replicaGroupPartitionAssignment.getNumReplicaGroups();

    // For now, we don't support for rebalancing partition level replica group so "numPartitions" will be 1.
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      List<String> referenceReplicaGroup = new ArrayList<>();
      for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
        List<String> serversInReplicaGroup =
            replicaGroupPartitionAssignment.getInstancesfromReplicaGroup(partitionId, replicaId);
        if (replicaId == 0) {
          // We need to keep the first replica group in case of mirroring.
          referenceReplicaGroup.addAll(serversInReplicaGroup);
        } else if (mirrorAssignment) {
          // Copy the segment assignment from the reference replica group
          for (int i = 0; i < serversInReplicaGroup.size(); i++) {
            serverToSegments.put(serversInReplicaGroup.get(i), serverToSegments.get(referenceReplicaGroup.get(i)));
          }
          continue;
        }

        // Uniformly distribute the segments among servers in a replica group
        rebalanceReplicaGroup(serversInReplicaGroup, serverToSegments, segmentsToCover);
      }
    }

    // Update Idealstate with rebalanced segment assignment
    Map<String, Map<String, String>> serverToSegmentsMapping = buildSegmentToServerMapping(serverToSegments);
    for (Map.Entry<String, Map<String, String>> entry : serverToSegmentsMapping.entrySet()) {
      idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
    }
    idealState.setReplicas(Integer.toString(numReplicaGroups));

    return idealState;
  }

  /**
   * Rebalances segments and updates the idealstate in Helix
   * @param tableConfig a table config
   * @param replicaGroupPartitionAssignment a replica group partition assignment
   * @return a rebalanced idealstate
   */
  private IdealState rebalanceSegmentsAndUpdateIdealState(final TableConfig tableConfig,
      final ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment) {
    final Function<IdealState, IdealState> updaterFunction = new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        return rebalanceSegments(idealState, tableConfig, replicaGroupPartitionAssignment);
      }
    };
    HelixHelper.updateIdealState(_helixManager, tableConfig.getTableName(), updaterFunction,
        RetryPolicies.exponentialBackoffRetryPolicy(5, 1000, 2.0f));
    return _helixAdmin.getResourceIdealState(_helixClusterName, tableConfig.getTableName());
  }

  /**
   * Uniformly distribute segments across servers in a replica group. It adopts a simple algorithm that pre-computes
   * the number of segments per server after rebalance and tries to assign/remove segments to/from a server until it
   * becomes to have the correct number of segments.
   *
   * @param serversInReplicaGroup A list of servers within the same replica group
   * @param serverToSegments A Mapping of servers to their segments
   */
  private void rebalanceReplicaGroup(List<String> serversInReplicaGroup,
      Map<String, LinkedList<String>> serverToSegments, Set<String> segmentsToCover) {
    // Make sure that all the segments are covered only once within a replica group.
    Set<String> currentCoveredSegments = new HashSet<>();
    for (String server : serversInReplicaGroup) {
      Iterator<String> segmentIter = serverToSegments.get(server).iterator();
      while (segmentIter.hasNext()) {
        String segment = segmentIter.next();
        if (currentCoveredSegments.contains(segment)) {
          segmentIter.remove();
        } else {
          currentCoveredSegments.add(segment);
        }
      }
    }

    // Compute the segments to add
    LinkedList<String> segmentsToAdd = new LinkedList<>(segmentsToCover);
    segmentsToAdd.removeAll(currentCoveredSegments);

    // Compute the number of segments per server after rebalance than numSegmentsPerServer
    int numSegmentsPerServer = segmentsToCover.size() / serversInReplicaGroup.size();

    // Remove segments from servers that has more segments
    for (String server : serversInReplicaGroup) {
      LinkedList<String> segmentsInServer = serverToSegments.get(server);
      int segmentToMove = numSegmentsPerServer - segmentsInServer.size();
      if (segmentToMove < 0) {
        // Server has more segments than needed, remove segments from this server
        for (int i = 0; i < Math.abs(segmentToMove); i++) {
          segmentsToAdd.add(segmentsInServer.pop());
        }
      }
    }

    // Add segments to servers that has less segments than numSegmentsPerServer
    for (String server : serversInReplicaGroup) {
      LinkedList<String> segmentsInServer = serverToSegments.get(server);
      int segmentToMove = numSegmentsPerServer - segmentsInServer.size();
      if (segmentToMove > 0) {
        // Server has less segments than needed, add segments from this server
        for (int i = 0; i < segmentToMove; i++) {
          segmentsInServer.add(segmentsToAdd.pop());
        }
      }
    }

    // Handling the remainder of segments to add
    int count = 0;
    while (!segmentsToAdd.isEmpty()) {
      int serverIndex = count % serversInReplicaGroup.size();
      serverToSegments.get(serversInReplicaGroup.get(serverIndex)).add(segmentsToAdd.pop());
      count++;
    }
  }

  /**
   * Build the mapping of servers to their associated segments given a mapping of segments to their associated servers.
   * @param segmentToServerMapping a mapping of segments to its associated servers
   * @return a mapping of a server to its associated segments
   */
  private Map<String, LinkedList<String>> buildServerToSegmentMapping(
      Map<String, Map<String, String>> segmentToServerMapping) {
    Map<String, LinkedList<String>> serverToSegments = new HashMap<>();
    for (String segment : segmentToServerMapping.keySet()) {
      for (String server : segmentToServerMapping.get(segment).keySet()) {
        if (!serverToSegments.containsKey(server)) {
          serverToSegments.put(server, new LinkedList<String>());
        }
        serverToSegments.get(server).add(segment);
      }
    }
    return serverToSegments;
  }

  /**
   * Build the mapping of segments to their associated servers given a mapping of servers to their associated segments.
   * @param serverToSegments a mapping of servers to their associated segments
   * @return a mapping of segments to their associated servers
   */
  private Map<String, Map<String, String>> buildSegmentToServerMapping(
      Map<String, LinkedList<String>> serverToSegments) {
    Map<String, Map<String, String>> segmentsToServerMapping = new HashMap<>();
    for (Map.Entry<String, LinkedList<String>> entry : serverToSegments.entrySet()) {
      String server = entry.getKey();
      for (String segment : entry.getValue()) {
        if (!segmentsToServerMapping.containsKey(segment)) {
          segmentsToServerMapping.put(segment, new HashMap<String, String>());
        }
        segmentsToServerMapping.get(segment)
            .put(server, CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE);
      }
    }
    return segmentsToServerMapping;
  }

  public enum ReplicaGroupRebalanceType {
    REPLACE, ADD_SERVER, ADD_REPLICA_GROUP, REMOVE_SERVER, REMOVE_REPLICA_GROUP, UNSUPPORTED
  }
}
