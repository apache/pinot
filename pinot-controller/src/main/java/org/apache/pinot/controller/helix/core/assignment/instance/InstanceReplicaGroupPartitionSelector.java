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
package org.apache.pinot.controller.helix.core.assignment.instance;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.utils.Pairs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The instance replica-group/partition selector is responsible for selecting the instances for each replica-group and
 * partition.
 */
public class InstanceReplicaGroupPartitionSelector extends InstancePartitionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceReplicaGroupPartitionSelector.class);

  public InstanceReplicaGroupPartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, @Nullable InstancePartitions existingInstancePartitions, boolean minimizeDataMovement) {
    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions, minimizeDataMovement);
  }

  /**
   * Selects instances based on the replica-group/partition config, and stores the result into the given instance
   * partitions.
   */
  public void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions) {
    int numPools = poolToInstanceConfigsMap.size();
    Preconditions.checkState(numPools != 0, "No pool qualified for selection");

    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    List<Integer> pools = new ArrayList<>(poolToInstanceConfigsMap.keySet());
    pools.sort(null);
    LOGGER.info("Starting instance replica-group/partition selection for table: {} with hash: {} from pools: {}",
        _tableNameWithType, tableNameHash, pools);

    if (_replicaGroupPartitionConfig.isReplicaGroupBased()) {
      // Replica-group based selection

      int numReplicaGroups = _replicaGroupPartitionConfig.getNumReplicaGroups();
      Preconditions.checkState(numReplicaGroups > 0, "Number of replica-groups must be positive");
      Map<Integer, List<Integer>> poolToReplicaGroupIdsMap = new TreeMap<>();
      Map<Integer, Integer> replicaGroupIdToPoolMap = new TreeMap<>();
      Map<Integer, Set<String>> poolToCandidateInstancesMap = new TreeMap<>();
      Map<String, Integer> instanceToPoolMap = new HashMap<>();
      for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
        Integer pool = entry.getKey();
        List<InstanceConfig> instanceConfigsInPool = entry.getValue();
        Set<String> candidateInstances = poolToCandidateInstancesMap.computeIfAbsent(pool, k -> new LinkedHashSet<>());
        for (InstanceConfig instanceConfig : instanceConfigsInPool) {
          String instanceName = instanceConfig.getInstanceName();
          candidateInstances.add(instanceName);
          instanceToPoolMap.put(instanceName, pool);
        }
      }

      if (_minimizeDataMovement && _existingInstancePartitions != null) {
        // Keep the same pool for the replica group if it's already been used for the table.
        int existingNumPartitions = _existingInstancePartitions.getNumPartitions();
        int existingNumReplicaGroups = _existingInstancePartitions.getNumReplicaGroups();
        int numCommonReplicaGroups = Math.min(numReplicaGroups, existingNumReplicaGroups);
        for (int replicaGroupId = 0; replicaGroupId < numCommonReplicaGroups; replicaGroupId++) {
          boolean foundExistingReplicaGroup = false;
          for (int partitionId = 0; partitionId < existingNumPartitions & !foundExistingReplicaGroup; partitionId++) {
            List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
            for (String existingInstance : existingInstances) {
              Integer existingPool = instanceToPoolMap.get(existingInstance);
              if (existingPool != null & pools.contains(existingPool)) {
                poolToReplicaGroupIdsMap.computeIfAbsent(existingPool, k -> new ArrayList<>()).add(replicaGroupId);
                replicaGroupIdToPoolMap.put(replicaGroupId, existingPool);
                foundExistingReplicaGroup = true;
                break;
              }
            }
          }
        }
        // Use a min heap to track the least frequently picked pool among all the pools
        PriorityQueue<Pairs.IntPair> minHeap = new PriorityQueue<>(pools.size(), Pairs.intPairComparator());
        for (int pool : pools) {
          int numExistingReplicaGroups =
              poolToReplicaGroupIdsMap.get(pool) != null ? poolToReplicaGroupIdsMap.get(pool).size() : 0;
          minHeap.add(new Pairs.IntPair(numExistingReplicaGroups, pool));
        }
        for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
          if (replicaGroupIdToPoolMap.containsKey(replicaId)) {
            continue;
          }
          // Increment the frequency for a given pool and put it back to the min heap to rotate the pool selection.
          Pairs.IntPair pair = minHeap.remove();
          int pool = pair.getRight();
          pair.setLeft(pair.getLeft() + 1);
          minHeap.add(pair);
          poolToReplicaGroupIdsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(replicaId);
          replicaGroupIdToPoolMap.put(replicaId, pool);
        }
      } else {
        // Current default way to assign pool to replica groups.
        for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
          // Pick one pool for each replica-group based on the table name hash
          int pool = pools.get((tableNameHash + replicaId) % numPools);
          poolToReplicaGroupIdsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(replicaId);
          replicaGroupIdToPoolMap.put(replicaId, pool);
        }
      }
      LOGGER.info("Selecting {} replica-groups from pool: {} for table: {}", numReplicaGroups, poolToReplicaGroupIdsMap,
          _tableNameWithType);

      int numInstancesPerReplicaGroup = _replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup();
      if (numInstancesPerReplicaGroup > 0) {
        // Check if we have enough instances if number of instances per replica-group is configured
        for (Map.Entry<Integer, List<Integer>> entry : poolToReplicaGroupIdsMap.entrySet()) {
          int pool = entry.getKey();
          int numInstancesInPool = poolToInstanceConfigsMap.get(pool).size();
          int numInstancesToSelect = numInstancesPerReplicaGroup * entry.getValue().size();
          Preconditions.checkState(numInstancesToSelect <= numInstancesInPool,
              "Not enough qualified instances from pool: %s (%s in the pool, asked for %s)", pool, numInstancesInPool,
              numInstancesToSelect);
        }
      } else {
        // Use as many instances as possible if number of instances per replica-group is not configured
        numInstancesPerReplicaGroup = Integer.MAX_VALUE;
        for (Map.Entry<Integer, List<Integer>> entry : poolToReplicaGroupIdsMap.entrySet()) {
          int pool = entry.getKey();
          int numReplicaGroupsInPool = entry.getValue().size();
          int numInstancesInPool = poolToInstanceConfigsMap.get(pool).size();
          Preconditions.checkState(numReplicaGroupsInPool <= numInstancesInPool,
              "Not enough qualified instances from pool: %s, cannot select %s replica-groups from %s instances", pool,
              numReplicaGroupsInPool, numInstancesInPool);
          numInstancesPerReplicaGroup =
              Math.min(numInstancesPerReplicaGroup, numInstancesInPool / numReplicaGroupsInPool);
        }
      }
      LOGGER.info("Selecting {} instances per replica-group for table: {}", numInstancesPerReplicaGroup,
          _tableNameWithType);

      // Assign instances within a replica-group to one partition if not configured
      int numPartitions = _replicaGroupPartitionConfig.getNumPartitions();
      if (numPartitions <= 0) {
        numPartitions = 1;
      }
      // Assign all instances within a replica-group to each partition if not configured
      int numInstancesPerPartition = _replicaGroupPartitionConfig.getNumInstancesPerPartition();
      if (numInstancesPerPartition > 0) {
        Preconditions.checkState(numInstancesPerPartition <= numInstancesPerReplicaGroup,
            "Number of instances per partition: %s must be smaller or equal to number of instances per replica-group:"
                + " %s", numInstancesPerPartition, numInstancesPerReplicaGroup);
      } else {
        numInstancesPerPartition = numInstancesPerReplicaGroup;
      }
      LOGGER.info("Selecting {} partitions, {} instances per partition within a replica-group for table: {}",
          numPartitions, numInstancesPerPartition, _tableNameWithType);

      if (_minimizeDataMovement && _existingInstancePartitions != null) {
        // Minimize data movement.
        int existingNumPartitions = _existingInstancePartitions.getNumPartitions();
        int existingNumReplicaGroups = _existingInstancePartitions.getNumReplicaGroups();
        int numCommonReplicaGroups = Math.min(numReplicaGroups, existingNumReplicaGroups);

        Map<Integer, Set<String>> replicaGroupIdToExistingInstancesMap = new TreeMap<>();
        // Step 1: find out the replica groups and their existing instances,
        //   so that these instances can be filtered out and won't be chosen for the other replica group.
        for (int replicaGroupId = 0; replicaGroupId < numCommonReplicaGroups; replicaGroupId++) {
          Integer pool = replicaGroupIdToPoolMap.get(replicaGroupId);
          if (pool == null) {
            // Skip the replica group if it's no longer needed.
            continue;
          }

          for (int partitionId = 0; partitionId < existingNumPartitions; partitionId++) {
            List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
            replicaGroupIdToExistingInstancesMap.computeIfAbsent(replicaGroupId, k -> new HashSet<>())
                .addAll(existingInstances);
          }
        }

        for (int replicaGroupId = 0; replicaGroupId < numCommonReplicaGroups; replicaGroupId++) {
          Integer pool = replicaGroupIdToPoolMap.get(replicaGroupId);
          // Step 2: filter out instances that belong to other replica groups which should not be the candidate.
          LinkedHashSet<String> candidateInstances = new LinkedHashSet<>(poolToCandidateInstancesMap.get(pool));
          for (int otherReplicaGroupId = 0;
              otherReplicaGroupId < existingNumReplicaGroups && otherReplicaGroupId < numReplicaGroups;
              otherReplicaGroupId++) {
            if (replicaGroupId != otherReplicaGroupId) {
              candidateInstances.removeAll(replicaGroupIdToExistingInstancesMap.get(otherReplicaGroupId));
            }
          }
          LinkedHashSet<String> chosenCandidateInstances = new LinkedHashSet<>();
          for (int partitionId = 0; partitionId < existingNumPartitions; partitionId++) {
            List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
            // Step 3: figure out the missing instances and the new instances to fill their vacant positions.
            List<String> instancesToSelect =
                getInstancesWithMinimumMovement(numInstancesPerPartition, candidateInstances, existingInstances);
            chosenCandidateInstances.addAll(instancesToSelect);
            instancePartitions.setInstances(partitionId, replicaGroupId, instancesToSelect);
          }
          // Remove instances that are already been chosen.
          poolToCandidateInstancesMap.get(pool).removeAll(chosenCandidateInstances);
        }

        // If the new number of replica groups is greater than the existing number of replica groups.
        for (int replicaGroupId = existingNumReplicaGroups; replicaGroupId < numReplicaGroups; replicaGroupId++) {
          int pool = replicaGroupIdToPoolMap.get(replicaGroupId);
          LinkedHashSet<String> candidateInstances = new LinkedHashSet<>(poolToCandidateInstancesMap.get(pool));

          Set<String> chosenCandidateInstances = new HashSet<>();
          for (int partitionId = 0; partitionId < existingNumPartitions; partitionId++) {
            List<String> existingInstances = Collections.emptyList();
            List<String> instancesToSelect =
                getInstancesWithMinimumMovement(numInstancesPerPartition, candidateInstances, existingInstances);
            chosenCandidateInstances.addAll(instancesToSelect);
            instancePartitions.setInstances(partitionId, replicaGroupId, instancesToSelect);
          }
          // Remove instances that are already been chosen.
          poolToCandidateInstancesMap.get(pool).removeAll(chosenCandidateInstances);
        }
      } else {
        // Pick instances based on the sorted list of instance names.
        String[][] replicaGroupIdToInstancesMap = new String[numReplicaGroups][numInstancesPerReplicaGroup];
        for (Map.Entry<Integer, List<Integer>> entry : poolToReplicaGroupIdsMap.entrySet()) {
          List<InstanceConfig> instanceConfigsInPool = poolToInstanceConfigsMap.get(entry.getKey());
          List<Integer> replicaGroupIdsInPool = entry.getValue();

          // Use round-robin to assign instances to each replica-group so that they get instances with similar picking
          // priority
          // E.g. (within a pool, 10 instances, 2 replica-groups, 3 instances per replica-group)
          // [i0, i1, i2, i3, i4, i5, i6, i7, i8, i9]
          //  r0  r1  r0  r1  r0  r1
          int instanceIdInPool = 0;
          for (int instanceIdInReplicaGroup = 0; instanceIdInReplicaGroup < numInstancesPerReplicaGroup;
              instanceIdInReplicaGroup++) {
            for (int replicaGroupId : replicaGroupIdsInPool) {
              replicaGroupIdToInstancesMap[replicaGroupId][instanceIdInReplicaGroup] =
                  instanceConfigsInPool.get(instanceIdInPool++).getInstanceName();
            }
          }
        }

        // Assign consecutive instances within a replica-group to each partition.
        // E.g. (within a replica-group, 5 instances, 3 partitions, 3 instances per partition)
        // [i0, i1, i2, i3, i4]
        //  p0  p0  p0  p1  p1
        //  p1  p2  p2  p2
        for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
          int instanceIdInReplicaGroup = 0;
          for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<String> instancesInPartition = new ArrayList<>(numInstancesPerPartition);
            for (int instanceIdInPartition = 0; instanceIdInPartition < numInstancesPerPartition;
                instanceIdInPartition++) {
              instancesInPartition.add(replicaGroupIdToInstancesMap[replicaGroupId][instanceIdInReplicaGroup]);
              instanceIdInReplicaGroup = (instanceIdInReplicaGroup + 1) % numInstancesPerReplicaGroup;
            }
            LOGGER.info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}",
                instancesInPartition, replicaGroupId, partitionId, _tableNameWithType);
            instancePartitions.setInstances(partitionId, replicaGroupId, instancesInPartition);
          }
        }
      }
    } else {
      // Non-replica-group based selection

      // Pick one pool based on the table name hash
      int pool = pools.get(tableNameHash % numPools);
      LOGGER.info("Selecting pool: {} for table: {}", pool, _tableNameWithType);
      List<InstanceConfig> instanceConfigs = poolToInstanceConfigsMap.get(pool);
      int numInstanceConfigs = instanceConfigs.size();

      // Assign all instances if not configured
      int numInstancesToSelect = _replicaGroupPartitionConfig.getNumInstances();
      if (numInstancesToSelect > 0) {
        Preconditions.checkState(numInstancesToSelect <= numInstanceConfigs,
            "Not enough qualified instances from pool: %s (%s in the pool, asked for %s)", pool, numInstanceConfigs,
            numInstancesToSelect);
      } else {
        numInstancesToSelect = numInstanceConfigs;
      }

      List<String> instancesToSelect;
      if (_minimizeDataMovement && _existingInstancePartitions != null) {
        // Minimize data movement.
        List<String> existingInstances = _existingInstancePartitions.getInstances(0, 0);
        LinkedHashSet<String> candidateInstances = new LinkedHashSet<>();
        instanceConfigs.forEach(k -> candidateInstances.add(k.getInstanceName()));
        instancesToSelect =
            getInstancesWithMinimumMovement(numInstancesToSelect, candidateInstances, existingInstances);
      } else {
        // Select instances sequentially.
        instancesToSelect = new ArrayList<>(numInstancesToSelect);
        for (int i = 0; i < numInstancesToSelect; i++) {
          instancesToSelect.add(instanceConfigs.get(i).getInstanceName());
        }
      }
      LOGGER.info("Selecting instances: {} for table: {}", instancesToSelect, _tableNameWithType);
      // Set the instances as partition 0 replica 0
      instancePartitions.setInstances(0, 0, instancesToSelect);
    }
  }

  /**
   * Select instances with minimum movement.
   * This algorithm can solve the following scenarios:
   *    * swap an instance
   *    * add/remove replica groups
   *    * increase/decrease number of instances per replica group
   * TODO: handle the scenarios that selected pools are changed.
   * TODO: improve the algorithm by doing the following steps:
   *         1. assign the existing instances for all partitions;
   *         2. assign the vacant positions based on the partitions already assigned to each instance.
   * @param numInstancesToSelect number of instances to select
   * @param candidateInstances candidate instances to be selected
   * @param existingInstances list of existing instances
   */
  private static List<String> getInstancesWithMinimumMovement(int numInstancesToSelect,
      LinkedHashSet<String> candidateInstances, List<String> existingInstances) {
    // Initialize the list with empty positions to fill.
    List<String> instancesToSelect = new ArrayList<>(numInstancesToSelect);
    for (int i = 0; i < numInstancesToSelect; i++) {
      instancesToSelect.add(null);
    }
    Deque<String> newlyAddedInstances = new LinkedList<>();

    // Find out the existing instances that are still alive.
    Set<String> existingInstancesStillAlive = new HashSet<>();
    for (String existingInstance : existingInstances) {
      if (candidateInstances.contains(existingInstance)) {
        existingInstancesStillAlive.add(existingInstance);
      }
    }

    // Find out the newly added instances.
    for (String candidateInstance : candidateInstances) {
      if (!existingInstancesStillAlive.contains(candidateInstance)) {
        newlyAddedInstances.add(candidateInstance);
      }
    }

    int numExistingInstances = existingInstances.size();
    for (int i = 0; i < numInstancesToSelect; i++) {
      String existingInstance = i < numExistingInstances ? existingInstances.get(i) : null;
      String selectedInstance;
      if (existingInstance != null && candidateInstances.contains(existingInstance)) {
        selectedInstance = existingInstance;
        existingInstancesStillAlive.remove(selectedInstance);
      } else {
        selectedInstance = newlyAddedInstances.poll();
      }
      instancesToSelect.set(i, selectedInstance);
      // If it's an existing alive instance, or it's for a new replica group, add the new instance to the tail,
      // so that it won't be firstly chosen for the next partition.
      // For newly added instances to fill the existing replica group, the sequence cannot change;
      // otherwise there is no guarantee that same vacant position will be filled with the same new instance.
      // The 'selectedInstance' object can still be null if there is no new instances from the candidate list.
      if (selectedInstance != null && (i < numExistingInstances || existingInstances.isEmpty())) {
        candidateInstances.remove(selectedInstance);
        candidateInstances.add(selectedInstance);
      }
    }

    // If there are still some vacant positions in the instance list,
    // try to fill with instances which are either left over or newly added.
    for (int i = 0; i < instancesToSelect.size(); i++) {
      if (instancesToSelect.get(i) == null) {
        if (!existingInstancesStillAlive.isEmpty()) {
          Iterator<String> iterator = existingInstancesStillAlive.iterator();
          String existingInstanceLeftOver = iterator.next();
          instancesToSelect.set(i, existingInstanceLeftOver);
          iterator.remove();
        } else if (!newlyAddedInstances.isEmpty()) {
          // pick a new instance to fill its vacant position.
          String newInstance = newlyAddedInstances.pollFirst();
          instancesToSelect.set(i, newInstance);
        }
      }
    }
    return instancesToSelect;
  }
}
