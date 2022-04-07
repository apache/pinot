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
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An extended class of {@link InstanceReplicaGroupPartitionSelector} to minimize data movement between instances.
 * Currently the following scenarios are supported:
 *    * swap instances within a pool
 *    * add / remove instances per replica group
 *    * increase / decrease number of replica groups
 *
 * TODO: Support the remaining scenarios:
 *    * add / remove pools
 */
public class MinimizedDataMovementInstanceReplicaGroupPartitionSelector extends InstanceReplicaGroupPartitionSelector {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MinimizedDataMovementInstanceReplicaGroupPartitionSelector.class);

  private final InstancePartitions _existingInstancePartitions;

  public MinimizedDataMovementInstanceReplicaGroupPartitionSelector(
      InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig, String tableNameWithType,
      InstancePartitions existingInstancePartitions) {
    super(replicaGroupPartitionConfig, tableNameWithType);
    _existingInstancePartitions = existingInstancePartitions;
  }

  @Override
  public void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions) {
    int numPools = poolToInstanceConfigsMap.size();
    Preconditions.checkState(numPools != 0, "No pool qualified for selection");

    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    List<Integer> pools = new ArrayList<>(poolToInstanceConfigsMap.keySet());
    pools.sort(null);
    Preconditions.checkState(pools.containsAll(_existingInstancePartitions.getPoolToReplicaGroupsMap().keySet()),
        String.format("The existing pool no longer exists in ZK any more. Existing pools: %s. Latest pools: %s",
            _existingInstancePartitions.getPoolToReplicaGroupsMap().keySet(), pools));
    LOGGER.info("Starting instance replica-group/partition selection for table: {} with hash: {} from pools: {}",
        _tableNameWithType, tableNameHash, pools);

    if (_replicaGroupPartitionConfig.isReplicaGroupBased()) {
      // Replica-group based selection

      // Find out the mapping between pool and replica groups.
      int numReplicaGroups = _replicaGroupPartitionConfig.getNumReplicaGroups();
      Preconditions.checkState(numReplicaGroups > 0, "Number of replica-groups must be positive");
      Map<Integer, List<Integer>> poolToReplicaGroupIdsMap = new TreeMap<>();
      Map<Integer, Integer> replicaGroupIdToPoolMap = new TreeMap<>();
      for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
        // Pick one pool for each replica-group based on the table name hash
        int pool = pools.get((tableNameHash + replicaId) % numPools);
        poolToReplicaGroupIdsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(replicaId);
        replicaGroupIdToPoolMap.put(replicaId, pool);
      }
      LOGGER.info("Selecting {} replica-groups from pool: {} for table: {}", numReplicaGroups, poolToReplicaGroupIdsMap,
          _tableNameWithType);

      // Finalize the number of instances per replica group.
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

      // Step 1: Identify candidate instances from latest list of instance configs in ZK.
      Map<Integer, Set<String>> poolToCandidateInstancesMap = new TreeMap<>();
      for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
        Integer pool = entry.getKey();
        List<InstanceConfig> instanceConfigs = entry.getValue();
        for (InstanceConfig instanceConfig : instanceConfigs) {
          poolToCandidateInstancesMap.computeIfAbsent(pool, k -> new LinkedHashSet<>())
              .add(instanceConfig.getInstanceName());
        }
      }

      Map<Integer, Map<String, String>> poolToGoneInstancesAndReplacedInstancesMap = new TreeMap<>();
      Map<String, List<String>> existingPartitionToLatestInstancesMap = new TreeMap<>();
      Map<String, List<String>> existingPartitionToInstancesMap =
          _existingInstancePartitions.getPartitionToInstancesMap();
      Map<Integer, Set<String>> poolToExistingAliveInstancesMap = new TreeMap<>();

      int maxNumberOfInstancesPerInstancePartitionAssignment = Integer.MIN_VALUE;
      for (List<String> instances : existingPartitionToInstancesMap.values()) {
        maxNumberOfInstancesPerInstancePartitionAssignment =
            Math.max(maxNumberOfInstancesPerInstancePartitionAssignment, instances.size());
      }

      // Step 2: by reusing the existing mapping, find out the missing instances.
      for (int replicaGroupId = 0; replicaGroupId < _existingInstancePartitions.getNumReplicaGroups();
          replicaGroupId++) {
        Integer pool = replicaGroupIdToPoolMap.get(replicaGroupId);
        if (pool == null) {
          // Skip validating replica group if it's no longer needed.
          continue;
        }
        for (int partitionId = 0; partitionId < _existingInstancePartitions.getNumPartitions(); partitionId++) {
          List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
          List<String> latestInstancesInMap = existingPartitionToLatestInstancesMap
              .computeIfAbsent(partitionId + "_" + replicaGroupId, k -> new ArrayList<>());

          for (String existingInstance : existingInstances) {
            // The instance still exists in the ZK.
            if (poolToReplicaGroupIdsMap.containsKey(pool) && poolToCandidateInstancesMap.containsKey(pool)
                && poolToCandidateInstancesMap.get(pool).contains(existingInstance)) {
              poolToExistingAliveInstancesMap.computeIfAbsent(pool, k -> new HashSet<>()).add(existingInstance);
              latestInstancesInMap.add(existingInstance);
            } else {
              // The instance no longer exists
              poolToGoneInstancesAndReplacedInstancesMap.computeIfAbsent(pool, k -> new TreeMap<>())
                  .put(existingInstance, null);
              latestInstancesInMap.add(null);
            }
          }
        }
      }

      // Step 3: Find out all new instances in each pool.
      Map<Integer, Deque<String>> poolToNewCandidateInstancesMap = new TreeMap<>();
      for (Map.Entry<Integer, Set<String>> entry : poolToCandidateInstancesMap.entrySet()) {
        Integer pool = entry.getKey();
        Set<String> candidateInstancesInPool = entry.getValue();
        Set<String> existingStillAliveInstances =
            poolToExistingAliveInstancesMap.computeIfAbsent(pool, k -> new HashSet<>());
        for (String candidateInstance : candidateInstancesInPool) {
          if (!existingStillAliveInstances.contains(candidateInstance)) {
            poolToNewCandidateInstancesMap.computeIfAbsent(pool, k -> new LinkedList<>()).add(candidateInstance);
          }
        }
      }

      // Step 4: Find the 1:1 mapping between the gone instance and the new instance.
      for (Map.Entry<Integer, Map<String, String>> entry : poolToGoneInstancesAndReplacedInstancesMap.entrySet()) {
        Integer pool = entry.getKey();
        Map<String, String> goneInstanceToNewInstanceMap = entry.getValue();
        Deque<String> newInstancesInPool =
            poolToNewCandidateInstancesMap.computeIfAbsent(pool, k -> new LinkedList<>());
        goneInstanceToNewInstanceMap.replaceAll((k, v) -> {
          if (!newInstancesInPool.isEmpty()) {
            return newInstancesInPool.pollFirst();
          } else {
            return v;
          }
        });
      }

      // Step 5: Fill the vacant positions with the new instances.
      Map<String, List<String>> newInstancePartitionsAssignmentMap = new TreeMap<>();
      int finalNumInstancesPerPartition = numInstancesPerPartition;
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
          Integer pool = replicaGroupIdToPoolMap.get(replicaGroupId);
          Map<String, String> goneInstanceToNewInstanceMap =
              poolToGoneInstancesAndReplacedInstancesMap.computeIfAbsent(pool, k -> new TreeMap<>());
          Set<String> candidateInstancesMap = poolToCandidateInstancesMap.get(pool);
          String partitionToReplicaGroupKey = partitionId + "_" + replicaGroupId;
          List<String> existingInstances =
              existingPartitionToInstancesMap.computeIfAbsent(partitionToReplicaGroupKey, k -> new ArrayList<>());

          // Construct an empty list to store the latest instances.
          List<String> latestInstanceAssignment =
              newInstancePartitionsAssignmentMap.computeIfAbsent(partitionToReplicaGroupKey, k -> {
                List<String> instances = new ArrayList<>(finalNumInstancesPerPartition);
                for (int i = 0; i < finalNumInstancesPerPartition; i++) {
                  instances.add(null);
                }
                return instances;
              });

          // Traverse the existing list of instances, fill the vacant positions with new instances from the map.
          for (int i = 0; i < existingInstances.size() && i < finalNumInstancesPerPartition; i++) {
            String existingInstance = existingInstances.get(i);
            String replacedInstance = goneInstanceToNewInstanceMap.get(existingInstance);
            if (replacedInstance != null) {
              latestInstanceAssignment.set(i, replacedInstance);
              candidateInstancesMap.remove(replacedInstance);
            } else {
              // If the instance does exist in the gone map but there is no new instance to replace its position,
              // skip adding anything into the assignment.
              if (!goneInstanceToNewInstanceMap.containsKey(existingInstance)) {
                latestInstanceAssignment.set(i, existingInstance);
                candidateInstancesMap.remove(existingInstance);
              }
            }
          }
          // If the new number of instances per partition is larger than the previous one, extend the vacant positions.
          if (finalNumInstancesPerPartition > existingInstances.size()) {
            Iterator<String> candidateInstancesInPoolIterator = candidateInstancesMap.iterator();
            for (int i = existingInstances.size(); i < finalNumInstancesPerPartition; i++) {
              if (candidateInstancesInPoolIterator.hasNext()) {
                String candidateInstance = candidateInstancesInPoolIterator.next();
                latestInstanceAssignment.set(i, candidateInstance);
                candidateInstancesInPoolIterator.remove();
              }
            }
          }

          // Fill up the vacant positions if any.
          for (int i = 0; i < latestInstanceAssignment.size(); i++) {
            Iterator<String> candidateInstancesInPoolIterator = candidateInstancesMap.iterator();
            if (latestInstanceAssignment.get(i) == null) {
              if (candidateInstancesInPoolIterator.hasNext()) {
                String candidateInstance = candidateInstancesInPoolIterator.next();
                latestInstanceAssignment.set(i, candidateInstance);
                candidateInstancesInPoolIterator.remove();
              }
            }
          }

          instancePartitions.setInstances(partitionId, replicaGroupId, latestInstanceAssignment);
        }
      }

      // Persist poolToReplicaGroupsMap to ZK.
      instancePartitions.setPoolToReplicaGroupsMap(poolToReplicaGroupIdsMap);
    } else {
      // Non-replica-group based selection
      selectForNonReplicaGroupBased(poolToInstanceConfigsMap, instancePartitions);
    }
  }
}
