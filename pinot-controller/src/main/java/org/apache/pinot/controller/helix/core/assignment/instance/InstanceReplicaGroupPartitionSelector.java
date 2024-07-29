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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
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

  @Override
  public void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions) {
    int numPools = poolToInstanceConfigsMap.size();
    Preconditions.checkState(numPools != 0, "No pool qualified for selection");

    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    List<Integer> pools = new ArrayList<>(poolToInstanceConfigsMap.keySet());
    pools.sort(null);
    LOGGER.info("Starting instance replica-group/partition selection for table: {} with hash: {} from pools: {}, "
        + "minimize data movement: {}", _tableNameWithType, tableNameHash, pools, _minimizeDataMovement);

    if (_replicaGroupPartitionConfig.isReplicaGroupBased()) {
      if (_minimizeDataMovement) {
        replicaGroupBasedMinimumMovement(poolToInstanceConfigsMap, instancePartitions, pools, tableNameHash);
      } else {
        replicaGroupBasedSimple(poolToInstanceConfigsMap, instancePartitions, pools, tableNameHash);
      }
    } else {
      nonReplicaGroupBased(poolToInstanceConfigsMap, instancePartitions, pools, tableNameHash);
    }
  }

  private void nonReplicaGroupBased(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions, List<Integer> pools, int tableNameHash) {
    // Pick one pool based on the table name hash
    int pool = pools.get(Math.abs(tableNameHash % pools.size()));
    LOGGER.info("Selecting pool: {} for table: {}", pool, _tableNameWithType);
    List<InstanceConfig> instanceConfigs = poolToInstanceConfigsMap.get(pool);
    int numInstances = instanceConfigs.size();

    // Assign all instances if not configured
    int numInstancesToSelect = _replicaGroupPartitionConfig.getNumInstances();
    if (numInstancesToSelect > 0) {
      Preconditions.checkState(numInstancesToSelect <= numInstances,
          "Not enough qualified instances from pool: %s (%s in the pool, asked for %s)", pool, numInstances,
          numInstancesToSelect);
    } else {
      numInstancesToSelect = numInstances;
    }

    List<String> instancesToSelect;
    if (_minimizeDataMovement) {
      List<String> existingInstances = _existingInstancePartitions.getInstances(0, 0);
      LinkedHashSet<String> candidateInstances = Sets.newLinkedHashSetWithExpectedSize(instanceConfigs.size());
      instanceConfigs.forEach(k -> candidateInstances.add(k.getInstanceName()));
      instancesToSelect =
          selectInstancesWithMinimumMovement(numInstancesToSelect, candidateInstances, existingInstances);
      LOGGER.info("Selecting instances: {} for table: {}, existing instances: {}", instancesToSelect,
          _tableNameWithType, existingInstances);
    } else {
      instancesToSelect = new ArrayList<>(numInstancesToSelect);
      for (int i = 0; i < numInstancesToSelect; i++) {
        instancesToSelect.add(instanceConfigs.get(i).getInstanceName());
      }
      LOGGER.info("Selecting instances: {} for table: {}", instancesToSelect, _tableNameWithType);
    }
    // Set the instances as partition 0 replica 0
    instancePartitions.setInstances(0, 0, instancesToSelect);
  }

  /**
   * Selects the instances with minimum movement.
   * For each instance in the existing instances, if it is still alive, keep it in the same position. Then fill the
   * vacant positions with the remaining candidate instances.
   * NOTE: This method will modify the candidate instances.
   */
  private static List<String> selectInstancesWithMinimumMovement(int numInstancesToSelect,
      LinkedHashSet<String> candidateInstances, List<String> existingInstances) {
    // Initialize the list with empty positions to fill
    List<String> instancesToSelect = new ArrayList<>(numInstancesToSelect);
    for (int i = 0; i < numInstancesToSelect; i++) {
      instancesToSelect.add(null);
    }

    // Keep the existing instances that are still alive
    int numInstancesToCheck = Math.min(numInstancesToSelect, existingInstances.size());
    for (int i = 0; i < numInstancesToCheck; i++) {
      String existingInstance = existingInstances.get(i);
      if (candidateInstances.remove(existingInstance)) {
        instancesToSelect.set(i, existingInstance);
      }
    }

    // Fill the vacant positions with the remaining candidate instances
    Iterator<String> iterator = candidateInstances.iterator();
    for (int i = 0; i < numInstancesToSelect; i++) {
      if (instancesToSelect.get(i) == null) {
        instancesToSelect.set(i, iterator.next());
      }
    }

    return instancesToSelect;
  }

  private void replicaGroupBasedSimple(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions, List<Integer> pools, int tableNameHash) {
    int numPools = pools.size();
    int numReplicaGroups = getNumReplicaGroups();

    // Pick one pool for each replica-group based on the table name hash
    Map<Integer, List<Integer>> poolToReplicaGroupIdsMap = new TreeMap<>();
    int startIndex = Math.abs(tableNameHash % numPools);
    for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
      int pool = pools.get((startIndex + replicaGroupId) % numPools);
      poolToReplicaGroupIdsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(replicaGroupId);
    }
    LOGGER.info("Selecting {} replica-groups from pool: {} for table: {}", numReplicaGroups, poolToReplicaGroupIdsMap,
        _tableNameWithType);

    int numInstancesPerReplicaGroup =
        getNumInstancesPerReplicaGroup(poolToInstanceConfigsMap, poolToReplicaGroupIdsMap);
    LOGGER.info("Selecting {} instances per replica-group for table: {}", numInstancesPerReplicaGroup,
        _tableNameWithType);
    int numPartitions = getNumPartitions();
    int numInstancesPerPartition = getNumInstancesPerPartition(numInstancesPerReplicaGroup);
    LOGGER.info("Selecting {} partitions, {} instances per partition within a replica-group for table: {}",
        numPartitions, numInstancesPerPartition, _tableNameWithType);

    // Pick instances based on the sorted list of instance names
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

    // Assign consecutive instances within a replica-group to each partition
    // E.g. (within a replica-group, 5 instances, 3 partitions, 3 instances per partition)
    // [i0, i1, i2, i3, i4]
    //  p0  p0  p0  p1  p1
    //  p1  p2  p2  p2
    for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
      String[] instancesInReplicaGroup = replicaGroupIdToInstancesMap[replicaGroupId];
      int instanceIdInReplicaGroup = 0;
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        List<String> instances = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instances.add(instancesInReplicaGroup[instanceIdInReplicaGroup]);
          instanceIdInReplicaGroup = (instanceIdInReplicaGroup + 1) % numInstancesPerReplicaGroup;
        }
        LOGGER.info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}", instances,
            replicaGroupId, partitionId, _tableNameWithType);
        instancePartitions.setInstances(partitionId, replicaGroupId, instances);
      }
    }
  }

  private int getNumReplicaGroups() {
    int numReplicaGroups = _replicaGroupPartitionConfig.getNumReplicaGroups();
    Preconditions.checkState(numReplicaGroups > 0, "Number of replica-groups must be positive");
    return numReplicaGroups;
  }

  private int getNumInstancesPerReplicaGroup(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      Map<Integer, List<Integer>> poolToReplicaGroupIdsMap) {
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
    return numInstancesPerReplicaGroup;
  }

  private int getNumPartitions() {
    // Assign instances within a replica-group to one partition if not configured
    int numPartitions = _replicaGroupPartitionConfig.getNumPartitions();
    if (numPartitions <= 0) {
      numPartitions = 1;
    }
    return numPartitions;
  }

  private int getNumInstancesPerPartition(int numInstancesPerReplicaGroup) {
    // Assign all instances within a replica-group to each partition if not configured
    int numInstancesPerPartition = _replicaGroupPartitionConfig.getNumInstancesPerPartition();
    if (numInstancesPerPartition > 0) {
      Preconditions.checkState(numInstancesPerPartition <= numInstancesPerReplicaGroup,
          "Number of instances per partition: %s must be smaller or equal to number of instances per replica-group: %s",
          numInstancesPerPartition, numInstancesPerReplicaGroup);
    } else {
      numInstancesPerPartition = numInstancesPerReplicaGroup;
    }
    return numInstancesPerPartition;
  }

  private void replicaGroupBasedMinimumMovement(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions, List<Integer> pools, int tableNameHash) {
    int numPools = pools.size();
    int numReplicaGroups = getNumReplicaGroups();

    Map<String, Integer> instanceToPoolMap = new HashMap<>();
    for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
      int pool = entry.getKey();
      for (InstanceConfig instanceConfig : entry.getValue()) {
        instanceToPoolMap.put(instanceConfig.getInstanceName(), pool);
      }
    }

    // Calculate the mapping from pool to replica-groups assigned to the pool
    List<Set<String>> replicaGroupIdToExistingInstancesMap = new ArrayList<>(numReplicaGroups);
    Map<Integer, List<Integer>> poolToReplicaGroupIdsMap = new TreeMap<>();
    int maxReplicaGroupsPerPool = (numReplicaGroups + numPools - 1) / numPools;
    int startIndex = Math.abs(tableNameHash % numPools);

    int existingNumReplicaGroups = _existingInstancePartitions.getNumReplicaGroups();
    int existingNumPartitions = _existingInstancePartitions.getNumPartitions();
    for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
      // For each replica-group, gather number of existing instances within each pool
      Set<String> existingInstanceSet = new HashSet<>();
      replicaGroupIdToExistingInstancesMap.add(existingInstanceSet);
      Map<Integer, Integer> poolToNumExistingInstancesMap = new TreeMap<>();
      if (replicaGroupId < existingNumReplicaGroups) {
        for (int partitionId = 0; partitionId < existingNumPartitions; partitionId++) {
          List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
          existingInstanceSet.addAll(existingInstances);
          for (String existingInstance : existingInstances) {
            Integer existingPool = instanceToPoolMap.get(existingInstance);
            if (existingPool != null) {
              poolToNumExistingInstancesMap.merge(existingPool, 1, Integer::sum);
            }
          }
        }
      }
      // Sort the pools based on the number of existing instances in the pool in descending order, then use the table
      // name hash to break even
      // Triple stores (pool, numExistingInstances, poolIndex) for sorting
      List<Triple<Integer, Integer, Integer>> triples = new ArrayList<>(numPools);
      for (int i = 0; i < numPools; i++) {
        int pool = pools.get((startIndex + replicaGroupId + i) % numPools);
        triples.add(Triple.of(pool, poolToNumExistingInstancesMap.getOrDefault(pool, 0), i));
      }
      triples.sort((o1, o2) -> {
        int result = Integer.compare(o2.getMiddle(), o1.getMiddle());
        return result != 0 ? result : Integer.compare(o1.getRight(), o2.getRight());
      });
      for (Triple<Integer, Integer, Integer> triple : triples) {
        int pool = triple.getLeft();
        List<Integer> replicaGroupIds = poolToReplicaGroupIdsMap.computeIfAbsent(pool, k -> new ArrayList<>());
        if (replicaGroupIds.size() < maxReplicaGroupsPerPool) {
          replicaGroupIds.add(replicaGroupId);
          break;
        }
      }
    }
    LOGGER.info("Selecting {} replica-groups from pool: {} for table: {}", numReplicaGroups, poolToReplicaGroupIdsMap,
        _tableNameWithType);

    int numInstancesPerReplicaGroup =
        getNumInstancesPerReplicaGroup(poolToInstanceConfigsMap, poolToReplicaGroupIdsMap);
    LOGGER.info("Selecting {} instances per replica-group for table: {}", numInstancesPerReplicaGroup,
        _tableNameWithType);
    int numPartitions = getNumPartitions();
    int numInstancesPerPartition = getNumInstancesPerPartition(numInstancesPerReplicaGroup);
    LOGGER.info("Selecting {} partitions, {} instances per partition within a replica-group for table: {}",
        numPartitions, numInstancesPerPartition, _tableNameWithType);

    List<List<String>> replicaGroupIdToInstancesMap = new ArrayList<>(numReplicaGroups);
    for (int i = 0; i < numReplicaGroups; i++) {
      replicaGroupIdToInstancesMap.add(new ArrayList<>(numInstancesPerReplicaGroup));
    }
    for (Map.Entry<Integer, List<Integer>> entry : poolToReplicaGroupIdsMap.entrySet()) {
      // For each pool, keep the existing instances that are still alive within each replica-group
      int pool = entry.getKey();
      List<Integer> replicaGroupIds = entry.getValue();
      List<String> newInstances = new ArrayList<>();
      for (InstanceConfig instanceConfig : poolToInstanceConfigsMap.get(pool)) {
        String instanceName = instanceConfig.getInstanceName();
        boolean isExistingInstance = false;
        for (int replicaGroupId : replicaGroupIds) {
          List<String> instances = replicaGroupIdToInstancesMap.get(replicaGroupId);
          if (instances.size() == numInstancesPerReplicaGroup) {
            continue;
          }
          if (replicaGroupIdToExistingInstancesMap.get(replicaGroupId).contains(instanceName)) {
            instances.add(instanceName);
            isExistingInstance = true;
            break;
          }
        }
        if (!isExistingInstance) {
          newInstances.add(instanceName);
        }
      }
      // Fill the vacant positions with the new instances. First fill the replica groups with the least instances, then
      // use round-robin to assign instances to each replica-group so that they get instances with similar picking
      // priority.
      int numInstancesToFill = numInstancesPerReplicaGroup * replicaGroupIds.size();
      for (int replicaGroupId : replicaGroupIds) {
        numInstancesToFill -= replicaGroupIdToInstancesMap.get(replicaGroupId).size();
      }
      for (int i = 0; i < numInstancesToFill; i++) {
        int leastNumInstances = Integer.MAX_VALUE;
        int replicaGroupIdWithLeastInstances = -1;
        for (int replicaGroupId : replicaGroupIds) {
          int numInstances = replicaGroupIdToInstancesMap.get(replicaGroupId).size();
          if (numInstances < leastNumInstances) {
            leastNumInstances = numInstances;
            replicaGroupIdWithLeastInstances = replicaGroupId;
          }
        }
        replicaGroupIdToInstancesMap.get(replicaGroupIdWithLeastInstances).add(newInstances.get(i));
      }
    }

    if (numPartitions == 1) {
      for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
        List<String> instancesInReplicaGroup = replicaGroupIdToInstancesMap.get(replicaGroupId);
        if (replicaGroupId < existingNumReplicaGroups) {
          List<String> existingInstances = _existingInstancePartitions.getInstances(0, replicaGroupId);
          LinkedHashSet<String> candidateInstances = new LinkedHashSet<>(instancesInReplicaGroup);
          List<String> instances =
              selectInstancesWithMinimumMovement(numInstancesPerReplicaGroup, candidateInstances, existingInstances);
          LOGGER.info(
              "Selecting instances: {} for replica-group: {}, partition: 0 for table: {}, existing instances: {}",
              instances, replicaGroupId, _tableNameWithType, existingInstances);
          instancePartitions.setInstances(0, replicaGroupId, instances);
        } else {
          LOGGER.info("Selecting instances: {} for replica-group: {}, partition: 0 for table: {}, "
              + "there is no existing instances", instancesInReplicaGroup, replicaGroupId, _tableNameWithType);
          instancePartitions.setInstances(0, replicaGroupId, instancesInReplicaGroup);
        }
      }
    } else {
      for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
        List<String> instancesInReplicaGroup = replicaGroupIdToInstancesMap.get(replicaGroupId);
        if (replicaGroupId < existingNumReplicaGroups) {
          int maxNumPartitionsPerInstance = (numInstancesPerReplicaGroup + numPartitions - 1) / numPartitions;
          Map<String, Integer> instanceToNumPartitionsMap =
              Maps.newHashMapWithExpectedSize(numInstancesPerReplicaGroup);
          for (String instance : instancesInReplicaGroup) {
            instanceToNumPartitionsMap.put(instance, 0);
          }

          List<List<String>> partitionIdToInstancesMap = new ArrayList<>(numPartitions);
          List<Set<String>> partitionIdToInstanceSetMap = new ArrayList<>(numPartitions);
          List<List<String>> partitionIdToExistingInstancesMap = new ArrayList<>(existingNumPartitions);
          for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            // Initialize the list with empty positions to fill
            List<String> instances = new ArrayList<>(numInstancesPerPartition);
            for (int i = 0; i < numInstancesPerPartition; i++) {
              instances.add(null);
            }
            partitionIdToInstancesMap.add(instances);
            Set<String> instanceSet = Sets.newHashSetWithExpectedSize(numInstancesPerPartition);
            partitionIdToInstanceSetMap.add(instanceSet);

            // Keep the existing instances that are still alive
            if (partitionId < existingNumPartitions) {
              List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
              partitionIdToExistingInstancesMap.add(existingInstances);
              int numInstancesToCheck = Math.min(numInstancesPerPartition, existingInstances.size());
              for (int i = 0; i < numInstancesToCheck; i++) {
                String existingInstance = existingInstances.get(i);
                Integer numPartitionsOnInstance = instanceToNumPartitionsMap.get(existingInstance);
                if (numPartitionsOnInstance != null && numPartitionsOnInstance < maxNumPartitionsPerInstance) {
                  instances.set(i, existingInstance);
                  instanceSet.add(existingInstance);
                  instanceToNumPartitionsMap.put(existingInstance, numPartitionsOnInstance + 1);
                }
              }
            }
          }

          // Fill the vacant positions with instance that serves the least partitions
          for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<String> instances = partitionIdToInstancesMap.get(partitionId);
            Set<String> instanceSet = partitionIdToInstanceSetMap.get(partitionId);
            int numInstancesToFill = numInstancesPerPartition - instanceSet.size();
            if (numInstancesToFill > 0) {
              // Triple stores (instance, numPartitionsOnInstance, instanceIndex) for sorting
              List<Triple<String, Integer, Integer>> triples = new ArrayList<>(numInstancesPerReplicaGroup);
              for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
                String instance = instancesInReplicaGroup.get(i);
                if (!instanceSet.contains(instance)) {
                  triples.add(Triple.of(instance, instanceToNumPartitionsMap.get(instance), i));
                }
              }
              triples.sort((o1, o2) -> {
                int result = Integer.compare(o1.getMiddle(), o2.getMiddle());
                return result != 0 ? result : Integer.compare(o1.getRight(), o2.getRight());
              });
              int instanceIdToFill = 0;
              for (int i = 0; i < numInstancesPerPartition; i++) {
                if (instances.get(i) == null) {
                  String instance = triples.get(instanceIdToFill++).getLeft();
                  instances.set(i, instance);
                  instanceToNumPartitionsMap.put(instance, instanceToNumPartitionsMap.get(instance) + 1);
                }
              }
            }

            if (partitionId < existingNumPartitions) {
              LOGGER.info(
                  "Selecting instances: {} for replica-group: {}, partition: {} for table: {}, existing instances: {}",
                  instances, replicaGroupId, partitionId, _tableNameWithType,
                  partitionIdToExistingInstancesMap.get(partitionId));
            } else {
              LOGGER.info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}, "
                  + "there is no existing instances", instances, replicaGroupId, partitionId, _tableNameWithType);
            }
            instancePartitions.setInstances(partitionId, replicaGroupId, instances);
          }
        } else {
          // Assign consecutive instances within a replica-group to each partition
          int instanceIdInReplicaGroup = 0;
          for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<String> instances = new ArrayList<>(numInstancesPerPartition);
            for (int i = 0; i < numInstancesPerPartition; i++) {
              instances.add(instancesInReplicaGroup.get(instanceIdInReplicaGroup));
              instanceIdInReplicaGroup = (instanceIdInReplicaGroup + 1) % numInstancesPerReplicaGroup;
            }
            LOGGER.info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}, "
                + "there is no existing instances", instances, replicaGroupId, partitionId, _tableNameWithType);
            instancePartitions.setInstances(partitionId, replicaGroupId, instances);
          }
        }
      }
    }
  }
}
