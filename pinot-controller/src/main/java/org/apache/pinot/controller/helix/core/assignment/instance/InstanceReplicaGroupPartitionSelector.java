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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The instance replica-group/partition selector is responsible for selecting the instances for each replica-group and
 * partition.
 */
public class InstanceReplicaGroupPartitionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceReplicaGroupPartitionSelector.class);

  private final InstanceReplicaGroupPartitionConfig _replicaGroupPartitionConfig;
  private final String _tableNameWithType;

  public InstanceReplicaGroupPartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType) {
    _replicaGroupPartitionConfig = replicaGroupPartitionConfig;
    _tableNameWithType = tableNameWithType;
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
      for (int replicaId = 0; replicaId < numReplicaGroups; replicaId++) {
        // Pick one pool for each replica-group based on the table name hash
        int pool = pools.get((tableNameHash + replicaId) % numPools);
        poolToReplicaGroupIdsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(replicaId);
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

      // Assign instances within a replica-group to one partition if not configured
      int numPartitions = _replicaGroupPartitionConfig.getNumPartitions();
      if (numPartitions <= 0) {
        numPartitions = 1;
      }
      // Assign all instances within a replica-group to each partition if not configured
      int numInstancesPerPartition = _replicaGroupPartitionConfig.getNumInstancesPerPartition();
      if (numInstancesPerPartition > 0) {
        Preconditions.checkState(numInstancesPerPartition <= numInstancesPerReplicaGroup,
            "Number of instances per partition: %s must be smaller or equal to number of instances per replica-group: %s",
            numInstancesPerPartition, numInstancesPerReplicaGroup);
      } else {
        numInstancesPerPartition = numInstancesPerReplicaGroup;
      }
      LOGGER.info("Selecting {} partitions, {} instances per partition within a replica-group for table: {}",
          numPartitions, numInstancesPerPartition, _tableNameWithType);

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
          instancesInPartition.sort(null);
          LOGGER
              .info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}", instancesInPartition,
                  replicaGroupId, partitionId, _tableNameWithType);
          instancePartitions.setInstances(partitionId, replicaGroupId, instancesInPartition);
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

      List<String> instancesToSelect = new ArrayList<>(numInstancesToSelect);
      for (int i = 0; i < numInstancesToSelect; i++) {
        instancesToSelect.add(instanceConfigs.get(i).getInstanceName());
      }
      instancesToSelect.sort(null);
      LOGGER.info("Selecting instances: {} for table: {}", instancesToSelect, _tableNameWithType);
      // Set the instances as partition 0 replica 0
      instancePartitions.setInstances(0, 0, instancesToSelect);
    }
  }
}
