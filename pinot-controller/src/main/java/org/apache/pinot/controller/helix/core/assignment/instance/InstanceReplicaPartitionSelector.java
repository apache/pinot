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
import org.apache.pinot.common.config.instance.InstanceReplicaPartitionConfig;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The instance replica/partition selector is responsible for selecting the instances for each replica and partition.
 * <p>NOTE: The replica here refers to the table level replica (replica-group), which is a set of instances that contain
 * all segments for a table.
 */
public class InstanceReplicaPartitionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceReplicaPartitionSelector.class);

  private final InstanceReplicaPartitionConfig _replicaPartitionConfig;
  private final String _tableNameWithType;

  public InstanceReplicaPartitionSelector(InstanceReplicaPartitionConfig replicaPartitionConfig,
      String tableNameWithType) {
    _replicaPartitionConfig = replicaPartitionConfig;
    _tableNameWithType = tableNameWithType;
  }

  /**
   * Selects instances based on the replica and partition config, and stores the result into the given instance
   * partitions.
   */
  public InstancePartitions selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap) {
    int numPools = poolToInstanceConfigsMap.size();
    Preconditions.checkState(numPools != 0, "No pool qualified for selection");

    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    List<Integer> pools = new ArrayList<>(poolToInstanceConfigsMap.keySet());
    pools.sort(null);
    LOGGER.info("Starting instance replica/partition selection for table: {} with hash: {} from pools: {}",
        _tableNameWithType, tableNameHash, pools);

    InstancePartitions instancePartitions = new InstancePartitions(_tableNameWithType);

    if (_replicaPartitionConfig.isReplicaGroupBased()) {
      // Replica-group based selection

      int numReplicas = _replicaPartitionConfig.getNumReplicas();
      Preconditions.checkState(numReplicas > 0, "Number of replicas must be positive");
      Map<Integer, List<Integer>> poolToReplicaIdsMap = new TreeMap<>();
      for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
        // Pick one pool for each replica based on the table name hash
        int pool = pools.get((tableNameHash + replicaId) % numPools);
        poolToReplicaIdsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(replicaId);
      }
      LOGGER.info("Selecting {} replicas from pool: {} for table: {}", numReplicas, poolToReplicaIdsMap,
          _tableNameWithType);

      int numInstancesPerReplica = _replicaPartitionConfig.getNumInstancesPerReplica();
      if (numInstancesPerReplica > 0) {
        // Check if we have enough instances if number of instances per replica is configured
        for (Map.Entry<Integer, List<Integer>> entry : poolToReplicaIdsMap.entrySet()) {
          int pool = entry.getKey();
          int numInstancesInPool = poolToInstanceConfigsMap.get(pool).size();
          int numInstancesToSelect = numInstancesPerReplica * entry.getValue().size();
          Preconditions.checkState(numInstancesToSelect <= numInstancesInPool,
              "Not enough qualified instances from pool: %s (%s in the pool, asked for %s)", pool, numInstancesInPool,
              numInstancesToSelect);
        }
      } else {
        // Use as many instances as possible if number of instances per replica is not configured
        numInstancesPerReplica = Integer.MAX_VALUE;
        for (Map.Entry<Integer, List<Integer>> entry : poolToReplicaIdsMap.entrySet()) {
          int pool = entry.getKey();
          int numReplicasInPool = entry.getValue().size();
          int numInstancesInPool = poolToInstanceConfigsMap.get(pool).size();
          Preconditions.checkState(numReplicasInPool <= numInstancesInPool,
              "Not enough qualified instances from pool: %s, cannot select %s replicas from %s instances", pool,
              numReplicasInPool, numInstancesInPool);
          numInstancesPerReplica = Math.min(numInstancesPerReplica, numInstancesInPool / numReplicasInPool);
        }
      }
      LOGGER.info("Selecting {} instances per replica for table: {}", numInstancesPerReplica, _tableNameWithType);

      String[][] replicaIdToInstancesMap = new String[numReplicas][numInstancesPerReplica];
      for (Map.Entry<Integer, List<Integer>> entry : poolToReplicaIdsMap.entrySet()) {
        List<InstanceConfig> instanceConfigsInPool = poolToInstanceConfigsMap.get(entry.getKey());
        List<Integer> replicaIdsInPool = entry.getValue();

        // Use round-robin to assign instances to each replica so that they get instances with similar picking priority
        int instanceIdInPool = 0;
        for (int instanceIdInReplica = 0; instanceIdInReplica < numInstancesPerReplica; instanceIdInReplica++) {
          for (int replicaId : replicaIdsInPool) {
            replicaIdToInstancesMap[replicaId][instanceIdInReplica] =
                instanceConfigsInPool.get(instanceIdInPool++).getInstanceName();
          }
        }
      }

      // Assign instances within a replica to one partition if not configured
      int numPartitions = _replicaPartitionConfig.getNumPartitions();
      if (numPartitions <= 0) {
        numPartitions = 1;
      }
      // Assign all instances within a replica to each partition if not configured
      int numInstancesPerPartition = _replicaPartitionConfig.getNumInstancesPerPartition();
      if (numInstancesPerPartition > 0) {
        Preconditions.checkState(numInstancesPerPartition <= numInstancesPerReplica,
            "Number of instances per partition: %s must be smaller or equal to number of instances per replica: %s",
            numInstancesPerPartition, numInstancesPerReplica);
      } else {
        numInstancesPerPartition = numInstancesPerReplica;
      }
      LOGGER.info("Selecting {} partitions, {} instances per partition within a replica for table: {}", numPartitions,
          numInstancesPerPartition, _tableNameWithType);

      // Assign consecutive instances within a replica to each partition.
      // E.g. (within a replica, 5 instances, 3 partitions, 3 instances per partition)
      // [i0, i1, i2, i3, i4]
      //  p0  p0  p0  p1  p1
      //  p1  p2  p2  p2
      for (int replicaId = 0; replicaId < numReplicas; replicaId++) {
        int instanceIdInReplica = 0;
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
          List<String> instancesInPartition = new ArrayList<>(numInstancesPerPartition);
          for (int instanceIdInPartition = 0; instanceIdInPartition < numInstancesPerPartition;
              instanceIdInPartition++) {
            instancesInPartition.add(replicaIdToInstancesMap[replicaId][instanceIdInReplica]);
            instanceIdInReplica = (instanceIdInReplica + 1) % numInstancesPerReplica;
          }
          instancesInPartition.sort(null);
          LOGGER.info("Selecting instances: {} for replica: {}, partition: {} for table: {}", instancesInPartition,
              replicaId, partitionId, _tableNameWithType);
          instancePartitions.setInstances(partitionId, replicaId, instancesInPartition);
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
      int numInstancesToSelect = _replicaPartitionConfig.getNumInstances();
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

    return instancePartitions;
  }
}
