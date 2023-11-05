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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The instance tag/pool selector is responsible for selecting instances based on the tag and pool config.
 */
public class InstanceTagPoolSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceTagPoolSelector.class);

  private final InstanceTagPoolConfig _tagPoolConfig;
  private final String _tableNameWithType;

  private final boolean _minimizeDataMovement;

  private final InstancePartitions _existingInstancePartitions;

  public InstanceTagPoolSelector(InstanceTagPoolConfig tagPoolConfig, String tableNameWithType,
      boolean minimizeDataMovement,
      @Nullable InstancePartitions existingInstancePartitions) {
    _tagPoolConfig = tagPoolConfig;
    _tableNameWithType = tableNameWithType;
    _minimizeDataMovement = minimizeDataMovement;
    _existingInstancePartitions = existingInstancePartitions;
  }

  /**
   * Returns a map from pool to instance configs based on the tag and pool config for the given instance configs.
   */
  public Map<Integer, List<InstanceConfig>> selectInstances(List<InstanceConfig> instanceConfigs) {
    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    LOGGER.info("Starting instance tag/pool selection for table: {} with hash: {}", _tableNameWithType, tableNameHash);

    // Filter out the instances with the correct tag
    String tag = _tagPoolConfig.getTag();
    List<InstanceConfig> candidateInstanceConfigs = new ArrayList<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (instanceConfig.getTags().contains(tag)) {
        candidateInstanceConfigs.add(instanceConfig);
      }
    }
    candidateInstanceConfigs.sort(Comparator.comparing(InstanceConfig::getInstanceName));
    int numCandidateInstances = candidateInstanceConfigs.size();
    Preconditions.checkState(numCandidateInstances > 0, "No enabled instance has the tag: %s", tag);
    LOGGER.info("{} enabled instances have the tag: {} for table: {}", numCandidateInstances, tag, _tableNameWithType);

    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = new TreeMap<>();
    if (_tagPoolConfig.isPoolBased()) {
      // Pool based selection

      Map<String, Integer> instanceToPoolMap = new HashMap<>();
      // Extract the pool information from the instance configs
      for (InstanceConfig instanceConfig : candidateInstanceConfigs) {
        Map<String, String> poolMap = instanceConfig.getRecord().getMapField(InstanceUtils.POOL_KEY);
        if (poolMap != null && poolMap.containsKey(tag)) {
          int pool = Integer.parseInt(poolMap.get(tag));
          poolToInstanceConfigsMap.computeIfAbsent(pool, k -> new ArrayList<>()).add(instanceConfig);
          instanceToPoolMap.put(instanceConfig.getInstanceName(), pool);
        }
      }
      Preconditions.checkState(!poolToInstanceConfigsMap.isEmpty(),
          "No enabled instance has the pool configured for the tag: %s", tag);
      Map<Integer, Integer> poolToNumInstancesMap = new TreeMap<>();
      for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
        poolToNumInstancesMap.put(entry.getKey(), entry.getValue().size());
      }
      LOGGER.info("Number instances for each pool: {} for table: {}", poolToNumInstancesMap, _tableNameWithType);

      // Calculate the pools to select based on the selection config
      Set<Integer> pools = poolToInstanceConfigsMap.keySet();
      List<Integer> poolsToSelect = _tagPoolConfig.getPools();
      if (poolsToSelect != null && !poolsToSelect.isEmpty()) {
        Preconditions.checkState(pools.containsAll(poolsToSelect), "Cannot find all instance pools configured: %s",
            poolsToSelect);
      } else {
        int numPools = poolToInstanceConfigsMap.size();
        int numPoolsToSelect = _tagPoolConfig.getNumPools();
        if (numPoolsToSelect > 0) {
          Preconditions.checkState(numPoolsToSelect <= numPools,
              "Not enough instance pools (%s in the cluster, asked for %s)", numPools, numPoolsToSelect);
        } else {
          numPoolsToSelect = numPools;
        }

        // Directly return the map if all the pools are selected
        if (numPools == numPoolsToSelect) {
          LOGGER.info("Selecting all {} pools: {} for table: {}", numPools, pools, _tableNameWithType);
          return poolToInstanceConfigsMap;
        }

        poolsToSelect = new ArrayList<>(numPoolsToSelect);
        if (_minimizeDataMovement && _existingInstancePartitions != null) {
          Set<Integer> existingPools = new HashSet<>(numPoolsToSelect);
          // Keep the same pool if it's already been used for the table.
          int existingNumPartitions = _existingInstancePartitions.getNumPartitions();
          int existingNumReplicaGroups = _existingInstancePartitions.getNumReplicaGroups();
          for (int replicaGroupId = 0; replicaGroupId < existingNumReplicaGroups; replicaGroupId++) {
            boolean foundExistingPoolForReplicaGroup = false;
            for (int partitionId = 0; partitionId < existingNumPartitions & !foundExistingPoolForReplicaGroup;
                partitionId++) {
              List<String> existingInstances = _existingInstancePartitions.getInstances(partitionId, replicaGroupId);
              for (String existingInstance : existingInstances) {
                Integer existingPool = instanceToPoolMap.get(existingInstance);
                if (existingPool != null & pools.contains(existingPool)) {
                  poolsToSelect.add(existingPool);
                  existingPools.add(existingPool);
                  foundExistingPoolForReplicaGroup = true;
                  break;
                }
              }
            }
          }
          LOGGER.info("Keep the same pool: {} for table: {}", existingPools, _tableNameWithType);
          // Pick a pool from remainingPools that isn't used before.
          List<Integer> remainingPools = new ArrayList<>(pools);
          remainingPools.retainAll(existingPools);
          // Skip selecting the existing pool.
          for (int i = 0; i < numPoolsToSelect; i++) {
            if (existingPools.contains(i)) {
              continue;
            }
            poolsToSelect.add(remainingPools.remove(i % remainingPools.size()));
          }
        } else {
          // Select pools based on the table name hash to evenly distribute the tables
          List<Integer> poolsInCluster = new ArrayList<>(pools);
          for (int i = 0; i < numPoolsToSelect; i++) {
            poolsToSelect.add(poolsInCluster.get((tableNameHash + i) % numPools));
          }
        }
      }

      // Keep the pools selected
      LOGGER.info("Selecting pools: {} for table: {}", poolsToSelect, _tableNameWithType);
      pools.retainAll(poolsToSelect);
    } else {
      // Non-pool based selection

      LOGGER.info("Selecting {} instances for table: {}", numCandidateInstances, _tableNameWithType);
      // Put all instance configs as pool 0
      poolToInstanceConfigsMap.put(0, candidateInstanceConfigs);
    }
    return poolToInstanceConfigsMap;
  }
}
