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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An extended version of {@link InstanceTagPoolSelector} that retains existing instance sequence
 * from the last instance assignment given the existing pool to instances map. If the map is null or empty,
 * there is no instance sequence to respect, and the instance configs will be sorted based on the instance ids.
 */
public class RetainedSequenceBasedInstanceTagPoolSelector extends InstanceTagPoolSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetainedSequenceBasedInstanceTagPoolSelector.class);

  private final Map<Integer, List<String>> _existingPoolToInstancesMap;

  public RetainedSequenceBasedInstanceTagPoolSelector(InstanceTagPoolConfig tagPoolConfig, String tableNameWithType,
      Map<Integer, List<String>> existingPoolToInstancesMap) {
    super(tagPoolConfig, tableNameWithType);
    // The existing poolToInstancesMap must not be null.
    _existingPoolToInstancesMap = existingPoolToInstancesMap;
  }

  @Override
  public Map<Integer, List<InstanceConfig>> selectInstances(List<InstanceConfig> instanceConfigs) {
    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    LOGGER.info("Starting instance tag/pool selection for table: {} with hash: {}", _tableNameWithType, tableNameHash);

    // Filter out the instances with the correct tag.
    // Use LinkedHashMap here to retain the sorted list of instance names.
    String tag = _tagPoolConfig.getTag();
    Map<String, InstanceConfig> candidateInstanceConfigsMap = new LinkedHashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      if (instanceConfig.getTags().contains(tag)) {
        candidateInstanceConfigsMap.put(instanceConfig.getInstanceName(), instanceConfig);
      }
    }

    // Find out newly added instances from the latest copies of instance configs.
    // A deque is used here in order to retain the sequence,
    // given the fact that the list of instance configs is always sorted.
    Deque<String> newlyAddedInstances = new LinkedList<>(candidateInstanceConfigsMap.keySet());
    for (List<String> existingInstancesWithSequence : _existingPoolToInstancesMap.values()) {
      newlyAddedInstances.removeAll(existingInstancesWithSequence);
    }

    int numCandidateInstances = candidateInstanceConfigsMap.size();
    Preconditions.checkState(numCandidateInstances > 0, "No enabled instance has the tag: %s", tag);
    LOGGER.info("{} enabled instances have the tag: {} for table: {}", numCandidateInstances, tag, _tableNameWithType);

    Map<Integer, List<InstanceConfig>> poolToLatestInstanceConfigsMap = new TreeMap<>();
    if (_tagPoolConfig.isPoolBased()) {
      // Pool based selection. All the instances should be associated with a specific pool number.
      // Instance selection should be done within the same pool.
      // E.g.: Pool0 -> [ I1, I2, I3 ]
      //       Pool1 -> [ I4, I5, I6 ]

      // Each pool number associates with a map that key is the instance name and value is the instance config.
      Map<Integer, Map<String, InstanceConfig>> poolToInstanceConfigsMap = new HashMap<>();
      // Each pool number associates with a list of newly added instance configs,
      // so that new instances can be fetched from this list.
      Map<Integer, Deque<InstanceConfig>> poolToNewInstanceConfigsMap = new HashMap<>();

      // Extract the pool information from the instance configs.
      for (Map.Entry<String, InstanceConfig> entry : candidateInstanceConfigsMap.entrySet()) {
        String instanceName = entry.getKey();
        InstanceConfig instanceConfig = entry.getValue();
        Map<String, String> poolMap = instanceConfig.getRecord().getMapField(InstanceUtils.POOL_KEY);
        if (poolMap != null && poolMap.containsKey(tag)) {
          int pool = Integer.parseInt(poolMap.get(tag));
          poolToInstanceConfigsMap.computeIfAbsent(pool, k -> new TreeMap<>()).put(instanceName, instanceConfig);
          if (newlyAddedInstances.contains(instanceName)) {
            poolToNewInstanceConfigsMap.computeIfAbsent(pool, k -> new LinkedList<>()).add(instanceConfig);
          }
        }
      }

      for (Map.Entry<Integer, List<String>> entry : _existingPoolToInstancesMap.entrySet()) {
        Integer pool = entry.getKey();
        List<String> existingInstanceAssignmentInPool = entry.getValue();
        List<InstanceConfig> candidateInstanceConfigsWithSequence = new ArrayList<>();
        for (String existingInstance : existingInstanceAssignmentInPool) {
          InstanceConfig instanceConfig = poolToInstanceConfigsMap.get(pool).get(existingInstance);
          // Add instances to the candidate list and respect the sequence of the existing instances from the ZK.
          // The missing/removed instances will be replaced by the newly instances.
          // If the instance still exists from ZK, then add it to the candidate list.
          // E.g. if the old instances are: [I1, I2, I3, I4] and the new instance are: [I1, I3, I4, I5, I6],
          // the removed instance is I2 and the newly added instances are I5 and I6.
          // The position of I2 would be replaced by I5, the new remaining I6 would be appended to the tail.
          // Thus, the new order would be [I1, I5, I3, I4, I6].
          if (instanceConfig != null) {
            candidateInstanceConfigsWithSequence.add(instanceConfig);
          } else {
            // The current chosen instance no longer lives in the cluster any more, thus pick a new instance.
            InstanceConfig newInstanceConfig = poolToNewInstanceConfigsMap.get(pool).pollFirst();
            // If there is no new instance from the same pool, then don't add it.
            if (newInstanceConfig != null) {
              candidateInstanceConfigsWithSequence.add(newInstanceConfig);
            }
          }
        }
        if (!candidateInstanceConfigsMap.isEmpty()) {
          poolToLatestInstanceConfigsMap.put(pool, candidateInstanceConfigsWithSequence);
        }
      }

      // The preceding list of instances has been traversed. Add the remaining new instances.
      for (Map.Entry<Integer, Deque<InstanceConfig>> entry : poolToNewInstanceConfigsMap.entrySet()) {
        Integer pool = entry.getKey();
        Deque<InstanceConfig> remainingNewInstanceConfigs = entry.getValue();
        poolToLatestInstanceConfigsMap.computeIfAbsent(pool, k -> new ArrayList<>())
            .addAll(remainingNewInstanceConfigs);
      }

      Preconditions.checkState(!poolToInstanceConfigsMap.isEmpty(),
          "No enabled instance has the pool configured for the tag: %s", tag);
      Map<Integer, Integer> poolToNumInstancesMap = new TreeMap<>();
      for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToLatestInstanceConfigsMap.entrySet()) {
        poolToNumInstancesMap.put(entry.getKey(), entry.getValue().size());
      }
      LOGGER.info("Number instances for each pool: {} for table: {}", poolToNumInstancesMap, _tableNameWithType);

      // Calculate the pools to select based on the selection config.
      // Note: the pools here refers to the key set of poolToLatestInstanceConfigsMap,
      //       so that removing the pool from the set means removing the key-value pair from the map.
      Set<Integer> pools = poolToLatestInstanceConfigsMap.keySet();
      List<Integer> poolsToSelect = _tagPoolConfig.getPools();
      if (poolsToSelect != null && !poolsToSelect.isEmpty()) {
        Preconditions.checkState(pools.containsAll(poolsToSelect), "Cannot find all instance pools configured: %s",
            poolsToSelect);
      } else {
        int numPools = poolToLatestInstanceConfigsMap.size();
        int numPoolsToSelect = _tagPoolConfig.getNumPools();
        if (numPoolsToSelect > 0) {
          Preconditions
              .checkState(numPoolsToSelect <= numPools, "Not enough instance pools (%s in the cluster, asked for %s)",
                  numPools, numPoolsToSelect);
        } else {
          numPoolsToSelect = numPools;
        }

        // Directly return the map if all the pools are selected
        if (numPools == numPoolsToSelect) {
          LOGGER.info("Selecting all {} pools: {} for table: {}", numPools, pools, _tableNameWithType);
          return poolToLatestInstanceConfigsMap;
        }

        // Select pools based on the table name hash to evenly distribute the tables
        poolsToSelect = new ArrayList<>(numPoolsToSelect);
        List<Integer> poolsInCluster = new ArrayList<>(pools);
        for (int i = 0; i < numPoolsToSelect; i++) {
          poolsToSelect.add(poolsInCluster.get((tableNameHash + i) % numPools));
        }
      }

      // Keep the pools selected
      LOGGER.info("Selecting pools: {} for table: {}", poolsToSelect, _tableNameWithType);
      pools.retainAll(poolsToSelect);
    } else {
      // Non-pool based selection. All the instances should be associated with a single pool, which is always 0.
      // E.g.: Pool0 -> [ I1, I2, I3, I4, I5, I6 ]

      LOGGER.info("Selecting {} instances for table: {}", candidateInstanceConfigsMap.size(), _tableNameWithType);
      // Put all instance configs as pool 0

      for (Map.Entry<Integer, List<String>> entry : _existingPoolToInstancesMap.entrySet()) {
        Integer pool = entry.getKey();
        List<String> existingInstanceAssignmentInPool = entry.getValue();
        List<InstanceConfig> candidateInstanceConfigsWithSequence = new ArrayList<>();
        for (String existingInstance : existingInstanceAssignmentInPool) {
          InstanceConfig instanceConfig = candidateInstanceConfigsMap.get(existingInstance);
          // Add instances to the candidate list and respect the sequence of the existing instances from the ZK.
          // The missing/removed instances will be replaced by the newly instances.
          // If the instance still exists from ZK, then add it to the candidate list.
          // E.g. if the old instances are: [I1, I2, I3, I4] and the new instance are: [I1, I3, I4, I5, I6],
          // the removed instance is I2 and the newly added instances are I5 and I6.
          // The position of I2 would be replaced by I5, the new remaining I6 would be appended to the tail.
          // Thus, the new order would be [I1, I5, I3, I4, I6].
          if (instanceConfig != null) {
            candidateInstanceConfigsWithSequence.add(instanceConfig);
          } else {
            // The current chosen instance no longer lives in the cluster any more, thus pick a new instance.
            String newInstance = newlyAddedInstances.pollFirst();
            // If there is no new instance from the same pool, then don't add it.
            if (newInstance != null) {
              candidateInstanceConfigsWithSequence.add(candidateInstanceConfigsMap.get(newInstance));
            }
          }
        }
        if (!candidateInstanceConfigsWithSequence.isEmpty()) {
          poolToLatestInstanceConfigsMap.put(pool, candidateInstanceConfigsWithSequence);
        }
      }
      // The preceding list of instances has been traversed. Add the remaining new instances.
      for (String remainingNewInstance : newlyAddedInstances) {
        poolToLatestInstanceConfigsMap.computeIfAbsent(0, k -> new ArrayList<>())
            .add(candidateInstanceConfigsMap.get(remainingNewInstance));
      }
    }
    return poolToLatestInstanceConfigsMap;
  }
}
