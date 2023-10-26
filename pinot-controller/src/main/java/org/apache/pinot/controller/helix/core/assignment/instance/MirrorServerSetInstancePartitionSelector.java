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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Detailed design see https://docs.google.com/document/d/1xxPkGPxyY21gAkFi9gtFDeSzEXjPjp-IQW70kHynsL8
 * During each creation/update/scale, the algorithm will refer to the corresponding tenant level instance partitions and
 * generate an instance partition by taking numInstancePerReplicaGroup mirror server sets from the tenant level
 * instance partitions.
 *
 * If an existingInstancePartition is provided, the algorithm will generate a best effort assignment that resembles
 * the existingInstancePartition.
 *
 * Assumptions for this algorithm:
 *  1. The number of replica groups in the tenant level instance partitions is the same as the number of replica groups
 *     in the table config.
 *  2. The number of partitions at replica group level is 1
 *  3. This algorithm only works for replica group based table assignment
 */
public class MirrorServerSetInstancePartitionSelector extends InstancePartitionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(MirrorServerSetInstancePartitionSelector.class);
  private final InstancePartitions _preConfiguredInstancePartitions;

  // dimensions of target instance partition
  private final int _numTargetInstancesPerReplicaGroup;
  private final int _numTargetReplicaGroups;
  private final int _numTargetTotalInstances;
  // look up tables for pre-configured instance partition
  private final List<List<String>> _preConfiguredMirroredServerLists = new ArrayList<>();
  private final Map<String, Integer> _preConfiguredInstanceNameToOffsetMap = new HashMap<>();
  private final List<List<String>> _existingMirroredServerLists = new ArrayList<>();
  // dimensions of pre-configured instance partition
  private int _numPreConfiguredReplicaGroups;
  private int _numPreConfiguredInstancesPerReplicaGroup;
  // dimensions of existing instance partition
  private int _numExistingReplicaGroups;
  private int _numExistingInstancesPerReplicaGroup;

  public MirrorServerSetInstancePartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, @Nullable InstancePartitions existingInstancePartitions,
      InstancePartitions preConfiguredInstancePartitions) {
    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions);
    _preConfiguredInstancePartitions = preConfiguredInstancePartitions;
    _numTargetInstancesPerReplicaGroup = _replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup();
    _numTargetReplicaGroups = _replicaGroupPartitionConfig.getNumReplicaGroups();
    _numTargetTotalInstances = _numTargetInstancesPerReplicaGroup * _numTargetReplicaGroups;
  }

  /**
   * validate if the poolToInstanceConfigsMap is a valid input for pre-configuration based replica-group selection
   */
  private void validatePoolDiversePreconditions(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap) {

    LOGGER.info("Validating pre-configured instance partitions for pre-configuration based replica-group selection");

    // numTargetInstancesPerReplica should be positive
    LOGGER.info("Number of instances per replica: {}", _numTargetInstancesPerReplicaGroup);
    Preconditions.checkState(_numTargetInstancesPerReplicaGroup > 0,
        "Number of instances per replica must be positive");

    // _numTargetReplicaGroups should be positive
    LOGGER.info("Number of replica-groups: {}", _numTargetReplicaGroups);
    Preconditions.checkState(_numTargetReplicaGroups > 0, "Number of replica-groups must be positive");

    // validate target partition count is 1
    LOGGER.info("Number of partitions: {}", _replicaGroupPartitionConfig.getNumPartitions());
    Preconditions.checkState(_replicaGroupPartitionConfig.getNumPartitions() <= 1,
        "This algorithm does not support table level partitioning for target assignment");

    // Validate the existing instance partitions is null or has only one partition
    LOGGER.info("Number of partitions in existing instance partitions: {}",
        _existingInstancePartitions == null ? 0 : _existingInstancePartitions.getNumPartitions());
    Preconditions.checkState(
        (_existingInstancePartitions == null || _existingInstancePartitions.getNumPartitions() == 1),
        "This algorithm does not support replica group level partitioning for existing assignment");

    _numExistingReplicaGroups =
        _existingInstancePartitions == null ? 0 : _existingInstancePartitions.getNumReplicaGroups();
    _numExistingInstancesPerReplicaGroup =
        _existingInstancePartitions == null ? 0 : _existingInstancePartitions.getInstances(0, 0).size();

    // Validate the pre-configured instance partitions is not null and has only one partition
    Preconditions.checkState(_preConfiguredInstancePartitions != null,
        "Pre-configured instance partitions must be provided for pre-configuration based selection");
    LOGGER.info("Number of partitions in pre-configured instance partitions: {}",
        _preConfiguredInstancePartitions.getNumPartitions());
    Preconditions.checkState(_preConfiguredInstancePartitions.getNumPartitions() == 1,
        "This algorithm does not support table level partitioning for pre-configured assignment");

    // Validate the number of replica-groups in the pre-configured instance partitions is equal to the target
    // number of replica-groups
    _numPreConfiguredReplicaGroups = _preConfiguredInstancePartitions.getNumReplicaGroups();
    LOGGER.info("Number of replica-groups in pre-configured instance partitions: {}", _numPreConfiguredReplicaGroups);
    Preconditions.checkState(_numPreConfiguredReplicaGroups == _numTargetReplicaGroups,
        "The number of replica-groups %s in the pre-configured instance partitions "
            + "is not equal to the target number of replica-groups %s", _numPreConfiguredReplicaGroups,
        _numTargetReplicaGroups);

    // Validate the number of instances per replica-group in the pre-configured instance partitions is greater than or
    // equal to the target number of instances per replica-group
    _numPreConfiguredInstancesPerReplicaGroup = _preConfiguredInstancePartitions.getInstances(0, 0).size();
    LOGGER.info("Number of instances per replica-group in pre-configured instance partitions: {}, target number of "
            + "instances per replica-group: {}", _numPreConfiguredInstancesPerReplicaGroup,
        _numTargetInstancesPerReplicaGroup);
    Preconditions.checkState(_numPreConfiguredInstancesPerReplicaGroup >= _numTargetInstancesPerReplicaGroup,
        "The number of instances per replica-group in the pre-configured "
            + "instance partitions is less than the target number of instances per replica-group %s",
        _numTargetInstancesPerReplicaGroup);

    // Validate the pool to instance configs map is not null or empty
    Preconditions.checkNotNull(poolToInstanceConfigsMap, "poolToInstanceConfigsMap is null");
    int numPools = poolToInstanceConfigsMap.size();
    Preconditions.checkState(numPools > 0, "No pool qualified for selection");
    Integer totalInstanceCount = poolToInstanceConfigsMap.values().stream().map(List::size)
        .reduce(Integer::sum).orElse(0);
    LOGGER.info("Total number of instances in all pools: {}, target number of instances: {}", totalInstanceCount,
        _numTargetTotalInstances);
    Preconditions.checkState(totalInstanceCount
            >= _numTargetTotalInstances,
        "The total number of instances in all pools is less than the target number of target instances");

    HashSet<String> availableInstanceSet = new HashSet<>();
    poolToInstanceConfigsMap.values().forEach(list -> list.forEach(i -> availableInstanceSet.add(i.getInstanceName())));
    LOGGER.info("Number of pools: {}", numPools);
    LOGGER.info("Number of instances in all pools: {}", availableInstanceSet.size());
    LOGGER.info("availableInstanceSet: {}", availableInstanceSet);

    for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
      List<String> instances = _preConfiguredInstancePartitions.getInstances(0, i);
      for (String instance : instances) {
        Preconditions.checkState(availableInstanceSet.contains(instance),
            "Instance %s in pre-configured instance partitions is not in "
                + "the pool to instance configs map",
            instance);
      }
    }

    LOGGER.info("Validation passed. The instances provided can satisfy the pool diverse requirement.");
    LOGGER.info("Trying to assign total {} instances to {} replica groups, " + "with {} instance per replica group",
        _numTargetTotalInstances, _numTargetReplicaGroups, _numTargetInstancesPerReplicaGroup);
  }

  private void createMirrorServerListFromPreconfiguredInstancePartition() {
    List<List<String>> preConfiguredReplicaGroups = new ArrayList<>(_numPreConfiguredReplicaGroups);
    for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
      preConfiguredReplicaGroups.add(_preConfiguredInstancePartitions.getInstances(0, i));
    }

    for (int j = 0; j < _numPreConfiguredInstancesPerReplicaGroup; j++) {
      List<String> mirroredServerList = new ArrayList<>();
      for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
        mirroredServerList.add(preConfiguredReplicaGroups.get(i).get(j));
      }
      _preConfiguredMirroredServerLists.add(mirroredServerList);
    }
  }

  private void createMirrorServerListLookupTablesFromPreconfiguredInstancePartition() {
    List<List<String>> preConfiguredReplicaGroups = new ArrayList<>(_numPreConfiguredReplicaGroups);
    for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
      preConfiguredReplicaGroups.add(_preConfiguredInstancePartitions.getInstances(0, i));
    }

    for (int i = 0; i < _numPreConfiguredReplicaGroups; i++) {
      for (int j = 0; j < _numPreConfiguredInstancesPerReplicaGroup; j++) {
        String instance = preConfiguredReplicaGroups.get(i).get(j);
        _preConfiguredInstanceNameToOffsetMap.put(instance, j);
      }
    }
  }

  @Override
  public void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions) {
    // throw exception instantly if not replica-group based
    if (!_replicaGroupPartitionConfig.isReplicaGroupBased()) {
      throw new IllegalStateException("Does not support Non-replica-group based selection");
    }

    validatePoolDiversePreconditions(poolToInstanceConfigsMap);
    if (_existingInstancePartitions == null) {
      // If no existing instance partitions, create new instance partitions based on the pre-configured instance
      // partitions. This is done by just selecting _targetNumInstancesPerReplicaGroup set of mirrored servers
      // from the pre-configured instance partitions.
      initialAssignment(instancePartitions);
    } else {
      // If existing instance partitions exist, adjust the existing instance partitions based on the pre-configured
      // instance partitions. This code path takes care of instance replacement, uplift, and downlift.
      // This is done by search in the pre-configured instance partitions for the mirrored
      // servers sets that are similar to the existing sets in instance partitions.
      scale(instancePartitions);
    }
  }

  private void initialAssignment(InstancePartitions instancePartitions) {
    LOGGER.info("No existing instance partitions found. Will build new on top of"
        + " the pre-configured instance partitions");
    // create a list of lists of mirrored servers from the pre-configured instance partitions
    createMirrorServerListFromPreconfiguredInstancePartition();
    // shuffle the list of lists of mirrored servers based on the table name hash
    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    // initialize a list of indices from 0 to _numPreConfiguredInstancesPerReplicaGroup
    List<Integer> shuffledIndex = new ArrayList<>(_numPreConfiguredInstancesPerReplicaGroup);
    for (int i = 0; i < _numPreConfiguredInstancesPerReplicaGroup; i++) {
      shuffledIndex.add(i);
    }
    // shuffle the list of indices based on the table name hash
    Collections.shuffle(shuffledIndex, new Random(tableNameHash));
    // select the first _numTargetInstancesPerReplicaGroup indices
    shuffledIndex = shuffledIndex.subList(0, _numTargetInstancesPerReplicaGroup);
    // sort the list of indices so that they follow the original order of the pre-configured instance partitions
    shuffledIndex.sort(Comparator.naturalOrder());

    // create the instance partitions based on the shuffled list of mirrored servers
    List<List<String>> resultReplicaGroups = new ArrayList<>(_numTargetReplicaGroups);
    for (int i = 0; i < _numTargetReplicaGroups; i++) {
      resultReplicaGroups.add(new ArrayList<>(_numTargetInstancesPerReplicaGroup));
    }

    // populate the instance partitions with the selected mirrored servers
    for (int j = 0; j < _numTargetInstancesPerReplicaGroup; j++) {
      for (int i = 0; i < _numTargetReplicaGroups; i++) {
        resultReplicaGroups.get(i).add(_preConfiguredMirroredServerLists.get(shuffledIndex.get(j)).get(i));
      }
    }
    for (int i = 0; i < _numTargetReplicaGroups; i++) {
      instancePartitions.setInstances(0, i, resultReplicaGroups.get(i));
    }
  }

  private void scale(InstancePartitions instancePartitions) {
    LOGGER.info("Existing instance partitions found. Will adjust the existing instance partitions"
        + " based on the pre-configured instance partitions");
    createMirrorServerListFromPreconfiguredInstancePartition();
    createMirrorServerListLookupTablesFromPreconfiguredInstancePartition();
    createListAndLookupTablesFromExistingInstancePartitions();
    Set<Integer> usedPreconfiguredInstanceOffsets = new HashSet<>();
    Map<Integer, Map.Entry<Integer, Long>> existingOffsetToResultTuple = new HashMap<>();

    // For each instance offset, find the mirrored server that is most similar to the existing mirrored server
    // set. If this mirrored server is not used, add it to the result list.
    for (int j = 0; j < _numExistingInstancesPerReplicaGroup; j++) {
      List<String> existingMirroredServers = _existingMirroredServerLists.get(j);
      int finalJ = j;
      existingMirroredServers.stream()
          .map(_preConfiguredInstanceNameToOffsetMap::get)
          .filter(Objects::nonNull)
          .filter(offset -> !usedPreconfiguredInstanceOffsets.contains(offset))
          .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
          .entrySet()
          .stream()
          .max(Map.Entry.comparingByValue())
          .ifPresent(e -> {
            existingOffsetToResultTuple.put(finalJ, e);
            usedPreconfiguredInstanceOffsets.add(e.getKey());
          });
    }

    if (_numExistingInstancesPerReplicaGroup > _numTargetInstancesPerReplicaGroup) {
      // If this is a downlift case
      List<Map.Entry<Integer, Long>> collect = existingOffsetToResultTuple.values()
          .stream()
          .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
          .limit(_numTargetInstancesPerReplicaGroup)
          .collect(Collectors.toList());
      int size = collect.size();
      existingOffsetToResultTuple.clear();
      usedPreconfiguredInstanceOffsets.clear();
      for (int j = 0; j < size; j++) {
        existingOffsetToResultTuple.put(j, collect.get(j));
        usedPreconfiguredInstanceOffsets.add(collect.get(j).getKey());
      }
    }

    if (existingOffsetToResultTuple.size() < _numTargetInstancesPerReplicaGroup) {
      // If the number of instances selected from the result list is less than the target number
      // of instances per replica group, add the remaining instances from the pre-configured instance partitions.
      ArrayList<Integer> shuffledOffsets = new ArrayList<>(_numPreConfiguredInstancesPerReplicaGroup);
      for (int j = 0; j < _numPreConfiguredInstancesPerReplicaGroup; j++) {
        shuffledOffsets.add(j);
      }
      // Commenting this out as
      // (1) Shuffling is already done in the initial step.
      // (2) We want to keep the order of the pre-configured instance partitions, so that the segment assignment
      //     strategy for single tenant cluster can be minimized-impact.
      // But keeping the code here in case we want to have a specific reordering strategy in the future.
      // Collections.shuffle(shuffledOffsets, new Random(Math.abs(_tableNameWithType.hashCode())));
      for (int k = 0, j = 0; j < _numTargetInstancesPerReplicaGroup; j++) {
        if (existingOffsetToResultTuple.containsKey(j)) {
          continue;
        }
        while (usedPreconfiguredInstanceOffsets.contains(shuffledOffsets.get(k))) {
          k++;
        }
        Integer offset = shuffledOffsets.get(k);
        existingOffsetToResultTuple.put(j, new AbstractMap.SimpleEntry<>(offset, 0L));
        usedPreconfiguredInstanceOffsets.add(offset);
      }
    }

    List<List<String>> resultReplicaGroups = new ArrayList<>(_numTargetReplicaGroups);
    for (int i = 0; i < _numTargetReplicaGroups; i++) {
      resultReplicaGroups.add(new ArrayList<>(_numTargetInstancesPerReplicaGroup));
    }
    for (int j = 0; j < _numTargetInstancesPerReplicaGroup; j++) {
      List<String> mirrorServers = _preConfiguredMirroredServerLists.get(existingOffsetToResultTuple.get(j).getKey());
      for (int i = 0; i < _numTargetReplicaGroups; i++) {
        resultReplicaGroups.get(i).add(mirrorServers.get(i));
      }
    }
    for (int i = 0; i < _numTargetReplicaGroups; i++) {
      instancePartitions.setInstances(0, i, resultReplicaGroups.get(i));
    }
  }

  private void createListAndLookupTablesFromExistingInstancePartitions() {
    List<List<String>> existingReplicaGroups = new ArrayList<>(_numExistingReplicaGroups);
    for (int i = 0; i < _numExistingReplicaGroups; i++) {
      existingReplicaGroups.add(_existingInstancePartitions.getInstances(0, i));
    }

    for (int j = 0; j < _numExistingInstancesPerReplicaGroup; j++) {
      List<String> existingMirroredServerList = new ArrayList<>();
      for (int i = 0; i < _numExistingReplicaGroups; i++) {
        existingMirroredServerList.add(existingReplicaGroups.get(i).get(j));
      }
      _existingMirroredServerLists.add(existingMirroredServerList);
    }
  }
}
