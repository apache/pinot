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
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The instance replica-group/partition selector is responsible for selecting the instances for every replica-group/
 * partition, with each fault-domain mapping 1:1 to a server pool
 * Algorithm details and proof see
 * https://docs.google.com/document/d/1KmJ1DsYXVdzrojj_JYBHRJ2gRMQ5y-o63YqPs7ei7nI/edit#heading=h.nrxxvujq7ael
 */
public class FDAwareInstancePartitionSelector extends InstancePartitionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(FDAwareInstancePartitionSelector.class);

  public FDAwareInstancePartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, @Nullable InstancePartitions existingInstancePartitions, boolean minimizeDataMovement) {
    super(replicaGroupPartitionConfig, tableNameWithType, existingInstancePartitions, minimizeDataMovement);
  }

  /**
   * @return pair of (numFaultDomains, numTotalInstances)
   */
  private Pair<Integer, Integer> processFaultDomainPreconditions(
      Map<Integer, List<InstanceConfig>> faultDomainToInstanceConfigsMap) {
    int numFDs = faultDomainToInstanceConfigsMap.size();
    Preconditions.checkState(numFDs != 0, "No pool (fault-domain) qualified for selection");

    // Collect the number of instances in each FD into a list and sort
    List<Integer> numInstancesPerFD =
        faultDomainToInstanceConfigsMap.values().stream().map(List::size).sorted().collect(Collectors.toList());
    // Should have non-zero total instances
    Optional<Integer> totalInstancesOptional = numInstancesPerFD.stream().reduce(Integer::sum);
    Preconditions.checkState(totalInstancesOptional.orElse(0) > 0, "The number of total instances is zero");
    int numTotalInstances = totalInstancesOptional.get();
    // Assume best-effort round-robin assignment of instances to FDs, the assignment should be balanced
    // i.e., the difference between max num instances and min num instances should be at most 1
    Preconditions.checkState(numInstancesPerFD.get(numInstancesPerFD.size() - 1) - numInstancesPerFD.get(0) <= 1,
        "The instances are not balanced for each pool (fault-domain)");
    return new ImmutablePair<>(numFDs, numTotalInstances);
  }

  /**
   * @return (numReplicaGroups, numInstancesPerReplicaGroup)
   */
  private Pair<Integer, Integer> processReplicaGroupAssignmentPreconditions(int numFaultDomains, int numTotalInstances,
      InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig) {
    int numReplicaGroups = replicaGroupPartitionConfig.getNumReplicaGroups();
    Preconditions.checkState(numReplicaGroups > 0, "Number of replica-groups must be positive");
    int numInstancesPerReplicaGroup = replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup();
    if (numInstancesPerReplicaGroup > 0) {
      int numInstancesToSelect = numInstancesPerReplicaGroup * numReplicaGroups;

      Preconditions.checkState(numInstancesToSelect <= numTotalInstances,
          "Not enough qualified instances, ask for: (numInstancesPerReplicaGroup: %s) * "
              + "(numReplicaGroups: %s) = %s, having only %s",
          numInstancesPerReplicaGroup, numReplicaGroups, numInstancesToSelect, numTotalInstances);
    } else {
      Preconditions.checkState(numTotalInstances % numReplicaGroups == 0,
          "The total num instances %s cannot be assigned evenly to %s replica groups, please "
              + "specify a numInstancesPerReplicaGroup in _replicaGroupPartitionConfig", numTotalInstances,
          numReplicaGroups);
      numInstancesPerReplicaGroup = numTotalInstances / numReplicaGroups;
    }

    if (numReplicaGroups > numFaultDomains) {
      LOGGER.info("Assigning {} replica groups to {} fault domains, "
              + "will have more than one replica group down if one fault domain is down", numReplicaGroups,
          numFaultDomains);
    } else {
      LOGGER.info("Assigning {} replica groups to {} fault domains", numReplicaGroups, numFaultDomains);
    }
    return new ImmutablePair<>(numReplicaGroups, numInstancesPerReplicaGroup);
  }

  @Override
  public void selectInstances(Map<Integer, List<InstanceConfig>> faultDomainToInstanceConfigsMap,
      InstancePartitions instancePartitions) {

    // Check if the instances are evenly assigned to each FD
    Pair<Integer, Integer> fdRet = processFaultDomainPreconditions(faultDomainToInstanceConfigsMap);
    int numFaultDomains = fdRet.getLeft();
    int numTotalInstances = fdRet.getRight();

    if (_replicaGroupPartitionConfig.isReplicaGroupBased()) {
      // Replica-group based selection

      // Check if the setup specified in _replicaGroupPartitionConfig can be satisfied
      Pair<Integer, Integer> rgRet =
          processReplicaGroupAssignmentPreconditions(numFaultDomains, numTotalInstances, _replicaGroupPartitionConfig);
      int numReplicaGroups = rgRet.getLeft();
      int numInstancesPerReplicaGroup = rgRet.getRight();

      /*
       * create an FD_id -> instance_names map and initialize with current instances, we will later exclude instances
       * in existing assignment, and use the rest instances to do the assigment
       */
      Map<Integer, LinkedHashSet<String>> faultDomainToCandidateInstancesMap = new TreeMap<>();
      faultDomainToInstanceConfigsMap.forEach(
          (k, v) -> faultDomainToCandidateInstancesMap.put(k, new LinkedHashSet<String>() {{
            v.forEach(instance -> add(instance.getInstanceName()));
          }}));

      // create an instance_name -> FD_id map, just for look up
      Map<String, Integer> aliveInstanceNameToFDMap = new HashMap<>();
      faultDomainToCandidateInstancesMap.forEach(
          (faultDomainId, value) -> value.forEach(instance -> aliveInstanceNameToFDMap.put(instance, faultDomainId)));

      // replicaGroupBasedAssignmentState for assignment
      ReplicaGroupBasedAssignmentState replicaGroupBasedAssignmentState = null;

      /*
       * initialize the new replicaGroupBasedAssignmentState for assignment,
       * place existing instances in their corresponding positions
       */
      if (_minimizeDataMovement) {
        int numExistingReplicaGroups = _existingInstancePartitions.getNumReplicaGroups();
        int numExistingPartitions = _existingInstancePartitions.getNumPartitions();
        /*
         * reconstruct a replica group -> instance mapping from _existingInstancePartitions,
         */
        LinkedHashSet<String> existingReplicaGroup = new LinkedHashSet<>();
        for (int i = 0; i < numExistingReplicaGroups; i++, existingReplicaGroup.clear()) {
          for (int j = 0; j < numExistingPartitions; j++) {
            existingReplicaGroup.addAll(_existingInstancePartitions.getInstances(j, i));
          }

          /*
           * We can only know the numExistingInstancesPerReplicaGroup after we reconstructed the sequence of instances
           * in the first replica group
           */
          if (i == 0) {
            int numExistingInstancesPerReplicaGroup = existingReplicaGroup.size();
            replicaGroupBasedAssignmentState =
                new ReplicaGroupBasedAssignmentState(numReplicaGroups, numInstancesPerReplicaGroup,
                    numExistingReplicaGroups, numExistingInstancesPerReplicaGroup, numFaultDomains);
          }

          replicaGroupBasedAssignmentState.reconstructExistingAssignment(existingReplicaGroup, i,
              aliveInstanceNameToFDMap);
        }
      } else {
        // Fresh new assignment
        replicaGroupBasedAssignmentState =
            new ReplicaGroupBasedAssignmentState(numReplicaGroups, numInstancesPerReplicaGroup, numFaultDomains);
      }
      /*finish replicaGroupBasedAssignmentState initialization*/

      Preconditions.checkState(replicaGroupBasedAssignmentState != null);

      // preprocess the downsizing and exclude unchanged existing assigment from the candidate list
      replicaGroupBasedAssignmentState.preprocessing(faultDomainToCandidateInstancesMap);

      // preprocess the problem of numReplicaGroups >= numFaultDomains to a problem
      replicaGroupBasedAssignmentState.normalize(faultDomainToCandidateInstancesMap);

      // fill the remaining vacant seats
      replicaGroupBasedAssignmentState.fill(faultDomainToCandidateInstancesMap);

      // adjust the instance assignment to achieve the invariant state
      replicaGroupBasedAssignmentState.swapToInvariantState();

      // In the following we assign instances to partitions.
      // TODO: refine this and segment assignment to minimize movement during numInstancesPerReplicaGroup uplift
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
            instancesInPartition.add(
                replicaGroupBasedAssignmentState.
                    _replicaGroupIdToInstancesMap[replicaGroupId][instanceIdInReplicaGroup].getInstanceName());
            instanceIdInReplicaGroup = (instanceIdInReplicaGroup + 1) % numInstancesPerReplicaGroup;
          }
          LOGGER.info("Selecting instances: {} for replica-group: {}, partition: {} for table: {}",
              instancesInPartition, replicaGroupId, partitionId, _tableNameWithType);
          instancePartitions.setInstances(partitionId, replicaGroupId, instancesInPartition);
        }
      }
    } else {
      throw new IllegalStateException("Non-replica-group based selection unfinished");
      // TODO:Non-replica-group based selection
    }
  }

  private static class CandidateQueue {
    NavigableMap<Integer, Deque<String>> _map;
    Integer _iter;

    CandidateQueue(Map<Integer, LinkedHashSet<String>> faultDomainToCandidateInstancesMap) {
      _map = new TreeMap<>();
      faultDomainToCandidateInstancesMap.entrySet().stream().filter(kv -> !kv.getValue().isEmpty())
          .forEach(kv -> _map.put(kv.getKey(), new LinkedList<>(kv.getValue())));
      _iter = _map.firstKey();
    }

    void seekKey(int startKey) {
      if (_map.containsKey(startKey)) {
        _iter = startKey;
      } else {
        _iter = _map.ceilingKey(_iter);
        _iter = (_iter == null && !_map.isEmpty()) ? _map.firstKey() : _iter;
      }
    }

    Instance getNextCandidate() {
      if (_iter == null) {
        throw new IllegalStateException("Illegal state in fault-domain-aware assignment");
      }

      Instance ret = new Instance(_map.get(_iter).pollFirst(), _iter, Instance.NEW_INSTANCE);

      if (_map.get(_iter).isEmpty()) {
        _map.remove(_iter);
      }

      _iter = _map.higherKey(_iter);
      _iter = (_iter == null && !_map.isEmpty()) ? _map.firstKey() : _iter;
      return ret;
    }
  }

  private static class ReplicaGroupBasedAssignmentState {
    private static final int INVALID_FD = -1;
    Instance[][] _replicaGroupIdToInstancesMap;
    int _numReplicaGroups;
    int _numInstancesPerReplicaGroup;
    int _numExistingReplicaGroups;
    int _numExistingInstancesPerReplicaGroup;
    int _numDownInstances = 0;
    int _mapDimReplicaGroup;
    int _mapDimInstancePerReplicaGroup;
    HashMap<String, Integer> _usedInstances = new HashMap<>();
    int _numFaultDomains;
    int[][] _fdCounter;

    ReplicaGroupBasedAssignmentState(int numReplicaGroups, int numInstancesPerReplicaGroup,
        int numExistingReplicaGroups, int numExistingInstancesPerReplicaGroup, int numFaultDomains) {

      _numFaultDomains = numFaultDomains;
      _numReplicaGroups = numReplicaGroups;
      _numInstancesPerReplicaGroup = numInstancesPerReplicaGroup;
      _numExistingReplicaGroups = numExistingReplicaGroups;
      _numExistingInstancesPerReplicaGroup = numExistingInstancesPerReplicaGroup;
      _mapDimReplicaGroup = Math.max(numExistingReplicaGroups, numReplicaGroups);
      _mapDimInstancePerReplicaGroup = Math.max(numExistingInstancesPerReplicaGroup, numInstancesPerReplicaGroup);

      _replicaGroupIdToInstancesMap = new Instance[_mapDimReplicaGroup][_mapDimInstancePerReplicaGroup];
      _fdCounter = new int[_mapDimInstancePerReplicaGroup][_numFaultDomains];
    }

    ReplicaGroupBasedAssignmentState(int numReplicaGroups, int numInstancesPerReplicaGroup, int numFaultDomains) {
      this(numReplicaGroups, numInstancesPerReplicaGroup, 0, numInstancesPerReplicaGroup, numFaultDomains);
    }

    /**
     * preprocess the downsizing and exclude unchanged existing assigment from the candidate list
     */
    public void preprocessing(Map<Integer, LinkedHashSet<String>> faultDomainToCandidateInstancesMap) {
      if (_numReplicaGroups < _numExistingReplicaGroups
          || _numInstancesPerReplicaGroup < _numExistingInstancesPerReplicaGroup) {
        throw new IllegalStateException("Downsizing unfinished");
        //TODO: Finish preprocessing for Downsizing using unSetInstance()
      }
      // remove unchanged existing assigment from the candidate list
      for (Map.Entry<String, Integer> instanceFDPair : this.getUsedInstances().entrySet()) {
        faultDomainToCandidateInstancesMap.get(instanceFDPair.getValue()).remove(instanceFDPair.getKey());
      }
    }

    private void setNewInstance(int replicaGroupId, int instanceIndex, Instance instance) {
      Preconditions.checkState(instance.getExistingReplicaGroupId() == Instance.NEW_INSTANCE);
      _replicaGroupIdToInstancesMap[replicaGroupId][instanceIndex] = instance;
      _usedInstances.put(instance.getInstanceName(), instance.getFaultDomainId());
      _fdCounter[instanceIndex][instance.getFaultDomainId()] += 1;
    }

    private void setExistingInstance(int replicaGroupId, int instanceIndex, String instance, int fdId) {
      _replicaGroupIdToInstancesMap[replicaGroupId][instanceIndex] = new Instance(instance, fdId, replicaGroupId);
      _usedInstances.put(instance, fdId);
      _fdCounter[instanceIndex][fdId] += 1;
    }

    private void unSetInstance(int replicaGroupId, int instanceIndex) {
      int fdId = _replicaGroupIdToInstancesMap[replicaGroupId][instanceIndex].getFaultDomainId();
      _usedInstances.remove(_replicaGroupIdToInstancesMap[replicaGroupId][instanceIndex].getInstanceName());
      _replicaGroupIdToInstancesMap[replicaGroupId][instanceIndex] = null;
      _fdCounter[instanceIndex][fdId] -= 1;
    }

    /**
     * From an exising replica group, remove the instances that are gone and set them in _replicaGroupIdToInstancesMap
     */
    public void reconstructExistingAssignment(LinkedHashSet<String> existingReplicaGroup, int replicaGroupId,
        Map<String, Integer> aliveInstanceNameToFDMap) {
      int instanceIndex = 0;
      for (String instance : existingReplicaGroup) {
        int fdId = aliveInstanceNameToFDMap.getOrDefault(instance, INVALID_FD);
        if (fdId != INVALID_FD) {
          // Existing assigment still alive should remain unchanged
          Preconditions.checkState(_replicaGroupIdToInstancesMap != null,
              "Error state, replicaGroupBasedAssignmentState is not initialized");
          setExistingInstance(replicaGroupId, instanceIndex, instance, fdId);
        } else {
          // Instance not in candidate instances. It is down and removed.
          _numDownInstances++;
        }
        instanceIndex++;
      }
    }

    public HashMap<String, Integer> getUsedInstances() {
      return _usedInstances;
    }

    /**
     * this function ensures that: for instances with the same instance id (_replicaGroupIdToInstancesMap[*][id]),
     * there are at least ceil(numReplicaGroups/numFaultDomain) instances from each fault domain
     */
    public void normalize(Map<Integer, LinkedHashSet<String>> faultDomainToCandidateInstancesMap) {
      LOGGER.info("Warning, normalizing isn't finished yet");
      //TODO: Finish normalizing for numReplicaGroups>numFaultDomains
    }

    private boolean isEmpty(int replicaGroupId, int instanceId) {
      return _replicaGroupIdToInstancesMap[replicaGroupId][instanceId] == null;
    }

    /**
     * Fill the vacant instances
     */
    public void fill(Map<Integer, LinkedHashSet<String>> faultDomainToCandidateInstancesMap) {
      // convert set to que and start to assign
      CandidateQueue candidateQueue = new CandidateQueue(faultDomainToCandidateInstancesMap);
      if (_numReplicaGroups != 0) { // uplift instance per replica group first if not a fresh new assignment
        for (int j = _numExistingInstancesPerReplicaGroup; j < _numInstancesPerReplicaGroup; j++) {
          for (int i = 0; i < _numReplicaGroups; i++) {
            setNewInstance(i, j, candidateQueue.getNextCandidate());
          }
        }
      }

      candidateQueue.seekKey(_numExistingReplicaGroups * _numExistingInstancesPerReplicaGroup % _numFaultDomains);
      for (int i = _numExistingReplicaGroups; i < _numReplicaGroups; i++) {
        for (int j = 0; j < _numExistingInstancesPerReplicaGroup; j++) {
          int rotatedInstanceIter = j;
          if (_numExistingInstancesPerReplicaGroup % _numFaultDomains == 0) {
            /*
             * if _numExistingInstancesPerReplicaGroup % _numFaultDomains == 0, explicitly rotate j for every replica
             * group, or we will always have the same FD id in the same position for all replica groups
             */
            rotatedInstanceIter = (rotatedInstanceIter + i) % _numExistingInstancesPerReplicaGroup;
          }
          setNewInstance(i, rotatedInstanceIter, candidateQueue.getNextCandidate());
        }
      }

      for (int i = 0; i < _numExistingReplicaGroups; i++) {
        for (int j = 0; j < _numExistingInstancesPerReplicaGroup; j++) {
          if (isEmpty(i, j)) {
            setNewInstance(i, j, candidateQueue.getNextCandidate());
          }
        }
      }
    }

    public void swapToInvariantState() {
    }
  }

  private static class Instance {
    static final int NEW_INSTANCE = -1;
    private final String _instanceName;
    private final int _faultDomainId;
    private final int _existingReplicaGroupId;

    public Instance(String instance, int faultDomainId, int existingReplicaGroupId) {
      _instanceName = instance;
      _faultDomainId = faultDomainId;
      _existingReplicaGroupId = existingReplicaGroupId;
    }

    String getInstanceName() {
      return _instanceName;
    }

    int getFaultDomainId() {
      return _faultDomainId;
    }

    int getExistingReplicaGroupId() {
      return _existingReplicaGroupId;
    }
  }
}
