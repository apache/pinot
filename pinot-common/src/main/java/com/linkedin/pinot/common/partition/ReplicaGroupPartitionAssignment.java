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
package com.linkedin.pinot.common.partition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Class to represent a replica group partition assignment
 *
 */
public class ReplicaGroupPartitionAssignment extends PartitionAssignment {
  private static final String SEPARATOR = "_";

  public ReplicaGroupPartitionAssignment(String tableName) {
    super(tableName);
  }

  public ReplicaGroupPartitionAssignment(String tableName, Map<String, List<String>> partitionToInstances) {
    super(tableName, partitionToInstances);
  }

  /**
   * Compute total number of partitions in the mapping.
   *
   * @return the number of partitions
   */
  public int getNumPartitions() {
    Set<String> partitions = new HashSet<>();
    for (String key : getPartitionToInstances().keySet()) {
      String partitionId = key.split(SEPARATOR)[0];
      partitions.add(partitionId);
    }
    return partitions.size();
  }

  /**
   * Compute total number of replica groups in the mapping.
   *
   * @return the number of replica groups
   */
  public int getNumReplicaGroups() {
    Set<String> replicaGroups = new HashSet<>();
    for (String key : getPartitionToInstances().keySet()) {
      String replicaGroupId = key.split(SEPARATOR)[1];
      replicaGroups.add(replicaGroupId);
    }
    return replicaGroups.size();
  }

  /**
   * Add an instance to a replica group for a partition.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @param instanceName Name of an instance
   */
  public void addInstanceToReplicaGroup(int partition, int replicaGroup, String instanceName) {
    String key = createMappingKey(partition, replicaGroup);
    if (!getPartitionToInstances().containsKey(key)) {
      addPartition(key, new ArrayList<String>());
    }
    getInstancesListForPartition(key).add(instanceName);
  }

  /**
   * Get instances of a replica group for a partition.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @return List of instances belongs to the given partition and replica group
   */
  public List<String> getInstancesfromReplicaGroup(int partition, int replicaGroup) {
    String key = createMappingKey(partition, replicaGroup);
    if (!getPartitionToInstances().containsKey(key)) {
      throw new NoSuchElementException();
    }
    return getInstancesListForPartition(key);
  }

  /**
   * Set instances of a replica group for a partition.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @param instances Instances that belongs to the given partition and replica group
   */
  public void setInstancesToReplicaGroup(int partition, int replicaGroup, List<String> instances) {
    String key = createMappingKey(partition, replicaGroup);
    addPartition(key, instances);
  }

  /**
   * Helper method to create a key for the partition mapping table.
   *
   * @param partition Partition number
   * @param replicaGroup Replica group number
   * @return Key for the partition mapping table
   */
  private String createMappingKey(int partition, int replicaGroup) {
    return partition + SEPARATOR + replicaGroup;
  }

}
