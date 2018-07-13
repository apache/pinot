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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Class to help to read, write, and generate replica group partition assignment
 *
 */
public class ReplicaGroupPartitionAssignmentGenerator {

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public ReplicaGroupPartitionAssignmentGenerator(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  /**
   * Read the replica group partition assignment from the property store
   * @param tableNameWithType a table name
   * @return Replica group partition assignment
   */
  public ReplicaGroupPartitionAssignment getReplicaGroupPartitionAssignment(String tableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(tableNameWithType);
    ZNRecord replicaGroupPartitionAssignment = _propertyStore.get(path, null, AccessOption.PERSISTENT);
    ReplicaGroupPartitionAssignment partitionAssignment = null;
    if (replicaGroupPartitionAssignment != null) {
      partitionAssignment =
          new ReplicaGroupPartitionAssignment(tableNameWithType, replicaGroupPartitionAssignment.getListFields());
    }
    return partitionAssignment;
  }

  /**
   * Write the replica group partition assignment to property store
   *
   * @param partitionAssignment a replica group partition assignment
   */
  public void writeReplicaGroupPartitionAssignment(ReplicaGroupPartitionAssignment partitionAssignment) {
    String tableNameWithType = partitionAssignment.getTableName();
    ZNRecord znRecord = new ZNRecord(tableNameWithType);
    znRecord.setListFields(partitionAssignment.getPartitionToInstances());
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(tableNameWithType);
    _propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
  }

  /**
   * Build the partition mapping table that maps a tuple of (partition number, replica group number) to a list of
   * servers. Two important configurations are explained below.
   *
   * 'numInstancesPerPartition': this number decides the number of servers within a replica group.
   * 'partitionColumn': this configuration decides whether to use the table or partition level replica groups.
   *
   * @param tableNameWithType: Name of table
   * @param tableConfig: Configuration for table
   * @return Replica group partition assignment from the given configuration
   */
  public ReplicaGroupPartitionAssignment buildReplicaGroupPartitionAssignment(String tableNameWithType,
      TableConfig tableConfig, List<String> servers) {

    // Fetch information required to build the mapping table from the table configuration.
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    Preconditions.checkNotNull(replicaGroupStrategyConfig, "Replica group strategy config should not be null");

    String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();
    int numInstancesPerPartition = replicaGroupStrategyConfig.getNumInstancesPerPartition();

    // If we do not have the partition column configuration, we assume to use the table level replica groups,
    // which is equivalent to have the same partition number for all segments (i.e. 1 partition).
    int numColumnPartitions = 1;
    if (partitionColumn != null) {
      numColumnPartitions = tableConfig.getIndexingConfig().getSegmentPartitionConfig().getNumPartitions(partitionColumn);
    }
    int numReplicas = tableConfig.getValidationConfig().getReplicationNumber();
    int numServers = servers.size();

    // Enforcing disjoint server sets for each replica group.
    if (numInstancesPerPartition * numReplicas > numServers) {
      throw new UnsupportedOperationException("Replica group aware segment assignment assumes that servers in "
          + "each replica group are disjoint. Check the configurations to see if the following inequality holds. "
          + "'numInstancePerPartition' * 'numReplicas' <= 'totalServerNumbers'");
    }

    // Creating a mapping table
    ReplicaGroupPartitionAssignment partitionAssignment = new ReplicaGroupPartitionAssignment(tableNameWithType);

    Collections.sort(servers);
    for (int partitionId = 0; partitionId < numColumnPartitions; partitionId++) {
      // If the configuration contains partition column information, we use the segment level replica groups.
      if (numColumnPartitions != 1) {
        Collections.shuffle(servers);
      }
      for (int i = 0; i < numInstancesPerPartition * numReplicas; i++) {
        int groupId = i / numInstancesPerPartition;
        partitionAssignment.addInstanceToReplicaGroup(partitionId, groupId, servers.get(i));
      }
    }
    return partitionAssignment;
  }
}