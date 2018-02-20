/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.controller.helix.core.realtime.partition;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Class to help generate partition assignment, given the table config, number of stream partitions,
 * instances and all tables needing reassignment
 */
public class StreamPartitionAssignmentGenerator {

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public StreamPartitionAssignmentGenerator(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  /**
   * Generates partition assignment for given table, given num partitions over given instances
   */
  public Map<String, PartitionAssignment> generatePartitionAssignment(TableConfig tableConfig, int numPartitions,
      List<String> instanceNames, List<String> allTables) {

    // get table configs for all tables in same tenant
    List<TableConfig> allTableConfigs = new ArrayList<>(allTables.size());

    // get current partition assignments for all tables in same tenant
    Map<String, PartitionAssignment> tableNameToPartitionAssignment = new HashMap<>(allTables.size());

    for (String tableName : allTables) {
      allTableConfigs.add(getRealtimeTableConfig(tableName));
      Map<String, List<String>> partitionToInstancesMap = getPartitionsToInstances(tableName);
      if (partitionToInstancesMap != null) {
        PartitionAssignment partitionAssignment = new PartitionAssignment(tableName, partitionToInstancesMap);
        tableNameToPartitionAssignment.put(tableName, partitionAssignment);
      }
    }

    StreamPartitionAssignmentStrategy streamPartitionAssignmentStrategy =
        StreamPartitionAssignmentStrategyFactory.getStreamPartitionAssignmentStrategy(tableConfig);

    streamPartitionAssignmentStrategy.init(allTableConfigs, instanceNames, tableNameToPartitionAssignment);
    Map<String, PartitionAssignment> newPartitionAssignment = streamPartitionAssignmentStrategy.
        generatePartitionAssignment(tableConfig, numPartitions);

    return newPartitionAssignment;
  }

  protected TableConfig getRealtimeTableConfig(String tableNameWithType) {
    return ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
  }

  /**
   * Given a realtime table name, fetches the stream partition assignment for that table from property store
   * @param realtimeTableName
   * @return
   */
  protected Map<String, List<String>> getPartitionsToInstances(String realtimeTableName) {
    Map<String, List<String>> partitionToInstanceMap = null;
    ZNRecord kafkaPartitionAssignment = getKafkaPartitionAssignment(realtimeTableName);
    if (kafkaPartitionAssignment != null) {
      partitionToInstanceMap = kafkaPartitionAssignment.getListFields();
    }
    return partitionToInstanceMap;
  }

  protected ZNRecord getKafkaPartitionAssignment(final String realtimeTableName) {
    final String path = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(realtimeTableName);
    return _propertyStore.get(path, null, AccessOption.PERSISTENT);
  }

  /**
   * Given map of table name to stream partition assignment, construct and write znodes to property store one by one
   * @param newPartitionAssignment
   */
  public void writeKafkaPartitionAssignment(Map<String, PartitionAssignment> newPartitionAssignment) {
    for (Map.Entry<String, PartitionAssignment> entry : newPartitionAssignment.entrySet()) {
      ZNRecord znRecord = new ZNRecord(entry.getKey());
      Map<String, List<String>> partitionToInstances = entry.getValue().getPartitionToInstances();
      for (Map.Entry<String, List<String>> partition : partitionToInstances.entrySet()) {
        znRecord.setListField(partition.getKey(), partition.getValue());
      }
      final String path = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(entry.getKey());
      _propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
    }
  }
}
