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

import com.linkedin.pinot.common.config.StreamConsumptionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;


/**
 * Generates partition assignment for all tables with
 * streamPartitionAssignmentStrategy as BalancedStreamPartitionAssignment, using AutoRebalanceStrategy
 *
 * The partitions are evenly distributed across available instances.
 * Any other tables in the same tenant which also have BalancedStreamPartitionAssignmentStrategy will also be repartitioned
 * AutoRebalance Strategy tries to not move partitions unnecessarily
 *
 * An example znode for 8 kafka partitions and and 6 realtime servers (Server_s1 to Server_s6)
 * for a tableConfig with BalancedStreamPartitionAssignmentStrategy in zookeeper.
 *
 *
 * {
   "id":"KafkaTopicName"
      ,"simpleFields":{
   }
   ,"listFields":{
      "0":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
      ,"1":["Server_s2.company.com_8001","Server_s3.company.com_8001","Server_s4.company.com_8001"]
      ,"2":["Server_s3.company.com_8001","Server_s4.company.com_8001","Server_s5.company.com_8001"]
      ,"3":["Server_s4.company.com_8001","Server_s5.company.com_8001","Server_s6.company.com_8001"]
      ,"4":["Server_s5.company.com_8001","Server_s6.company.com_8001","Server_s1.company.com_8001"]
      ,"5":["Server_s6.company.com_8001","Server_s1.company.com_8001","Server_s2.company.com_8001"]
      ,"6":["Server_s1.company.com_8001","Server_s2.company.com_8001","Server_s3.company.com_8001"]
      ,"7":["Server_s2.company.com_8001","Server_s3.company.com_8001","Server_s4.company.com_8001"]
   }
   ,"mapFields":{
   }
 }
 *
 */
public class BalancedStreamPartitionAssignmentStrategy implements StreamPartitionAssignmentStrategy {

  private static final String PARTITION_JOINER = "_";

  private List<String> _tablesForPartitionAssignment;
  private List<String> _instanceNames;
  private Map<String, PartitionAssignment> _tableNameToPartitionAssignment;

  @Override
  public void init(List<TableConfig> allTablesInTenant, List<String> instanceNames,
      Map<String, PartitionAssignment> tableNameToPartitionAssignment) {

    // NOTE: we only consider those tables which have explicitly stated that their streamPartitionAssignmentStrategy
    // is Balanced. We treat Uniform strategy as our default, which writes only a single table,
    // until we resolve race conditions in the znodes read/update code.
    // Therefore, unless explicitly asked for, Balanced strategy will not be applied to the tables

    _tablesForPartitionAssignment = new ArrayList<>();
    for (TableConfig tableConfig : allTablesInTenant) {
      StreamConsumptionConfig streamConsumptionConfig = tableConfig.getIndexingConfig().getStreamConsumptionConfig();
      if (streamConsumptionConfig != null && streamConsumptionConfig.getStreamPartitionAssignmentStrategy()
          .equals(StreamPartitionAssignmentStrategyEnum.BalancedStreamPartitionAssignment.toString())) {
        _tablesForPartitionAssignment.add(tableConfig.getTableName());
      }
    }
    _instanceNames = instanceNames;
    _tableNameToPartitionAssignment = tableNameToPartitionAssignment;
  }

  @Override
  public Map<String, PartitionAssignment> generatePartitionAssignment(TableConfig tableConfig, int numPartitions) {

    Map<String, PartitionAssignment> newPartitionAssignment =
        new HashMap<>(_tablesForPartitionAssignment.size());

    // get num replicas
    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    // get num partitions
    Map<String, Integer> tableToNumPartitions = new HashMap<>(_tablesForPartitionAssignment.size());
    for (String realtimeTableName : _tablesForPartitionAssignment) {
      PartitionAssignment partitionAssignment = _tableNameToPartitionAssignment.get(realtimeTableName);
      if (partitionAssignment != null) {
        tableToNumPartitions.put(realtimeTableName, partitionAssignment.getNumPartitions());
      }
    }
    tableToNumPartitions.put(tableConfig.getTableName(), numPartitions);

    Map<String, Map<String, String>> currentPartitions = new HashMap<>(_tablesForPartitionAssignment.size());
    List<String> newPartitions = new ArrayList<>();

    for (String realtimeTableName : _tablesForPartitionAssignment) {

      // construct current partitions map
      PartitionAssignment partitionAssignment = _tableNameToPartitionAssignment.get(realtimeTableName);
      if (partitionAssignment != null) {

        Map<String, List<String>> partitionToInstances = _tableNameToPartitionAssignment.get(realtimeTableName).getPartitionToInstances();
        for (Map.Entry<String, List<String>> partition : partitionToInstances.entrySet()) {
          String key = realtimeTableName + PARTITION_JOINER + partition.getKey();
          Map<String, String> value = new HashMap<>();
          for (String instance : partition.getValue()) {
            value.put(instance, RealtimeSegmentOnlineOfflineStateModel.ONLINE);
          }
          currentPartitions.put(key, value);
        }
      }

      // construct new partitions list
      if (tableToNumPartitions.get(realtimeTableName) != null) { // if new table was added by another controller to the same tenant,
        // we will find it in this list, but not have the current partition assignment for it
        for (int i = 0; i < tableToNumPartitions.get(realtimeTableName); i++) {
          newPartitions.add(realtimeTableName + PARTITION_JOINER + Integer.toString(i));
        }
      }
    }

    // get states
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
    states.put(RealtimeSegmentOnlineOfflineStateModel.OFFLINE, 0);
    states.put(RealtimeSegmentOnlineOfflineStateModel.ONLINE, numReplicas);

    // auto rebalance
    AutoRebalanceStrategy autoRebalanceStrategy =
        new AutoRebalanceStrategy(tableConfig.getTableName(), newPartitions, states);
    ZNRecord partitionAssignmentAcrossAllTables =
        autoRebalanceStrategy.computePartitionAssignment(_instanceNames, _instanceNames, currentPartitions, null);

    // extract partitions from auto rebalance result
    Map<String, List<String>> listFields = partitionAssignmentAcrossAllTables.getListFields();
    for (Map.Entry<String, List<String>> entry : listFields.entrySet()) {
      String partitionName = entry.getKey();
      int lastIndex = partitionName.lastIndexOf(PARTITION_JOINER);
      String tableName = partitionName.substring(0, lastIndex);
      String partitionNumber = partitionName.substring(lastIndex + 1);

      PartitionAssignment partitionAssignmentForTable = newPartitionAssignment.get(tableName);
      if (partitionAssignmentForTable == null) {
        partitionAssignmentForTable = new PartitionAssignment(tableName);
        newPartitionAssignment.put(tableName, partitionAssignmentForTable);
      }
      partitionAssignmentForTable.addPartition(partitionNumber, entry.getValue());
    }

    return newPartitionAssignment;
  }
}
