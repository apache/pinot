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
package com.linkedin.pinot.controller.helix.core.realtime;

import com.linkedin.pinot.common.config.RoutingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.RoutingTableBuilderName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;


/**
 * Generates partition assignment for all tables provided, using AutoRebalanceStrategy
 */
public class AutoRebalancePartitionAssignmentGenerator extends PartitionAssignmentGenerator {

  public AutoRebalancePartitionAssignmentGenerator(TableConfig tableConfig, int nKafkaPartitions,
      List<String> instanceNames, Map<String, TableConfig> allTableConfigsInTenant,
      Map<String, ZNRecord> currentPartitionAssignment) {

    super(tableConfig, nKafkaPartitions, instanceNames, currentPartitionAssignment);

    _tablesForPartitionAssignment = new ArrayList<>();
    for (TableConfig config : allTableConfigsInTenant.values()) {
      RoutingConfig routingConfig = config.getRoutingConfig();
      if (routingConfig == null || routingConfig.getRoutingTableBuilderName() == null ||
          !RoutingTableBuilderName.PartitionAwareRealtime.toString().equals(routingConfig.getRoutingTableBuilderName())) {
        _tablesForPartitionAssignment.add(config.getTableName());
      }
    }
  }

  @Override
  public Map<String, ZNRecord> generatePartitionAssignment() {
    Map<String, ZNRecord> tableNameToPartitionAssignment = new HashMap<>(_tablesForPartitionAssignment.size());
    final String partitionJoiner = "_";

    Map<String, Map<String, String>> currentPartitions = new HashMap<>();
    List<String> newPartitions = new ArrayList<>();
    for (String realtimeTableName : _tablesForPartitionAssignment) {

      // construct current partitions map
      ZNRecord partitionAssignmentZNode = _currentPartitionAssignment.get(realtimeTableName);
      if (partitionAssignmentZNode != null) {

        Map<String, List<String>> partitionToInstances = partitionAssignmentZNode.getListFields();
        for (Map.Entry<String, List<String>> partitionEntry : partitionToInstances.entrySet()) {
          String partitionNumber = partitionEntry.getKey();
          List<String> partitionInstances = partitionEntry.getValue();
          String key = realtimeTableName + partitionJoiner + partitionNumber;
          Map<String, String> value = new HashMap<>();
          for (String instance : partitionInstances) {
            value.put(instance, "ONLINE");
          }
          currentPartitions.put(key, value);
        }
      }

      // construct new partitions list
      int p = _nPartitions.get(realtimeTableName);
      for (int i = 0; i < p; i++) {
        newPartitions.add(realtimeTableName + partitionJoiner + Integer.toString(i));
      }
    }

    // get states
    LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
    states.put("OFFLINE", 0);
    states.put("ONLINE", _nReplicas);

    // auto rebalance
    AutoRebalanceStrategy autoRebalanceStrategy =
        new AutoRebalanceStrategy(_tablesForPartitionAssignment.get(0), newPartitions, states);
    ZNRecord partitionAssignmentAcrossAllTables =
        autoRebalanceStrategy.computePartitionAssignment(_instanceNames, _instanceNames, currentPartitions, null);

    // extract kafka partition assignment znodes from auto rebalance result
    Map<String, List<String>> listFields = partitionAssignmentAcrossAllTables.getListFields();
    for (Map.Entry<String, List<String>> entry : listFields.entrySet()) {
      String partitionName = entry.getKey();
      int lastIndex = partitionName.lastIndexOf(partitionJoiner);
      String tableName = partitionName.substring(0, lastIndex);
      String partitionNumber = partitionName.substring(lastIndex + 1);

      ZNRecord znRecord = tableNameToPartitionAssignment.get(tableName);
      if (znRecord == null) {
        znRecord = new ZNRecord(tableName);
        tableNameToPartitionAssignment.put(tableName, znRecord);
      }
      znRecord.setListField(partitionNumber, entry.getValue());
    }

    return tableNameToPartitionAssignment;
  }
}
