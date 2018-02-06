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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.RoutingTableBuilderName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;


/**
 * Generates partition assignment for all partition aware routing tables
 * The partition assignment is done by uniformly spraying the partitions across available instances,
 * such that the assignment can be used for a partition aware routing
 *
 * NOTE: We do not support/expect partition aware tables with multi tenant setup, hence this strategy should suffice
 */
public class PartitionAwareAssignmentGenerator extends PartitionAssignmentGenerator {

  public PartitionAwareAssignmentGenerator(TableConfig tableConfig, int numPartitions,
      List<String> instanceNames, Map<String, TableConfig> allTableConfigsInTenant,
      Map<String, ZNRecord> currentPartitionAssignment) {

    super(tableConfig, numPartitions, instanceNames, currentPartitionAssignment);

    _tablesForPartitionAssignment = new ArrayList<>();
    for (TableConfig config : allTableConfigsInTenant.values()) {
      if (config.getRoutingConfig() != null && config.getRoutingConfig()
          .getRoutingTableBuilderName()
          .equals(RoutingTableBuilderName.PartitionAwareRealtime.toString())) {
        _tablesForPartitionAssignment.add(config.getTableName());
      }
    }
  }

  @Override
  public Map<String, ZNRecord> generatePartitionAssignment() {
    Map<String, ZNRecord> tableNameToPartitionAssignment = new HashMap<>(_tablesForPartitionAssignment.size());

    for (String realtimeTableName : _tablesForPartitionAssignment) {
      ZNRecord znRecord = new ZNRecord(realtimeTableName);
      int serverId = 0;
      for (int p = 0; p < _tableToNumPartitions.get(realtimeTableName); p++) {
        List<String> instances = new ArrayList<>(_numReplicas);
        for (int r = 0; r < _numReplicas; r++) {
          instances.add(_instanceNames.get(serverId++));
          if (serverId == _instanceNames.size()) {
            serverId = 0;
          }
        }
        znRecord.setListField(Integer.toString(p), instances);
      }
      tableNameToPartitionAssignment.put(realtimeTableName, znRecord);
    }
    return tableNameToPartitionAssignment;
  }
}
