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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;


/**
 * Base class for partition assignment generation across tables of a tenant
 */
public abstract class PartitionAssignmentGenerator {

  List<String> _tablesForPartitionAssignment;
  List<String> _instanceNames;
  int _nReplicas;
  Map<String, Integer> _nPartitions;
  Map<String, ZNRecord> _currentPartitionAssignment;

  /**
   * Sets up the fields required for the partition assignment algorithm
   * @param tableConfig Table config requesting the partition assignment generation
   * @param nKafkaPartitions number of kafka partitions found for the table
   * @param instanceNames instance names over which to generate the partition assignment
   * @param currentPartitionAssignment current partition assignment for all tables
   */
  PartitionAssignmentGenerator(TableConfig tableConfig, int nKafkaPartitions, List<String> instanceNames,
      Map<String, ZNRecord> currentPartitionAssignment) {

    _currentPartitionAssignment = currentPartitionAssignment;
    _instanceNames = instanceNames;
    _nReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    _nPartitions = new HashMap<>();
    for (Map.Entry<String, ZNRecord> entry : currentPartitionAssignment.entrySet()) {
      _nPartitions.put(entry.getKey(), entry.getValue().getListFields().size());
    }
    _nPartitions.put(tableConfig.getTableName(), nKafkaPartitions);
  }

  public abstract Map<String, ZNRecord> generatePartitionAssignment();
}
