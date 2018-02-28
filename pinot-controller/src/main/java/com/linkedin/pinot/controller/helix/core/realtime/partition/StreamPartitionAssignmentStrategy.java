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
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import java.util.List;
import java.util.Map;


/**
 * Interface for stream partition assignment strategy across realtime tables
 */
public interface StreamPartitionAssignmentStrategy {

  /**
   * Initialize with list of all table configs in the tenant,
   * the instances over which to generate partition assignment and the current partition assignment
   * @param allTablesInTenant
   * @param instanceNames
   * @param tableNameToPartitionAssignment
   */
  void init(List<TableConfig> allTablesInTenant, List<String> instanceNames,
      Map<String, PartitionAssignment> tableNameToPartitionAssignment);

  /**
   * Generates partition assignment for the table, and all other tables which might need a reassignment
   * @param tableConfig
   * @param numPartitions
   * @return
   */
  Map<String, PartitionAssignment> generatePartitionAssignment(TableConfig tableConfig, int numPartitions);
}
