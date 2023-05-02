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
package org.apache.pinot.controller.helix.core.assignment.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;

public class SegmentUtils {
  private SegmentUtils() {
  }

  /**
   * Get the partition column from InstanceAssignmentConfigUtils
   * @param tableConfig table config
   * @return partition column
   */
  public static String getPartitionColumn(TableConfig tableConfig) {
    String partitionColumn = null;

/*    // check getInstanceAssignmentConfigMap is null or empty,
    if (!MapUtils.isEmpty(tableConfig.getInstanceAssignmentConfigMap())) {
      for (String key : tableConfig.getInstanceAssignmentConfigMap().keySet()) {
        //check getInstanceAssignmentConfigMap has the key of TableType
        if (Objects.equals(key, tableConfig.getTableType().toString())) {
          // if true, set the partitionColumn value.
          partitionColumn = tableConfig.getInstanceAssignmentConfigMap().get(key).
              getReplicaGroupPartitionConfig().getPartitionColumn();
        }
      }
    }*/

    // check, if partitionColumn is not empty, return the value.
    if (!StringUtils.isEmpty(partitionColumn)) {
      return partitionColumn;
    }

    // for backward-compatibility, If partitionColumn value isn't there in InstanceReplicaGroupPartitionConfig
    // check ReplicaGroupStrategyConfig for partitionColumn
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    return replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
  }
}
