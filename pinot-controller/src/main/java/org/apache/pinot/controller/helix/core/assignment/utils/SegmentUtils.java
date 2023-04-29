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

import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;

public class SegmentUtils {
  private SegmentUtils() {
  }

  /**
   * Get the partition column from InstanceAssignmentConfigUtils
   * @param tableConfig table config
   * @return partition column
   */
  public static String getPartitionColumn(TableConfig tableConfig) {
    String partitionColumn;
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      InstanceAssignmentConfig instanceAssignmentConfig =
          InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE);

      partitionColumn = instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn();
    } else {
      InstanceAssignmentConfig instanceAssignmentConfig =
          InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.CONSUMING);
      partitionColumn = instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn();
      if (partitionColumn == null) {
        instanceAssignmentConfig = InstanceAssignmentConfigUtils.
            getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.CONSUMING);
        partitionColumn = instanceAssignmentConfig.getReplicaGroupPartitionConfig().getPartitionColumn();
      }
    }
    return partitionColumn;
  }
}
