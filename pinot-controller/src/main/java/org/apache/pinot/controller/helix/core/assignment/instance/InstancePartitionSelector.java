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
package org.apache.pinot.controller.helix.core.assignment.instance;

import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;


abstract class InstancePartitionSelector {
  protected final InstanceReplicaGroupPartitionConfig _replicaGroupPartitionConfig;
  protected final String _tableNameWithType;
  protected final InstancePartitions _existingInstancePartitions;
  protected final boolean _minimizeDataMovement;

  public InstancePartitionSelector(InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig,
      String tableNameWithType, InstancePartitions existingInstancePartitions, boolean minimizeDataMovement) {
    _replicaGroupPartitionConfig = replicaGroupPartitionConfig;
    _tableNameWithType = tableNameWithType;
    _existingInstancePartitions = existingInstancePartitions;
    _minimizeDataMovement = minimizeDataMovement;
  }

  /**
   * Selects instances based on the replica-group/partition config, and stores the result into the given instance
   * partitions.
   */
  abstract void selectInstances(Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap,
      InstancePartitions instancePartitions);
}
