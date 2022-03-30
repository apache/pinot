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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Driver for the instance assignment.
 * <p>Instance assignment is performed in 3 steps:
 * <ul>
 *   <li>Select instances based on the tag/pool configuration</li>
 *   <li>Apply constraints to the instances (optional, multiple constraints can be chained up)</li>
 *   <li>Select instances based on the replica-group/partition configuration</li>
 * </ul>
 */
public class InstanceAssignmentDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceAssignmentDriver.class);

  private final TableConfig _tableConfig;

  public InstanceAssignmentDriver(TableConfig tableConfig) {
    _tableConfig = tableConfig;
  }

  /**
   * Assign instances to InstancePartitions object.
   * @param instancePartitionsType type of instance partitions
   * @param instanceConfigs list of instance configs
   * @param existingPoolToInstancesMap a map of existing pools and the instances with sequence that should be respected.
   *                                   If it's null, there is no requirement to respect the existing sequence and thus
   *                                   the instance will be sorted.
   *                                   If it's not null but an empty list, there is no preceding sequence to respect,
   *                                   and the instances will also be sorted.
   */
  public InstancePartitions assignInstances(InstancePartitionsType instancePartitionsType,
      List<InstanceConfig> instanceConfigs, @Nullable Map<Integer, List<String>> existingPoolToInstancesMap) {
    boolean shouldRetainInstanceSequence = (existingPoolToInstancesMap != null);
    String tableNameWithType = _tableConfig.getTableName();
    LOGGER.info("Starting {} instance assignment for table: {}. Should retain instance sequence: {}",
        instancePartitionsType, tableNameWithType, shouldRetainInstanceSequence);

    // Step 0: initialize an instance assignment config.
    InstanceAssignmentConfig assignmentConfig =
        InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(_tableConfig, instancePartitionsType);

    // Step 1: select pools and lists of instance configs given the latest instance configs from the ZK.
    InstanceTagPoolSelector tagPoolSelector = TagPoolSelectorFactory
        .getInstanceTagPoolSelector(tableNameWithType, assignmentConfig.getTagPoolConfig(), existingPoolToInstancesMap);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = tagPoolSelector.selectInstances(instanceConfigs);

    // Step 2: apply instance constraints to the poolToInstanceConfigsMap.
    List<InstanceConstraintApplier> constraintAppliers = InstanceConstraintAppliersFactory
        .constructConstraintAppliers(tableNameWithType, assignmentConfig.getConstraintConfig(),
            existingPoolToInstancesMap);
    for (InstanceConstraintApplier constraintApplier : constraintAppliers) {
      poolToInstanceConfigsMap = constraintApplier.applyConstraint(poolToInstanceConfigsMap);
    }

    // Step 3: select instances based on the replica group partition strategy.
    InstanceReplicaGroupPartitionSelector replicaPartitionSelector =
        new InstanceReplicaGroupPartitionSelector(assignmentConfig.getReplicaGroupPartitionConfig(), tableNameWithType);
    InstancePartitions instancePartitions = new InstancePartitions(
        instancePartitionsType.getInstancePartitionsName(TableNameBuilder.extractRawTableName(tableNameWithType)));
    replicaPartitionSelector.selectInstances(poolToInstanceConfigsMap, instancePartitions);

    // Step 4: persist instance sequence if necessary.
    if (shouldRetainInstanceSequence) {
      // Keep the pool to instances map if instance sequence should be retained.
      // This map will be persisted into ZNode if dryRun is false.
      instancePartitions
          .setPoolToInstancesMap(generatePoolToInstanceNamesMapFromPoolToInstanceConfigsMap(poolToInstanceConfigsMap));
    }
    return instancePartitions;
  }

  private Map<Integer, List<String>> generatePoolToInstanceNamesMapFromPoolToInstanceConfigsMap(
      Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap) {
    Map<Integer, List<String>> partitionToInstancesMap = new TreeMap<>();
    for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
      Integer pool = entry.getKey();
      List<InstanceConfig> instanceConfigs = entry.getValue();
      partitionToInstancesMap.put(pool, extractInstanceNamesFromInstanceConfigs(instanceConfigs));
    }
    return partitionToInstancesMap;
  }

  private List<String> extractInstanceNamesFromInstanceConfigs(List<InstanceConfig> instanceConfigs) {
    List<String> instanceNames = new ArrayList<>(instanceConfigs.size());
    for (InstanceConfig instanceConfig : instanceConfigs) {
      instanceNames.add(instanceConfig.getInstanceName());
    }
    return instanceNames;
  }
}
