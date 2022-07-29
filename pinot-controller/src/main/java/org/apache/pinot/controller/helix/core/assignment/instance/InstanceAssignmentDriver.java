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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
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

  public InstancePartitions assignInstances(InstancePartitionsType instancePartitionsType,
      List<InstanceConfig> instanceConfigs, @Nullable InstancePartitions existingInstancePartitions) {
    String tableNameWithType = _tableConfig.getTableName();
    Preconditions.checkState(!TableConfigUtils.hasPreConfiguredInstancePartitions(
        _tableConfig, instancePartitionsType));

    InstanceAssignmentConfig assignmentConfig =
        InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(_tableConfig, instancePartitionsType);
    String instancePartitionsName = instancePartitionsType.getInstancePartitionsName(
        TableNameBuilder.extractRawTableName(tableNameWithType));
    return assignInstances(instancePartitionsName,
        tableNameWithType, instanceConfigs, assignmentConfig, existingInstancePartitions);
  }

  /**
   * Assigns instance to a given group, and returns the "group instance partitions", which essentially
   * means that the InstancePartitions name has a _GROUP suffix.
   */
  public static InstancePartitions assignInstancesToGroup(String groupName,
      List<InstanceConfig> instanceConfigs, InstanceAssignmentConfig assignmentConfig) {
    String instancePartitionsName = InstancePartitionsUtils.getGroupInstancePartitionsName(groupName);
    return assignInstances(instancePartitionsName, groupName, instanceConfigs, assignmentConfig, null);
  }

  private static InstancePartitions assignInstances(String instancePartitionsName, String entityName,
      List<InstanceConfig> instanceConfigs, InstanceAssignmentConfig assignmentConfig,
      @Nullable InstancePartitions existingInstancePartitions) {
    LOGGER.info("Starting instance assignment for entity: {}", entityName);

    InstanceTagPoolSelector tagPoolSelector =
        new InstanceTagPoolSelector(assignmentConfig.getTagPoolConfig(), entityName);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = tagPoolSelector.selectInstances(instanceConfigs);

    InstanceConstraintConfig constraintConfig = assignmentConfig.getConstraintConfig();
    List<InstanceConstraintApplier> constraintAppliers = new ArrayList<>();
    if (constraintConfig == null) {
      LOGGER.info("No instance constraint is configured, using default hash-based-rotate instance constraint");
      constraintAppliers.add(new HashBasedRotateInstanceConstraintApplier(entityName));
    }
    // TODO: support more constraints
    for (InstanceConstraintApplier constraintApplier : constraintAppliers) {
      poolToInstanceConfigsMap = constraintApplier.applyConstraint(poolToInstanceConfigsMap);
    }

    InstancePartitionSelector instancePartitionSelector =
        InstancePartitionSelectorFactory.getInstance(assignmentConfig.getPartitionSelector(),
            assignmentConfig.getReplicaGroupPartitionConfig(), entityName, existingInstancePartitions);
    InstancePartitions instancePartitions = new InstancePartitions(instancePartitionsName);
    instancePartitionSelector.selectInstances(poolToInstanceConfigsMap, instancePartitions);
    return instancePartitions;
  }
}
