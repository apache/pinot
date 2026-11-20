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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utility class for the instance assignment.
///
/// Instance assignment is performed in 3 steps:
/// - Select instances based on the tag/pool configuration
/// - Apply constraints to the instances (optional, multiple constraints can be chained up)
/// - Select instances based on the replica-group/partition configuration
public class InstanceAssignmentUtils {
  private InstanceAssignmentUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceAssignmentUtils.class);

  public static InstancePartitions assignInstances(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, List<InstanceConfig> instanceConfigs,
      @Nullable InstancePartitions existingInstancePartitions) {
    return assignInstances(tableConfig, instancePartitionsType, instanceConfigs, existingInstancePartitions,
        Enablement.DEFAULT);
  }

  public static InstancePartitions assignInstances(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, List<InstanceConfig> instanceConfigs,
      @Nullable InstancePartitions existingInstancePartitions, Enablement minimizeDataMovement) {
    String tableNameWithType = tableConfig.getTableName();
    InstanceAssignmentConfig assignmentConfig =
        InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, instancePartitionsType);
    return getInstancePartitions(tableConfig,
        instancePartitionsType.getInstancePartitionsName(TableNameBuilder.extractRawTableName(tableNameWithType)),
        assignmentConfig, instanceConfigs, existingInstancePartitions, null, minimizeDataMovement);
  }

  public static InstancePartitions assignInstances(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, List<InstanceConfig> instanceConfigs,
      @Nullable InstancePartitions existingInstancePartitions,
      @Nullable InstancePartitions preConfiguredInstancePartitions) {
    return assignInstances(tableConfig, instancePartitionsType, instanceConfigs, existingInstancePartitions,
        preConfiguredInstancePartitions, Enablement.DEFAULT);
  }

  public static InstancePartitions assignInstances(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, List<InstanceConfig> instanceConfigs,
      @Nullable InstancePartitions existingInstancePartitions,
      @Nullable InstancePartitions preConfiguredInstancePartitions, Enablement minimizeDataMovement) {
    String tableNameWithType = tableConfig.getTableName();
    InstanceAssignmentConfig assignmentConfig =
        InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, instancePartitionsType);
    return getInstancePartitions(tableConfig,
        instancePartitionsType.getInstancePartitionsName(TableNameBuilder.extractRawTableName(tableNameWithType)),
        assignmentConfig, instanceConfigs, existingInstancePartitions, preConfiguredInstancePartitions,
        minimizeDataMovement);
  }

  public static InstancePartitions assignInstances(TableConfig tableConfig, String tierName,
      List<InstanceConfig> instanceConfigs, @Nullable InstancePartitions existingInstancePartitions,
      InstanceAssignmentConfig instanceAssignmentConfig) {
    return assignInstances(tableConfig, tierName, instanceConfigs, existingInstancePartitions, instanceAssignmentConfig,
        Enablement.DEFAULT);
  }

  public static InstancePartitions assignInstances(TableConfig tableConfig, String tierName,
      List<InstanceConfig> instanceConfigs, @Nullable InstancePartitions existingInstancePartitions,
      InstanceAssignmentConfig instanceAssignmentConfig, Enablement minimizeDataMovement) {
    return getInstancePartitions(tableConfig,
        InstancePartitionsUtils.getInstancePartitionsNameForTier(tableConfig.getTableName(), tierName),
        instanceAssignmentConfig, instanceConfigs, existingInstancePartitions, null, minimizeDataMovement);
  }

  /// Computes the instance partitions of the given type for the table config without persisting them. The instances
  /// are assigned from the given instance configs, unless the table has pre-configured instance partitions of the
  /// type: those are fetched from the property store, and either returned renamed for the table, or used as the
  /// server pool of the mirror server set selector when that selector is configured.
  public static InstancePartitions computeInstancePartitions(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, HelixPropertyStore<ZNRecord> propertyStore,
      List<InstanceConfig> instanceConfigs, @Nullable InstancePartitions existingInstancePartitions) {
    return computeInstancePartitions(tableConfig, instancePartitionsType, propertyStore, instanceConfigs,
        existingInstancePartitions, Enablement.DEFAULT);
  }

  public static InstancePartitions computeInstancePartitions(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, HelixPropertyStore<ZNRecord> propertyStore,
      List<InstanceConfig> instanceConfigs, @Nullable InstancePartitions existingInstancePartitions,
      Enablement minimizeDataMovement) {
    if (!InstancePartitionsUtils.hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType)) {
      return assignInstances(tableConfig, instancePartitionsType, instanceConfigs, existingInstancePartitions,
          minimizeDataMovement);
    }
    InstancePartitions preConfiguredInstancePartitions =
        InstancePartitionsUtils.fetchInstancePartitionsWithRename(propertyStore,
            tableConfig.getInstancePartitionsMap().get(instancePartitionsType),
            InstancePartitionsUtils.getInstancePartitionsName(tableConfig.getTableName(), instancePartitionsType));
    if (InstanceAssignmentConfigUtils.isMirrorServerSetAssignment(tableConfig, instancePartitionsType)) {
      return assignInstances(tableConfig, instancePartitionsType, instanceConfigs, existingInstancePartitions,
          preConfiguredInstancePartitions, minimizeDataMovement);
    }
    return preConfiguredInstancePartitions;
  }

  private static InstancePartitions getInstancePartitions(TableConfig tableConfig, String instancePartitionsName,
      InstanceAssignmentConfig instanceAssignmentConfig, List<InstanceConfig> instanceConfigs,
      @Nullable InstancePartitions existingInstancePartitions,
      @Nullable InstancePartitions preConfiguredInstancePartitions, Enablement minimizeDataMovementEnablement) {
    String tableNameWithType = tableConfig.getTableName();

    // minimizeDataMovement might be set back to false within InstanceTagPoolSelector and InstancePartitionSelector
    // if existingInstancePartitions is null.
    boolean minimizeDataMovementFromTableConfig = instanceAssignmentConfig.isMinimizeDataMovement();
    boolean minimizeDataMovement = minimizeDataMovementEnablement.isEnabled(minimizeDataMovementFromTableConfig);
    InstancePartitionSelector instancePartitionSelector =
        InstancePartitionSelectorFactory.getInstance(tableConfig, instanceAssignmentConfig.getPartitionSelector(),
            instanceAssignmentConfig.getReplicaGroupPartitionConfig(), tableNameWithType, existingInstancePartitions,
            preConfiguredInstancePartitions, minimizeDataMovement);

    LOGGER.info("Starting {} instance assignment for table: {} with minimizeDataMovement: {} (from table config: {}, "
            + "override: {})", instancePartitionsName, tableNameWithType, minimizeDataMovement,
        minimizeDataMovementFromTableConfig, minimizeDataMovementEnablement);

    return getInstancePartitions(tableConfig, instancePartitionsName, instanceAssignmentConfig, instanceConfigs,
        existingInstancePartitions, minimizeDataMovement, instancePartitionSelector);
  }

  @VisibleForTesting
  static InstancePartitions getInstancePartitions(TableConfig tableConfig, String instancePartitionsName,
      InstanceAssignmentConfig instanceAssignmentConfig, List<InstanceConfig> instanceConfigs,
      @Nullable InstancePartitions existingInstancePartitions, boolean minimizeDataMovement,
      InstancePartitionSelector instancePartitionSelector) {
    String tableNameWithType = tableConfig.getTableName();

    InstanceTagPoolSelector tagPoolSelector =
        new InstanceTagPoolSelector(instanceAssignmentConfig.getTagPoolConfig(), tableNameWithType,
            minimizeDataMovement, existingInstancePartitions);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = tagPoolSelector.selectInstances(instanceConfigs);

    InstanceConstraintConfig constraintConfig = instanceAssignmentConfig.getConstraintConfig();
    List<InstanceConstraintApplier> constraintAppliers = new ArrayList<>();
    if (constraintConfig == null) {
      LOGGER.info("No instance constraint is configured, using default hash-based-rotate instance constraint");
      constraintAppliers.add(new HashBasedRotateInstanceConstraintApplier(tableNameWithType));
    }
    // TODO: support more constraints
    for (InstanceConstraintApplier constraintApplier : constraintAppliers) {
      poolToInstanceConfigsMap = constraintApplier.applyConstraint(poolToInstanceConfigsMap);
    }

    InstancePartitions instancePartitions = new InstancePartitions(instancePartitionsName);
    instancePartitionSelector.selectInstances(poolToInstanceConfigsMap, instancePartitions);
    return instancePartitions;
  }
}
