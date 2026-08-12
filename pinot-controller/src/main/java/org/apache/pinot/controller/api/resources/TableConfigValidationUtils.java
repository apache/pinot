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
package org.apache.pinot.controller.api.resources;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentUtils;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils.ValidationType;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utility class that encapsulates table config validation logic.
///
/// This lives in `pinot-controller` (not `pinot-segment-local`'s `TableConfigUtils`)
/// because validation requires controller-level dependencies like [PinotHelixResourceManager],
/// [ControllerConf], [PinotTaskManager], and [InstanceAssignmentUtils].
public final class TableConfigValidationUtils {

  private TableConfigValidationUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigValidationUtils.class);

  /// Description of the `validationTypesToSkip` request parameter parsed by [#parseValidationTypesToSkip].
  public static final String VALIDATION_TYPES_TO_SKIP_DESCRIPTION =
      "Comma separated list of validation type(s) to skip. Supported types: "
          + "(ALL|TASK|UPSERT|DEDUP|INSTANCE_ASSIGNMENT|TENANT|MINION_INSTANCES). TENANT and MINION_INSTANCES apply "
          + "only to validating and tuning table configs, as creating or updating a table always validates them";

  /// Parses the `validationTypesToSkip` request parameter, rejecting an unknown validation type as a bad request.
  /// Returns `null` when the parameter is not set or blank.
  @Nullable
  public static EnumSet<ValidationType> parseValidationTypesToSkip(@Nullable String typesToSkip) {
    try {
      return TableConfigUtils.parseTypesToSkipString(typesToSkip);
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid validationTypesToSkip: %s, supported types: %s", typesToSkip,
              Arrays.toString(ValidationType.values())), Response.Status.BAD_REQUEST, e);
    }
  }

  /// Validates a table config against the given schema and controller configuration.
  ///
  /// @param tableConfig   the table config to validate
  /// @param schema        the schema for the table (must not be null)
  /// @param skipTypes     the validation types to skip, or null to skip none, see [ValidationType]
  /// @param resourceManager the Helix resource manager
  /// @param controllerConf  the controller configuration
  /// @param taskManager     the task manager, or null to skip task validation
  public static void validateTableConfig(TableConfig tableConfig, Schema schema,
      @Nullable EnumSet<ValidationType> skipTypes, PinotHelixResourceManager resourceManager,
      ControllerConf controllerConf, @Nullable PinotTaskManager taskManager) {
    validateTableConfig(tableConfig, schema, skipTypes, resourceManager, controllerConf, taskManager, null);
  }

  public static void validateTableConfig(TableConfig tableConfig, Schema schema,
      @Nullable EnumSet<ValidationType> skipTypes, PinotHelixResourceManager resourceManager,
      ControllerConf controllerConf, @Nullable PinotTaskManager taskManager,
      @Nullable TableConfig existingTableConfig) {
    validateTableConfig(tableConfig, schema, skipTypes, resourceManager, controllerConf, taskManager,
        existingTableConfig, true);
  }

  static void validateTableConfig(TableConfig tableConfig, Schema schema,
      @Nullable EnumSet<ValidationType> skipTypes, PinotHelixResourceManager resourceManager,
      ControllerConf controllerConf, @Nullable PinotTaskManager taskManager,
      @Nullable TableConfig existingTableConfig, boolean checkStoredHybridConfig) {
    validateEnvironmentVariables(tableConfig);
    TableConfigUtils.validate(tableConfig, schema, skipTypes, existingTableConfig);
    TableConfigUtils.validateTableName(tableConfig);
    TableConfigUtils.ensureMinReplicas(tableConfig, controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, controllerConf.getDimTableMaxSize());
    if (checkStoredHybridConfig) {
      checkHybridTableConfig(resourceManager, tableConfig);
    }
    TaskConfigUtils.validateTaskConfigs(tableConfig, schema, taskManager, skipTypes);
    if (!ValidationType.INSTANCE_ASSIGNMENT.isSkipped(skipTypes)) {
      validateInstanceAssignment(resourceManager, tableConfig);
    }
    resourceManager.validateTableTenantConfig(tableConfig);
    resourceManager.validateTableTaskMinionInstanceTagConfig(tableConfig);
    TableConfigValidatorRegistry.validate(tableConfig, schema);
  }

  /// Validates that every environment variable / system property referenced by the table config (via the
  /// `${VAR}` template syntax, without a default value) can be resolved at the time the config is written.
  ///
  /// Table configs are stored as templates and resolved lazily every time they are read. If a referenced variable
  /// does not exist, the failure surfaces only at read time and can break operations (e.g. segment commit)
  /// Resolving here makes the write fail fast so the bad config is never persisted.
  @VisibleForTesting
  static void validateEnvironmentVariables(TableConfig tableConfig) {
    try {
      // Returns a resolved copy without mutating the original config; we only care about whether it throws.
      ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(tableConfig);
    } catch (RuntimeException e) {
      // ConfigUtils wraps the underlying "Missing environment Variable: <name>" message in its cause, so surface
      // the root cause to make the offending variable visible in the rejection message.
      Throwable rootCause = e;
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      String reason = rootCause.getMessage() != null ? rootCause.getMessage() : rootCause.toString();
      throw new IllegalStateException(
          "Failed to resolve environment variables/system properties referenced in table " + "config for table: "
              + tableConfig.getTableName() + ", reason: " + reason, e);
    }
  }

  private static void checkHybridTableConfig(PinotHelixResourceManager resourceManager, TableConfig tableConfig) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    if (tableConfig.getTableType() == TableType.REALTIME) {
      if (resourceManager.hasOfflineTable(rawTableName)) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, resourceManager.getOfflineTableConfig(rawTableName),
            tableConfig);
      }
    } else {
      if (resourceManager.hasRealtimeTable(rawTableName)) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, tableConfig,
            resourceManager.getRealtimeTableConfig(rawTableName));
      }
    }
  }

  /// Computes the instance partitions the table would get on creation, so that an instance assignment that cannot be
  /// satisfied by the current instances is rejected before the table config is persisted, and validates them: the
  /// OFFLINE segments must be placed by partition when the OFFLINE instance partitions have multiple partitions, and
  /// the instance partitions must fit strict replica-group routing. This covers pre-configured instance partitions,
  /// whose layout is not visible in the table config.
  private static void validateInstanceAssignment(PinotHelixResourceManager resourceManager, TableConfig tableConfig) {
    List<InstancePartitionsType> instancePartitionsTypes;
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      instancePartitionsTypes = List.of(InstancePartitionsType.OFFLINE);
    } else if (InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
      instancePartitionsTypes = List.of(InstancePartitionsType.CONSUMING, InstancePartitionsType.COMPLETED);
    } else {
      instancePartitionsTypes = List.of(InstancePartitionsType.CONSUMING);
    }
    List<InstanceConfig> instanceConfigs = resourceManager.getAllHelixInstanceConfigs();
    InstancePartitions offlineInstancePartitions = null;
    InstancePartitions consumingInstancePartitions = null;
    try {
      for (InstancePartitionsType instancePartitionsType : instancePartitionsTypes) {
        InstancePartitions instancePartitions;
        if (InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, instancePartitionsType)) {
          instancePartitions = InstanceAssignmentUtils.computeInstancePartitions(tableConfig, instancePartitionsType,
              resourceManager.getPropertyStore(), instanceConfigs, null);
        } else {
          instancePartitions = InstancePartitionsUtils.computeDefaultInstancePartitions(
              resourceManager.getHelixZkManager(), tableConfig, instancePartitionsType);
        }
        if (instancePartitionsType == InstancePartitionsType.OFFLINE) {
          offlineInstancePartitions = instancePartitions;
        } else if (instancePartitionsType == InstancePartitionsType.CONSUMING) {
          consumingInstancePartitions = instancePartitions;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to calculate instance partitions for table: " + tableConfig.getTableName() + ", reason: "
              + e.getMessage(), e);
    }
    if (offlineInstancePartitions != null && offlineInstancePartitions.getNumPartitions() > 1) {
      TableConfigUtils.validatePartitionedSegmentAssignment(tableConfig, InstancePartitionsType.OFFLINE.name());
    }
    validateStrictReplicaGroupInstancePartitions(tableConfig, offlineInstancePartitions, consumingInstancePartitions);
  }

  /// Validates the computed or fetched instance partitions of a table using strict replica-group routing, which is the
  /// only place where pre-configured instance partitions can be checked; the strict replica-group routing check in
  /// [TableConfigUtils#validate] covers the instance assignment config. The OFFLINE instance partitions must place each
  /// partition on exactly one instance per replica-group. CONSUMING segments are placed by their stream partition, so
  /// the CONSUMING instance partitions need the same layout only when they have multiple partitions. COMPLETED segments
  /// are never relocated for such tables.
  @VisibleForTesting
  static void validateStrictReplicaGroupInstancePartitions(TableConfig tableConfig,
      @Nullable InstancePartitions offlineInstancePartitions,
      @Nullable InstancePartitions consumingInstancePartitions) {
    if (!TableConfigUtils.requiresStrictReplicaGroupAssignment(tableConfig)) {
      return;
    }
    if (offlineInstancePartitions != null) {
      validateOneInstancePerPartition(tableConfig, offlineInstancePartitions);
    }
    if (consumingInstancePartitions != null && consumingInstancePartitions.getNumPartitions() > 1) {
      validateOneInstancePerPartition(tableConfig, consumingInstancePartitions);
    }
  }

  private static void validateOneInstancePerPartition(TableConfig tableConfig, InstancePartitions instancePartitions) {
    for (Map.Entry<String, List<String>> entry : instancePartitions.getPartitionToInstancesMap().entrySet()) {
      Preconditions.checkState(entry.getValue().size() == 1,
          "Table: %s using strict replica-group routing must have exactly one instance per partition in each "
              + "replica-group, found instances: %s for partition_replicaGroup: %s in instance partitions: %s",
          tableConfig.getTableName(), entry.getValue(), entry.getKey(), instancePartitions.getInstancePartitionsName());
    }
  }
}
