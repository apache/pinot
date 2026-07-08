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
import javax.annotation.Nullable;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Utility class that encapsulates table config validation logic.
 *
 * <p>This lives in {@code pinot-controller} (not {@code pinot-segment-local}'s {@code TableConfigUtils})
 * because validation requires controller-level dependencies like {@link PinotHelixResourceManager},
 * {@link ControllerConf}, {@link PinotTaskManager}, and {@link TableRebalancer}.</p>
 */
public final class TableConfigValidationUtils {

  private TableConfigValidationUtils() {
  }

  public static void validateTableConfig(TableConfig tableConfig, Schema schema,
      @Nullable String typesToSkip, PinotHelixResourceManager resourceManager,
      ControllerConf controllerConf, @Nullable PinotTaskManager taskManager,
      @Nullable TableConfig existingTableConfig) {
    validateEnvironmentVariables(tableConfig);
    TableConfigUtils.validate(tableConfig, schema, typesToSkip, existingTableConfig);
    TableConfigUtils.validateTableName(tableConfig);
    TableConfigUtils.ensureMinReplicas(tableConfig, controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, controllerConf.getDimTableMaxSize());
    checkHybridTableConfig(resourceManager, tableConfig);
    TaskConfigUtils.validateTaskConfigs(tableConfig, schema, taskManager, typesToSkip);
    validateInstanceAssignment(resourceManager, tableConfig);
    resourceManager.validateTableTenantConfig(tableConfig);
    resourceManager.validateTableTaskMinionInstanceTagConfig(tableConfig);
    TableConfigValidatorRegistry.validate(tableConfig, schema);
  }

  /**
   * Validates that every environment variable / system property referenced by the table config (via the
   * {@code ${VAR}} template syntax, without a default value) can be resolved at the time the config is written.
   *
   * Table configs are stored as templates and resolved lazily every time they are read. If a referenced variable
   * does not exist, the failure surfaces only at read time and can break operations (e.g. segment commit)
   * Resolving here makes the write fail fast so the bad config is never persisted.
   */
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
      throw new IllegalStateException("Failed to resolve environment variables/system properties referenced in table "
          + "config for table: " + tableConfig.getTableName() + ", reason: " + reason, e);
    }
  }

  private static void checkHybridTableConfig(PinotHelixResourceManager resourceManager, TableConfig tableConfig) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    if (tableConfig.getTableType() == TableType.REALTIME) {
      if (resourceManager.hasOfflineTable(rawTableName)) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName,
            resourceManager.getOfflineTableConfig(rawTableName), tableConfig);
      }
    } else {
      if (resourceManager.hasRealtimeTable(rawTableName)) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, tableConfig,
            resourceManager.getRealtimeTableConfig(rawTableName));
      }
    }
  }

  private static void validateInstanceAssignment(PinotHelixResourceManager resourceManager,
      TableConfig tableConfig) {
    TableRebalancer tableRebalancer = new TableRebalancer(resourceManager.getHelixZkManager(),
        resourceManager.getQueryWorkloadManager());
    try {
      tableRebalancer.getInstancePartitionsMap(tableConfig, true, true, true);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to calculate instance partitions for table: " + tableConfig.getTableName() + ", reason: "
              + e.getMessage(), e);
    }
  }
}
