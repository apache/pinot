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

import javax.annotation.Nullable;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Utility class that encapsulates table config validation logic shared across
 * {@link PinotTableRestletResource} and StarTree's managed logical table resource.
 *
 * <p>This lives in {@code pinot-controller} (not {@code pinot-segment-local}'s {@code TableConfigUtils})
 * because validation requires controller-level dependencies like {@link PinotHelixResourceManager},
 * {@link ControllerConf}, {@link PinotTaskManager}, and {@link TableRebalancer}.</p>
 */
public final class TableConfigValidationUtils {

  private TableConfigValidationUtils() {
  }

  /**
   * Validates a table config against the given schema and controller configuration.
   *
   * <p>Performs the following validations in order:</p>
   * <ol>
   *   <li>Core validation ({@link TableConfigUtils#validate})</li>
   *   <li>Table name validation ({@link TableConfigUtils#validateTableName})</li>
   *   <li>Min replicas enforcement</li>
   *   <li>Storage quota constraints</li>
   *   <li>Hybrid table config check (if both OFFLINE and REALTIME versions exist)</li>
   *   <li>Task config validation (skipped if {@code taskManager} is null)</li>
   *   <li>Instance assignment validation</li>
   * </ol>
   *
   * <p><b>NOT included</b> (caller responsibility):</p>
   * <ul>
   *   <li>Schema retrieval — caller resolves it</li>
   *   <li>Tuner config application — CREATE-only, mutates config</li>
   *   <li>Active tasks check — caller-specific</li>
   * </ul>
   *
   * @param tableConfig   the table config to validate
   * @param schema        the schema for the table (must not be null)
   * @param typesToSkip   comma-separated list of validation types to skip (ALL|TASK|UPSERT), or null
   * @param resourceManager the Helix resource manager
   * @param controllerConf  the controller configuration
   * @param taskManager     the task manager, or null to skip task validation
   */
  public static void validateTableConfig(TableConfig tableConfig, Schema schema,
      @Nullable String typesToSkip, PinotHelixResourceManager resourceManager,
      ControllerConf controllerConf, @Nullable PinotTaskManager taskManager) {
    TableConfigUtils.validate(tableConfig, schema, typesToSkip);
    TableConfigUtils.validateTableName(tableConfig);
    TableConfigUtils.ensureMinReplicas(tableConfig, controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, controllerConf.getDimTableMaxSize());
    checkHybridTableConfig(resourceManager, tableConfig);
    TaskConfigUtils.validateTaskConfigs(tableConfig, schema, taskManager, typesToSkip);
    validateInstanceAssignment(resourceManager, tableConfig);
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
    TableRebalancer tableRebalancer = new TableRebalancer(resourceManager.getHelixZkManager());
    try {
      tableRebalancer.getInstancePartitionsMap(tableConfig, true, true, true);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to calculate instance partitions for table: " + tableConfig.getTableName() + ", reason: "
              + e.getMessage());
    }
  }
}
