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
package org.apache.pinot.controller.helix.core.periodictask;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.migration.ConfigMigrationRegistry;
import org.apache.pinot.spi.config.migration.MigrationResult;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Controller periodic task that transparently upgrades stored [TableConfig]s and [Schema]s to the
/// current migration version, so that a cluster upgrade does not surprise users with configs the new
/// controller can no longer parse or validate.
///
/// For every table this controller leads, the task:
/// 1. Reads the config **exactly as stored** (no env-var substitution, no decorator) together with
///    its ZK version.
/// 2. Runs the [ConfigMigrationRegistry] chain. If nothing changed (already at the current version),
///    it writes nothing.
/// 3. Persists the migrated config through the standard [PinotHelixResourceManager] write path
///    (`setExistingTableConfig` / `updateSchema`), which validates the config, performs a
///    version-checked write, and — crucially — sends the broker/server cache-refresh messages so
///    in-memory caches converge. A concurrent operator edit is never clobbered: the version check
///    fails, the write is skipped, and the table is retried on the next cycle.
/// 4. Migrates the table's schema the same way (deduplicated across a table's OFFLINE/REALTIME halves
///    within a single run, since they share one schema).
///
/// Note: the table-config and schema writes are two independent operations; a migration that must
/// change both atomically cannot be expressed in this model and would need a different mechanism.
///
/// The task is idempotent: once every config is at the current version, subsequent runs are cheap
/// no-ops. Leadership and per-table iteration are handled by [ControllerPeriodicTask].
public class ConfigMigrationManager extends ControllerPeriodicTask<ConfigMigrationManager.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigMigrationManager.class);
  public static final String TASK_NAME = ConfigMigrationManager.class.getSimpleName();

  private final ConfigMigrationRegistry _migrationRegistry;

  public ConfigMigrationManager(PinotHelixResourceManager helixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      ConfigMigrationRegistry migrationRegistry) {
    super(TASK_NAME, config.getConfigMigrationFrequencyInSeconds(), config.getConfigMigrationInitialDelaySeconds(),
        config.getConfigMigrationCronExpression(), helixResourceManager, leadControllerManager, controllerMetrics);
    _migrationRegistry = migrationRegistry;
  }

  /// Per-run context tracking which schemas were already migrated, so a hybrid table's OFFLINE and
  /// REALTIME halves don't both attempt to migrate the shared schema. Tables are processed sequentially
  /// within a single run (see [ControllerPeriodicTask#processTables]), so a plain [HashSet] is sufficient.
  public static class Context {
    private final Set<String> _processedSchemas = new HashSet<>();
  }

  @Override
  protected Context preprocess(Properties periodicTaskProperties) {
    return new Context();
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    boolean hasTableConfigMigrators = _migrationRegistry.getCurrentTableConfigVersion() > 0;
    boolean hasSchemaMigrators = _migrationRegistry.getCurrentSchemaVersion() > 0;
    // Short-circuit entirely when both migration chains are empty (avoids all ZK reads for the common no-op case).
    if (!hasTableConfigMigrators && !hasSchemaMigrators) {
      return;
    }

    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

    // Fetch the schema at most once per table and reuse it for both table-config validation and schema migration.
    // Null is an expected transient state (schema not created yet), not a failure.
    Schema schema = _pinotHelixResourceManager.getSchema(rawTableName);

    if (hasTableConfigMigrators) {
      migrateTableConfig(propertyStore, tableNameWithType, schema);
    }
    // Schema is shared between the OFFLINE and REALTIME halves of a table; migrate it at most once per run.
    if (hasSchemaMigrators && schema != null && context._processedSchemas.add(rawTableName)) {
      migrateSchema(propertyStore, schema);
    }
  }

  private void migrateTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      @Nullable Schema schema) {
    // Read the config as stored (no variable substitution, no decorator) together with its ZK version, so we never
    // persist resolved env-var values and can perform a version-checked write.
    ImmutablePair<TableConfig, Integer> configAndVersion =
        ZKMetadataProvider.getTableConfigWithVersion(propertyStore, tableNameWithType, false, false);
    if (configAndVersion == null) {
      return;
    }
    TableConfig tableConfig = configAndVersion.getLeft();
    int expectedVersion = configAndVersion.getRight();

    MigrationResult<TableConfig> result = _migrationRegistry.migrateTableConfig(tableConfig);
    if (!result.isChanged()) {
      return;
    }
    TableConfig migrated = result.getConfig();

    // Validate before persisting so a buggy migrator can never write an invalid config into ZK. A missing schema is
    // an expected transient state (e.g. schema not yet created), not a migration failure — skip quietly and retry.
    if (schema == null) {
      LOGGER.info("Skipping table config migration for table: {}; schema not found yet, will retry", tableNameWithType);
      return;
    }
    try {
      TableConfigUtils.validate(migrated, schema);
    } catch (Exception e) {
      LOGGER.error("Migrated table config for table: {} failed validation; skipping persist", tableNameWithType, e);
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
      return;
    }

    // Persist through the standard resource-manager path: version-checked write + broker/server cache-refresh messages.
    try {
      _pinotHelixResourceManager.setExistingTableConfig(migrated, expectedVersion, true);
      LOGGER.info("Migrated table config for table: {} to version: {}", tableNameWithType, result.getVersion());
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
    } catch (Exception e) {
      // Most likely lost the optimistic-concurrency race with a concurrent update; will retry on the next cycle.
      LOGGER.warn("Failed to persist migrated table config for table: {} (expected version: {}); will retry",
          tableNameWithType, expectedVersion, e);
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
    }
  }

  private void migrateSchema(ZkHelixPropertyStore<ZNRecord> propertyStore, Schema schema) {
    String schemaName = schema.getSchemaName();
    MigrationResult<Schema> result = _migrationRegistry.migrateSchema(schema);
    if (!result.isChanged()) {
      return;
    }
    Schema migrated = result.getConfig();

    // NOTE: We persist directly via ZKMetadataProvider rather than PinotHelixResourceManager.updateSchema because
    // updateSchema short-circuits when schema.equals(oldSchema) is true, and Schema.equals() intentionally excludes the
    // migration-version marker. A migration that only bumps the marker would therefore never persist and would re-run
    // every cycle. The direct write always persists the marker; we then send the schema-refresh message so
    // broker/server caches converge. Segments are not reloaded: a version-marker bump changes no queryable field.
    try {
      migrated.validate();
      ZKMetadataProvider.setSchema(propertyStore, migrated);
      LOGGER.info("Migrated schema: {} to version: {}", schemaName, result.getVersion());
      _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
    } catch (Exception e) {
      LOGGER.warn("Failed to persist migrated schema: {}; will retry", schemaName, e);
      _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
      return;
    }

    // Best-effort cache refresh for tables using this schema; a refresh failure does not undo the successful write
    // (the persisted marker prevents re-migration next cycle).
    try {
      for (String tableNameWithType : _pinotHelixResourceManager.getExistingTableNamesWithType(schemaName, null)) {
        _pinotHelixResourceManager.sendTableConfigSchemaRefreshMessage(tableNameWithType);
      }
    } catch (Exception e) {
      LOGGER.warn("Migrated schema: {} persisted, but failed to send refresh messages; caches will converge on the "
          + "next schema update or controller restart", schemaName, e);
    }
  }
}
