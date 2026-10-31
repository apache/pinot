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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.migration.ConfigMigrationRegistry;
import org.apache.pinot.spi.config.migration.ConfigMigrationUtils;
import org.apache.pinot.spi.config.migration.MigrationResult;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TableConfigDecoratorRegistry;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Inspects stored table definitions and recommends available schema and table-config migrations.
/// By default only warnings and recommendation meters are emitted. Explicitly enabling
/// the cluster config `controller.config.migration.auto.update.enabled` also persists validated candidates with the
/// captured ZooKeeper version and refreshes caches through the resource manager.
///
/// Schema transforms move to all affected table configs before being removed, using one version-checked transaction.
/// Raw templates are retained for storage; a separate resolved table config is used for validation and
/// runtime side effects. Table leadership and sequential per-run processing are provided by [ControllerPeriodicTask].
public class ConfigMigrationManager extends ControllerPeriodicTask<ConfigMigrationManager.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigMigrationManager.class);
  public static final String TASK_NAME = ConfigMigrationManager.class.getSimpleName();

  private final ConfigMigrationRegistry _migrationRegistry;
  private volatile boolean _autoUpdateEnabled =
      ControllerPeriodicTasksConf.DEFAULT_CONFIG_MIGRATION_AUTO_UPDATE_ENABLED;

  public ConfigMigrationManager(PinotHelixResourceManager helixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      ConfigMigrationRegistry migrationRegistry) {
    super(TASK_NAME, config.getConfigMigrationFrequencyInSeconds(), config.getConfigMigrationInitialDelaySeconds(),
        config.getConfigMigrationCronExpression(), helixResourceManager, leadControllerManager, controllerMetrics);
    _migrationRegistry = migrationRegistry;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (changedConfigs.contains(ControllerPeriodicTasksConf.CONFIG_MIGRATION_AUTO_UPDATE_ENABLED)) {
      _autoUpdateEnabled = Boolean.parseBoolean(clusterConfigs.getOrDefault(
          ControllerPeriodicTasksConf.CONFIG_MIGRATION_AUTO_UPDATE_ENABLED,
          Boolean.toString(ControllerPeriodicTasksConf.DEFAULT_CONFIG_MIGRATION_AUTO_UPDATE_ENABLED)));
    }
  }

  /// Tracks shared schemas so a hybrid table's OFFLINE/REALTIME halves report and update it once per run.
  public static class Context {
    private final Set<String> _processedSchemas = new HashSet<>();
    private final Set<String> _processedTables = new HashSet<>();
  }

  @Override
  protected Context preprocess(Properties periodicTaskProperties) {
    return new Context();
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    if (_migrationRegistry.getCurrentTableConfigVersion() == 0 && _migrationRegistry.getCurrentSchemaVersion() == 0) {
      return;
    }
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    String schemaName = TableNameBuilder.extractRawTableName(tableNameWithType);
    ImmutablePair<Schema, Integer> schemaAndVersion =
        ZKMetadataProvider.getSchemaWithVersion(propertyStore, schemaName);
    Schema schema = schemaAndVersion != null ? schemaAndVersion.getLeft() : null;
    if (_migrationRegistry.getCurrentSchemaVersion() > 0 && schemaAndVersion != null
        && context._processedSchemas.add(schemaName)) {
      schema = migrateSchema(schema, schemaAndVersion.getRight(), context);
    }
    if (_migrationRegistry.getCurrentTableConfigVersion() > 0 && context._processedTables.add(tableNameWithType)) {
      migrateTableConfig(propertyStore, tableNameWithType, schema);
    }
  }

  private void migrateTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      @Nullable Schema schema) {
    try {
      ImmutablePair<TableConfig, Integer> configAndVersion =
          ZKMetadataProvider.getTableConfigWithVersion(propertyStore, tableNameWithType, false, false);
      if (configAndVersion == null) {
        return;
      }
      TableConfig original = configAndVersion.getLeft();
      MigrationResult<TableConfig> result = _migrationRegistry.migrateTableConfig(original);
      if (!result.isChanged()) {
        return;
      }
      LOGGER.warn("Table: {} has legacy configuration; recommend updating to config migration version: {}. "
          + "Automatic updates enabled: {}", tableNameWithType, result.getVersion(), _autoUpdateEnabled);
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.CONFIG_MIGRATION_RECOMMENDED, 1L);
      if (schema == null) {
        return;
      }
      TableConfig migrated = result.getConfig();
      TableConfig effective = TableConfigDecoratorRegistry.applyDecorator(
          ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(migrated));
      TableConfig existingEffective = TableConfigDecoratorRegistry.applyDecorator(
          ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(original));
      TableConfigUtils.validate(effective, schema, null, existingEffective);
      TableConfigValidatorRegistry.validate(effective, schema);
      if (!_autoUpdateEnabled) {
        return;
      }
      _pinotHelixResourceManager.setExistingTableConfig(migrated, effective, configAndVersion.getRight(), false);
      LOGGER.info("Updated table config for table: {} to migration version: {}", tableNameWithType,
          result.getVersion());
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
    } catch (Exception e) {
      // Exception messages can contain resolved config values; report only the type, never the config payload.
      LOGGER.warn("Could not complete table config migration for table: {}; will recheck next scan ({})",
          tableNameWithType, e.getClass().getSimpleName());
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
    }
  }

  private Schema migrateSchema(Schema original, int expectedVersion, Context context) {
    String schemaName = original.getSchemaName();
    try {
      if (ConfigMigrationUtils.getSchemaVersion(original) > _migrationRegistry.getCurrentSchemaVersion()) {
        return original;
      }
      MigrationResult<Schema> result = _migrationRegistry.migrateSchema(original);
      Schema migrated = result.getConfig();
      Map<String, String> transforms = getSchemaTransforms(migrated);
      if (!result.isChanged() && transforms.isEmpty()) {
        return original;
      }
      LOGGER.warn("Schema: {} has legacy configuration; recommend updating to schema migration version: {}. "
          + "Automatic updates enabled: {}", schemaName, result.getVersion(), _autoUpdateEnabled);
      _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.SCHEMA_MIGRATION_RECOMMENDED, 1L);
      if (!transforms.isEmpty()) {
        return migrateSchemaTransforms(original, migrated, expectedVersion, transforms, context);
      }
      List<TableConfig> tableConfigs = new ArrayList<>();
      for (String tableName : _pinotHelixResourceManager.getExistingTableNamesWithType(schemaName, null)) {
        TableConfig config = _pinotHelixResourceManager.getTableConfig(tableName);
        if (config != null) {
          tableConfigs.add(config);
        }
      }
      SchemaUtils.validate(migrated, tableConfigs, false, original);
      if (!_autoUpdateEnabled) {
        return original;
      }
      if (!_pinotHelixResourceManager.updateSchema(migrated, expectedVersion)) {
        _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
        return original;
      }
      LOGGER.info("Updated schema: {} to migration version: {}", schemaName, result.getVersion());
      _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
      return migrated;
    } catch (Exception e) {
      LOGGER.warn("Could not complete schema migration: {}; will recheck next scan ({})",
          schemaName, e.getClass().getSimpleName());
      _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
      return original;
    }
  }

  // Read legacy schema transforms deliberately; empty and virtual transforms have different runtime semantics.
  @SuppressWarnings("deprecation")
  private static Map<String, String> getSchemaTransforms(Schema schema) {
    Map<String, String> transforms = new LinkedHashMap<>();
    for (FieldSpec field : schema.getAllFieldSpecs()) {
      String transform = field.getTransformFunction();
      if (!field.isVirtualColumn() && transform != null && !transform.isEmpty()) {
        transforms.put(field.getName(), transform);
      }
    }
    return transforms;
  }

  // Clear deprecated schema properties only in a copy whose table replacements are committed atomically.
  @SuppressWarnings("deprecation")
  private Schema migrateSchemaTransforms(Schema original, Schema candidate, int expectedVersion,
      Map<String, String> transforms, Context context)
      throws Exception {
    String schemaName = original.getSchemaName();
    Schema migrated = Schema.fromString(candidate.toSingleLineJsonString());
    for (String column : transforms.keySet()) {
      migrated.getFieldSpecFor(column).setTransformFunction(null);
    }
    ConfigMigrationUtils.setSchemaVersion(migrated, _migrationRegistry.getCurrentSchemaVersion());
    List<String> tables = _pinotHelixResourceManager.getExistingTableNamesWithType(schemaName, null);
    context._processedTables.addAll(tables);
    Map<String, Pair<TableConfig, Integer>> tableConfigs = new LinkedHashMap<>();
    List<TableConfig> effectiveConfigs = new ArrayList<>();
    Set<String> changedTables = new HashSet<>();
    for (String tableName : tables) {
      Pair<TableConfig, Integer> stored = ZKMetadataProvider.getTableConfigWithVersion(
          _pinotHelixResourceManager.getPropertyStore(), tableName, false, false);
      if (stored == null || ConfigMigrationUtils.getTableConfigVersion(stored.getLeft())
          > _migrationRegistry.getCurrentTableConfigVersion()) {
        throw new IllegalStateException("Table config unavailable for schema transform migration");
      }
      TableConfig existing = stored.getLeft();
      TableConfig updated = new TableConfig(_migrationRegistry.migrateTableConfig(existing).getConfig());
      addSchemaTransforms(updated, transforms);
      boolean changed = !JsonUtils.objectToJsonNode(existing).equals(JsonUtils.objectToJsonNode(updated));
      if (changed) {
        ConfigMigrationUtils.setTableConfigVersion(updated, _migrationRegistry.getCurrentTableConfigVersion());
        changedTables.add(tableName);
        _controllerMetrics.addMeteredTableValue(tableName, ControllerMeter.CONFIG_MIGRATION_RECOMMENDED, 1L);
      }
      TableConfig existingEffective = TableConfigDecoratorRegistry.applyDecorator(
          ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(existing));
      TableConfig effective = TableConfigDecoratorRegistry.applyDecorator(
          ConfigUtils.applyConfigWithEnvVariablesAndSystemProperties(updated));
      TableConfig expectedEffective =
          new TableConfig(_migrationRegistry.migrateTableConfig(existingEffective).getConfig());
      addSchemaTransforms(expectedEffective, transforms);
      if (changed) {
        ConfigMigrationUtils.setTableConfigVersion(expectedEffective,
            _migrationRegistry.getCurrentTableConfigVersion());
      }
      if (!JsonUtils.objectToJsonNode(effective).equals(JsonUtils.objectToJsonNode(expectedEffective))) {
        throw new IllegalStateException("Moving schema transforms would change the effective table config");
      }
      TableConfigUtils.validate(effective, migrated, null, existingEffective);
      TableConfigValidatorRegistry.validate(effective, migrated);
      effectiveConfigs.add(effective);
      tableConfigs.put(tableName, Pair.of(updated, stored.getRight()));
    }
    SchemaUtils.validate(migrated, effectiveConfigs, false, original);
    if (!_autoUpdateEnabled) {
      return original;
    }
    if (!new HashSet<>(_pinotHelixResourceManager.getExistingTableNamesWithType(schemaName, null))
        .equals(tableConfigs.keySet())
        || !_pinotHelixResourceManager.updateSchemaAndTableConfigs(migrated, expectedVersion, tableConfigs)) {
      _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
      return original;
    }
    for (String table : changedTables) {
      _controllerMetrics.addMeteredTableValue(table, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
    }
    _controllerMetrics.addMeteredTableValue(schemaName, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
    LOGGER.info("Moved schema transforms to table configs for schema: {}", schemaName);
    return migrated;
  }

  private static void addSchemaTransforms(TableConfig config, Map<String, String> transforms) {
    Map<String, String> existing = getTableTransforms(config);
    List<TransformConfig> updated = new ArrayList<>();
    IngestionConfig ingestion = config.getIngestionConfig();
    if (ingestion != null && ingestion.getTransformConfigs() != null) {
      updated.addAll(ingestion.getTransformConfigs());
    }
    for (Map.Entry<String, String> transform : transforms.entrySet()) {
      if (!existing.containsKey(transform.getKey())) {
        updated.add(new TransformConfig(transform.getKey(), transform.getValue()));
      }
    }
    if (updated.size() != existing.size()) {
      if (ingestion == null) {
        ingestion = new IngestionConfig();
        config.setIngestionConfig(ingestion);
      }
      ingestion.setTransformConfigs(updated);
    }
  }

  private static Map<String, String> getTableTransforms(TableConfig config) {
    Map<String, String> transforms = new LinkedHashMap<>();
    IngestionConfig ingestion = config.getIngestionConfig();
    if (ingestion != null && ingestion.getTransformConfigs() != null) {
      for (TransformConfig transform : ingestion.getTransformConfigs()) {
        transforms.put(transform.getColumnName(), transform.getTransformFunction());
      }
    }
    return transforms;
  }
}
