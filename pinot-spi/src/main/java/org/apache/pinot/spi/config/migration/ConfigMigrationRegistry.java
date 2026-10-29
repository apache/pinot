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
package org.apache.pinot.spi.config.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


/// Registry of ordered [ConfigMigrator]s that upgrade stored [TableConfig] and [Schema] objects to
/// the current migration version.
///
/// Migrators form a dense chain: the migrator at index {@code i} declares {@code fromVersion() == i}
/// and produces version {@code i + 1}. The current version is therefore the number of registered
/// migrators. [#migrateTableConfig]/[#migrateSchema] apply every idempotent migrator in order for supported
/// configs, including already-marked configs: a later user update may have reintroduced a legacy property.
/// A future-version marker prevents migration after a downgrade. [MigrationResult#isChanged()] reports an
/// actual content change, rather than the presence or absence of a marker.
///
/// Migrators are registered once during startup (before any migration runs) and the registry is then
/// read-only, so migration itself is thread-safe across tables. Registration is guarded to enforce
/// the dense, in-order contract.
///
/// The default no-migrator registry (current version 0) is a safe identity: every config is already
/// current and nothing is rewritten.
public class ConfigMigrationRegistry {
  private final List<TableConfigMigrator> _tableConfigMigrators = new ArrayList<>();
  private final List<SchemaMigrator> _schemaMigrators = new ArrayList<>();

  /// Registers the next table-config migrator. Its [ConfigMigrator#fromVersion()] must equal the
  /// current number of registered table-config migrators, keeping the chain dense and in order.
  public synchronized void registerTableConfigMigrator(TableConfigMigrator migrator) {
    Preconditions.checkArgument(migrator.fromVersion() == _tableConfigMigrators.size(),
        "TableConfig migrator fromVersion %s does not match expected version %s (migrators must be registered in "
            + "dense, ascending order)", migrator.fromVersion(), _tableConfigMigrators.size());
    _tableConfigMigrators.add(migrator);
  }

  /// Registers the next schema migrator. Its [ConfigMigrator#fromVersion()] must equal the current
  /// number of registered schema migrators, keeping the chain dense and in order.
  public synchronized void registerSchemaMigrator(SchemaMigrator migrator) {
    Preconditions.checkArgument(migrator.fromVersion() == _schemaMigrators.size(),
        "Schema migrator fromVersion %s does not match expected version %s (migrators must be registered in dense, "
            + "ascending order)", migrator.fromVersion(), _schemaMigrators.size());
    _schemaMigrators.add(migrator);
  }

  /// The current table-config migration version, i.e. the number of registered table-config migrators.
  public int getCurrentTableConfigVersion() {
    return _tableConfigMigrators.size();
  }

  /// The current schema migration version, i.e. the number of registered schema migrators.
  public int getCurrentSchemaVersion() {
    return _schemaMigrators.size();
  }

  /// Migrates the given table config from its stored version up to [#getCurrentTableConfigVersion()].
  /// Only a substantive transformation is reported as changed and stamped with the target version.
  /// A modern config without a marker is left untouched, rather than recommended for a bookkeeping-only update.
  public MigrationResult<TableConfig> migrateTableConfig(TableConfig tableConfig) {
    int currentVersion = ConfigMigrationUtils.getTableConfigVersion(tableConfig);
    int targetVersion = getCurrentTableConfigVersion();
    if (targetVersion == 0 || currentVersion > targetVersion) {
      return new MigrationResult<>(tableConfig, targetVersion, false);
    }
    JsonNode original = JsonUtils.objectToJsonNode(tableConfig);
    TableConfig migrated = migrate(copy(original, TableConfig.class), _tableConfigMigrators);
    if (original.equals(JsonUtils.objectToJsonNode(migrated))) {
      return new MigrationResult<>(tableConfig, targetVersion, false);
    }
    ConfigMigrationUtils.setTableConfigVersion(migrated, targetVersion);
    return new MigrationResult<>(migrated, targetVersion, true);
  }

  /// Migrates the given schema from its stored version up to [#getCurrentSchemaVersion()]. The
  /// returned schema carries the stamped version marker only when its content changed. Migrators operate
  /// on a deep copy, so recommendations cannot mutate a cached schema.
  public MigrationResult<Schema> migrateSchema(Schema schema) {
    int currentVersion = ConfigMigrationUtils.getSchemaVersion(schema);
    int targetVersion = getCurrentSchemaVersion();
    if (targetVersion == 0 || currentVersion > targetVersion) {
      return new MigrationResult<>(schema, targetVersion, false);
    }
    JsonNode original = JsonUtils.objectToJsonNode(schema);
    Schema migrated = migrate(copySchema(schema), _schemaMigrators);
    if (original.equals(JsonUtils.objectToJsonNode(migrated))) {
      return new MigrationResult<>(schema, targetVersion, false);
    }
    ConfigMigrationUtils.setSchemaVersion(migrated, targetVersion);
    return new MigrationResult<>(migrated, targetVersion, true);
  }

  // Legacy TIME serialization omits common FieldSpec attributes. Copy the full field before running a
  // compatibility migrator, so attributes such as nullability and column identity are not lost by the copy itself.
  @SuppressWarnings("deprecation")
  private static Schema copySchema(Schema schema) {
    ObjectNode json = schema.toJsonObject();
    if (schema.getTimeFieldSpec() != null) {
      ObjectNode timeField = schema.getTimeFieldSpec().toJsonObject();
      timeField.setAll((ObjectNode) JsonUtils.objectToJsonNode(schema.getTimeFieldSpec()));
      json.set("timeFieldSpec", timeField);
    }
    return copy(json, Schema.class);
  }

  private static <T> T copy(JsonNode json, Class<T> type) {
    try {
      return JsonUtils.jsonNodeToObject(json, type);
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot copy config for migration", e);
    }
  }

  /// Reruns the ordered, idempotent chain so legacy properties reintroduced by later edits are still detected.
  private static <T, M extends ConfigMigrator<T>> T migrate(T config, List<M> migrators) {
    T result = config;
    for (M migrator : migrators) {
      result = migrator.migrate(result);
    }
    return result;
  }
}
