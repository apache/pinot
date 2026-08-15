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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/// Registry of ordered [ConfigMigrator]s that upgrade stored [TableConfig] and [Schema] objects to
/// the current migration version.
///
/// Migrators form a dense chain: the migrator at index {@code i} declares {@code fromVersion() == i}
/// and produces version {@code i + 1}. The current version is therefore the number of registered
/// migrators. Given a config at version {@code v}, [#migrateTableConfig]/[#migrateSchema] apply every
/// migrator with {@code fromVersion() >= v} in order, returning a [MigrationResult] whose
/// {@code changed} flag tells the caller whether a write-back is needed.
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
  /// The returned config carries the stamped version marker. If no migrator applied, the result wraps
  /// the original config unchanged.
  public MigrationResult<TableConfig> migrateTableConfig(TableConfig tableConfig) {
    int currentVersion = ConfigMigrationUtils.getTableConfigVersion(tableConfig);
    TableConfig migrated = migrate(tableConfig, currentVersion, _tableConfigMigrators);
    int targetVersion = getCurrentTableConfigVersion();
    boolean changed = currentVersion < targetVersion;
    if (changed) {
      ConfigMigrationUtils.setTableConfigVersion(migrated, targetVersion);
    }
    return new MigrationResult<>(migrated, targetVersion, changed);
  }

  /// Migrates the given schema from its stored version up to [#getCurrentSchemaVersion()]. The
  /// returned schema carries the stamped version marker. If no migrator applied, the result wraps the
  /// original schema unchanged.
  public MigrationResult<Schema> migrateSchema(Schema schema) {
    int currentVersion = ConfigMigrationUtils.getSchemaVersion(schema);
    Schema migrated = migrate(schema, currentVersion, _schemaMigrators);
    int targetVersion = getCurrentSchemaVersion();
    boolean changed = currentVersion < targetVersion;
    if (changed) {
      ConfigMigrationUtils.setSchemaVersion(migrated, targetVersion);
    }
    return new MigrationResult<>(migrated, targetVersion, changed);
  }

  /// Applies every migrator whose {@code fromVersion() >= currentVersion} in ascending order. The
  /// migrator list is already dense and ordered by registration (enforced at registration time), so
  /// this walks the tail starting at {@code currentVersion}. A stored version above the current
  /// version (e.g. after a controller downgrade) leaves the config untouched.
  private static <T, M extends ConfigMigrator<T>> T migrate(T config, int currentVersion, List<M> migrators) {
    T result = config;
    for (int version = currentVersion; version < migrators.size(); version++) {
      result = migrators.get(version).migrate(result);
    }
    return result;
  }
}
