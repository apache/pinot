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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.data.Schema;


/// Reads and stamps the migration-version marker on [TableConfig] and [Schema].
///
/// The marker records the highest migration version that has been applied to a stored config, so
/// the migration chain can resume from the right step and skip configs that are already current.
///
/// - **TableConfig**: the marker lives under [#MIGRATION_VERSION_KEY] in
///   [TableCustomConfig#getCustomConfigs()]. Storing it in the free-form custom-config map avoids
///   adding a first-class SPI field and keeps mixed-version rollback safe (older controllers simply
///   preserve the unknown key).
/// - **Schema**: the marker is the dedicated {@code configMigrationVersion} field on [Schema], which
///   older readers ignore via {@code @JsonIgnoreProperties(ignoreUnknown = true)}.
///
/// A missing or unparseable marker is treated as version {@link #INITIAL_VERSION} (0) — the state of
/// any config written before this framework existed.
public class ConfigMigrationUtils {
  private ConfigMigrationUtils() {
  }

  /// Reserved custom-config key under which a table config's applied migration version is stored.
  ///
  /// This key is **controller-managed**: it is written by the config-migration task and is visible in the table
  /// config's `metadata.customConfigs` in REST responses. Operators should not set or edit it; a user-provided value
  /// is overwritten on the next migration. Other custom-config entries are preserved untouched.
  public static final String MIGRATION_VERSION_KEY = "config.migration.version";

  /// The version assigned to configs written before the migration framework existed.
  public static final int INITIAL_VERSION = 0;

  /// Returns the migration version recorded on the given table config, or {@link #INITIAL_VERSION}
  /// if the marker is absent or unparseable.
  public static int getTableConfigVersion(TableConfig tableConfig) {
    TableCustomConfig customConfig = tableConfig.getCustomConfig();
    Map<String, String> customConfigs = customConfig != null ? customConfig.getCustomConfigs() : null;
    if (customConfigs == null) {
      return INITIAL_VERSION;
    }
    return parseVersion(customConfigs.get(MIGRATION_VERSION_KEY));
  }

  /// Stamps the given migration version onto the table config's custom-config map in place, creating
  /// the [TableCustomConfig] if necessary. Existing custom configs are preserved.
  public static void setTableConfigVersion(TableConfig tableConfig, int version) {
    TableCustomConfig existing = tableConfig.getCustomConfig();
    // getCustomConfig() never returns null, but the backing map may be immutable, so always copy.
    Map<String, String> customConfigs =
        existing.getCustomConfigs() != null ? new LinkedHashMap<>(existing.getCustomConfigs()) : new LinkedHashMap<>();
    customConfigs.put(MIGRATION_VERSION_KEY, Integer.toString(version));
    tableConfig.setCustomConfig(new TableCustomConfig(customConfigs));
  }

  /// Returns the migration version recorded on the given schema.
  public static int getSchemaVersion(Schema schema) {
    return schema.getConfigMigrationVersion();
  }

  /// Stamps the given migration version onto the schema in place.
  public static void setSchemaVersion(Schema schema, int version) {
    schema.setConfigMigrationVersion(version);
  }

  private static int parseVersion(String value) {
    if (value == null) {
      return INITIAL_VERSION;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return INITIAL_VERSION;
    }
  }
}
