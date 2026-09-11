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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ConfigMigrationRegistryTest {

  private static TableConfig newTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
  }

  private static Schema newSchema() {
    Schema schema = new Schema();
    schema.setSchemaName("myTable");
    schema.addField(new DimensionFieldSpec("d", DataType.STRING, true));
    return schema;
  }

  /// A schema migrator that records the version it saw and bumps a counter in the schema description so we can assert
  /// migrators run in order.
  private static class RecordingSchemaMigrator implements SchemaMigrator {
    private final int _fromVersion;
    private final List<Integer> _log;

    RecordingSchemaMigrator(int fromVersion, List<Integer> log) {
      _fromVersion = fromVersion;
      _log = log;
    }

    @Override
    public int fromVersion() {
      return _fromVersion;
    }

    @Override
    public Schema migrate(Schema input) {
      _log.add(_fromVersion);
      return input;
    }
  }

  @Test
  public void testEmptyRegistryIsIdentity() {
    ConfigMigrationRegistry registry = new ConfigMigrationRegistry();
    assertEquals(registry.getCurrentTableConfigVersion(), 0);
    assertEquals(registry.getCurrentSchemaVersion(), 0);

    TableConfig tableConfig = newTableConfig();
    MigrationResult<TableConfig> result = registry.migrateTableConfig(tableConfig);
    assertFalse(result.isChanged());
    assertSame(result.getConfig(), tableConfig);
    assertEquals(result.getVersion(), 0);
  }

  @Test
  public void testMigratorsRunInOrderFromStoredVersion() {
    List<Integer> log = new ArrayList<>();
    ConfigMigrationRegistry registry = new ConfigMigrationRegistry();
    registry.registerSchemaMigrator(new RecordingSchemaMigrator(0, log));
    registry.registerSchemaMigrator(new RecordingSchemaMigrator(1, log));
    registry.registerSchemaMigrator(new RecordingSchemaMigrator(2, log));
    assertEquals(registry.getCurrentSchemaVersion(), 3);

    // A version-0 schema runs all three migrators in order.
    Schema schema = newSchema();
    MigrationResult<Schema> result = registry.migrateSchema(schema);
    assertTrue(result.isChanged());
    assertEquals(result.getVersion(), 3);
    assertEquals(ConfigMigrationUtils.getSchemaVersion(result.getConfig()), 3);
    assertEquals(log, List.of(0, 1, 2));

    // A schema already at version 2 only runs the last migrator.
    log.clear();
    Schema partiallyMigrated = newSchema();
    partiallyMigrated.setConfigMigrationVersion(2);
    MigrationResult<Schema> result2 = registry.migrateSchema(partiallyMigrated);
    assertTrue(result2.isChanged());
    assertEquals(result2.getVersion(), 3);
    assertEquals(log, List.of(2));
  }

  @Test
  public void testAlreadyCurrentIsNoOp() {
    List<Integer> log = new ArrayList<>();
    ConfigMigrationRegistry registry = new ConfigMigrationRegistry();
    registry.registerSchemaMigrator(new RecordingSchemaMigrator(0, log));

    Schema schema = newSchema();
    schema.setConfigMigrationVersion(1);
    MigrationResult<Schema> result = registry.migrateSchema(schema);
    assertFalse(result.isChanged());
    assertSame(result.getConfig(), schema);
    assertEquals(result.getVersion(), 1);
    assertTrue(log.isEmpty());
  }

  @Test
  public void testStoredVersionAboveCurrentIsUntouched() {
    // e.g. after a controller downgrade: a config stamped at a higher version must not be "migrated" backwards.
    ConfigMigrationRegistry registry = new ConfigMigrationRegistry();
    registry.registerSchemaMigrator(new RecordingSchemaMigrator(0, new ArrayList<>()));

    Schema schema = newSchema();
    schema.setConfigMigrationVersion(5);
    MigrationResult<Schema> result = registry.migrateSchema(schema);
    assertFalse(result.isChanged());
    assertEquals(ConfigMigrationUtils.getSchemaVersion(result.getConfig()), 5);
  }

  @Test
  public void testRegistrationEnforcesDenseAscendingOrder() {
    ConfigMigrationRegistry registry = new ConfigMigrationRegistry();
    // First migrator must declare fromVersion == 0.
    assertThrows(IllegalArgumentException.class,
        () -> registry.registerSchemaMigrator(new RecordingSchemaMigrator(1, new ArrayList<>())));

    registry.registerSchemaMigrator(new RecordingSchemaMigrator(0, new ArrayList<>()));
    // Next migrator must declare fromVersion == 1, not skip to 2.
    assertThrows(IllegalArgumentException.class,
        () -> registry.registerSchemaMigrator(new RecordingSchemaMigrator(2, new ArrayList<>())));
  }

  @Test
  public void testTableConfigMarkerRoundTrip()
      throws Exception {
    // Start with a pre-existing user custom config that must be preserved when the marker is stamped.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable")
        .setCustomConfig(new TableCustomConfig(new HashMap<>(Map.of("userKey", "userValue"))))
        .build();
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(tableConfig), 0);

    ConfigMigrationUtils.setTableConfigVersion(tableConfig, 3);
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(tableConfig), 3);
    // Stamping the marker preserves the user's existing custom config.
    Map<String, String> customConfigs = tableConfig.getCustomConfig().getCustomConfigs();
    assertEquals(customConfigs.get("userKey"), "userValue");
    assertEquals(customConfigs.get(ConfigMigrationUtils.CONFIG_MIGRATION_VERSION_KEY), "3");

    // The marker (and the user config) survive a full JSON round-trip.
    TableConfig deserialized = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(deserialized), 3);
    assertEquals(deserialized.getCustomConfig().getCustomConfigs().get("userKey"), "userValue");
  }
}
