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

import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.utils.migration.DefaultConfigMigrationRegistry;
import org.apache.pinot.spi.config.migration.ConfigMigrationRegistry;
import org.apache.pinot.spi.config.migration.ConfigMigrationUtils;
import org.apache.pinot.spi.config.migration.SchemaMigrator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class ConfigMigrationManagerTest {
  private static final String RAW_TABLE_NAME = "myTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final String CONFIG_PATH = "/CONFIGS/TABLE/" + OFFLINE_TABLE_NAME;
  private static final String SCHEMA_PATH = "/SCHEMAS/" + RAW_TABLE_NAME;

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private PinotHelixResourceManager _resourceManager;
  private ControllerMetrics _controllerMetrics;

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setUp() {
    _propertyStore = mock(ZkHelixPropertyStore.class);
    _resourceManager = mock(PinotHelixResourceManager.class);
    when(_resourceManager.getPropertyStore()).thenReturn(_propertyStore);
    _controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  }

  private ConfigMigrationManager newManager(ConfigMigrationRegistry registry) {
    return new ConfigMigrationManager(_resourceManager, mock(LeadControllerManager.class), new ControllerConf(),
        _controllerMetrics, registry);
  }

  private void stubStoredTableConfig(TableConfig tableConfig, int zkVersion)
      throws Exception {
    ZNRecord znRecord = TableConfigSerDeUtils.toZNRecord(tableConfig);
    when(_propertyStore.get(eq(CONFIG_PATH), any(), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      Stat stat = invocation.getArgument(1);
      if (stat != null) {
        stat.setVersion(zkVersion);
      }
      return znRecord;
    });
  }

  private static TableConfig legacyTableConfig() {
    // OFFLINE avoids a required stream config; the deprecated batch push fields still trigger a v0 -> v1 migration.
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setSegmentPushType("APPEND")
        .setSegmentPushFrequency("HOURLY")
        .build();
  }

  private static Schema validSchema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("d", DataType.STRING)
        .build();
  }

  private long meterValue(ControllerMeter meter) {
    return _controllerMetrics.getMeteredTableValue(OFFLINE_TABLE_NAME, meter).count();
  }

  @Test
  public void testMigratesAndPersistsThroughResourceManagerWithVersionCheck()
      throws Exception {
    stubStoredTableConfig(legacyTableConfig(), 7);
    when(_resourceManager.getSchema(RAW_TABLE_NAME)).thenReturn(validSchema());

    newManager(DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());

    // Persisted through the standard write path (which sends cache-refresh messages) with the exact ZK version read.
    ArgumentCaptor<TableConfig> captor = ArgumentCaptor.forClass(TableConfig.class);
    verify(_resourceManager).setExistingTableConfig(captor.capture(), eq(7), eq(true));
    TableConfig persisted = captor.getValue();
    assertEquals(ConfigMigrationUtils.getTableConfigVersion(persisted), 1);
    // Deprecated fields folded into ingestionConfig and cleared.
    assertEquals(persisted.getValidationConfig().getSegmentPushType(), null);
    assertEquals(meterValue(ControllerMeter.CONFIG_MIGRATION_SUCCESS), 1L);
  }

  @Test
  public void testAlreadyCurrentConfigIsNotWritten()
      throws Exception {
    TableConfig current = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    ConfigMigrationUtils.setTableConfigVersion(current, 1);
    stubStoredTableConfig(current, 3);

    newManager(DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());

    verify(_resourceManager, never()).setExistingTableConfig(any(), anyInt(), anyBoolean());
  }

  @Test
  public void testMissingSchemaSkipsWithoutFailureMetric()
      throws Exception {
    stubStoredTableConfig(legacyTableConfig(), 7);
    when(_resourceManager.getSchema(RAW_TABLE_NAME)).thenReturn(null);

    newManager(DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());

    // A not-yet-created schema is a transient state, not a migration failure: skip quietly and do not persist.
    verify(_resourceManager, never()).setExistingTableConfig(any(), anyInt(), anyBoolean());
    assertEquals(meterValue(ControllerMeter.CONFIG_MIGRATION_FAILURE), 0L);
  }

  @Test
  public void testPersistFailureIsCaughtAndMetered()
      throws Exception {
    stubStoredTableConfig(legacyTableConfig(), 7);
    when(_resourceManager.getSchema(RAW_TABLE_NAME)).thenReturn(validSchema());
    // Simulate a lost optimistic-concurrency race: the resource manager throws on the version-checked write.
    doThrow(new RuntimeException("version mismatch")).when(_resourceManager)
        .setExistingTableConfig(any(), eq(7), eq(true));

    // Should complete without propagating; the table will be retried on the next cycle.
    newManager(DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());

    verify(_resourceManager).setExistingTableConfig(any(), eq(7), eq(true));
    assertEquals(meterValue(ControllerMeter.CONFIG_MIGRATION_FAILURE), 1L);
  }

  @Test
  public void testSchemaMigratedOnceAndMarkerPersistedAcrossHybridHalves()
      throws Exception {
    // Register a real (identity) schema migrator so the schema path runs and "migrated once" is non-vacuous.
    ConfigMigrationRegistry registry = new ConfigMigrationRegistry();
    registry.registerSchemaMigrator(new IdentitySchemaMigrator());
    stubStoredTableConfig(currentOfflineConfig(), 1);
    when(_resourceManager.getSchema(RAW_TABLE_NAME)).thenReturn(validSchema());
    when(_resourceManager.getExistingTableNamesWithType(RAW_TABLE_NAME, null)).thenReturn(List.of(OFFLINE_TABLE_NAME));

    ConfigMigrationManager manager = newManager(registry);
    ConfigMigrationManager.Context context = new ConfigMigrationManager.Context();
    manager.processTable(OFFLINE_TABLE_NAME, context);
    manager.processTable(REALTIME_TABLE_NAME, context);

    // The shared schema is written to ZK exactly once across both hybrid halves, with the version marker stamped.
    // Direct ZKMetadataProvider write (not updateSchema) is required so the marker persists despite Schema.equals()
    // excluding it — otherwise a marker-only migration would loop forever.
    ArgumentCaptor<ZNRecord> captor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(_propertyStore, times(1)).set(eq(SCHEMA_PATH), captor.capture(), eq(AccessOption.PERSISTENT));
    Schema persisted = SchemaSerDeUtils.fromZNRecord(captor.getValue());
    assertEquals(persisted.getConfigMigrationVersion(), 1);
    // A refresh message is sent so broker/server caches converge.
    verify(_resourceManager).sendTableConfigSchemaRefreshMessage(OFFLINE_TABLE_NAME);
  }

  @Test
  public void testEmptySchemaChainSkipsSchemaRead()
      throws Exception {
    // The shipped registry has an empty schema chain: schema migration must not touch ZK at all (no wasted reads).
    stubStoredTableConfig(currentOfflineConfig(), 1);

    newManager(DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());

    verify(_propertyStore, never()).set(eq(SCHEMA_PATH), any(), anyInt());
    verify(_propertyStore, never()).set(eq(SCHEMA_PATH), any(), eq(AccessOption.PERSISTENT));
  }

  private static TableConfig currentOfflineConfig() {
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    // Already at the current table-config version so the run focuses on the schema path.
    ConfigMigrationUtils.setTableConfigVersion(config, DefaultConfigMigrationRegistry.create()
        .getCurrentTableConfigVersion());
    return config;
  }

  /// A schema migrator that advances v0 -> v1 without changing any field, used to make the schema path observable.
  private static final class IdentitySchemaMigrator implements SchemaMigrator {
    @Override
    public int fromVersion() {
      return 0;
    }

    @Override
    public Schema migrate(Schema input) {
      return input;
    }
  }
}
