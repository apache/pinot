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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.segment.local.utils.migration.DefaultConfigMigrationRegistry;
import org.apache.pinot.spi.config.migration.ConfigMigrationRegistry;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.TableConfigDecoratorRegistry;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Exercises reporting-only defaults, explicit writes, failure reporting, and shared-schema deduplication.
public class ConfigMigrationManagerTest {
  private static final String RAW_TABLE_NAME = "myTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final String CONFIG_PATH = "/CONFIGS/TABLE/" + OFFLINE_TABLE_NAME;
  private static final String SCHEMA_PATH = "/SCHEMAS/" + RAW_TABLE_NAME;
  private static final String AUTO_UPDATE_KEY =
      ControllerConf.ControllerPeriodicTasksConf.CONFIG_MIGRATION_AUTO_UPDATE_ENABLED;

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private PinotHelixResourceManager _resourceManager;
  private ControllerMetrics _controllerMetrics;

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setUp()
      throws Exception {
    _propertyStore = mock(ZkHelixPropertyStore.class);
    _resourceManager = mock(PinotHelixResourceManager.class);
    when(_resourceManager.getPropertyStore()).thenReturn(_propertyStore);
    when(_resourceManager.getExistingTableNamesWithType(RAW_TABLE_NAME, null)).thenReturn(List.of());
    _controllerMetrics = mock(ControllerMetrics.class);
  }

  private ConfigMigrationManager newManager(boolean autoUpdate, ConfigMigrationRegistry registry) {
    ConfigMigrationManager manager =
        new ConfigMigrationManager(_resourceManager, mock(LeadControllerManager.class), new ControllerConf(),
            _controllerMetrics, registry);
    Map<String, String> clusterConfig = autoUpdate ? Map.of(AUTO_UPDATE_KEY, "true") : Map.of();
    manager.onChange(clusterConfig.keySet(), clusterConfig);
    return manager;
  }

  private void stubRecord(String path, ZNRecord record, int version) {
    when(_propertyStore.get(eq(path), any(), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      Stat stat = invocation.getArgument(1);
      if (stat != null) {
        stat.setVersion(version);
      }
      return record;
    });
  }

  private void stubTable(TableConfig config)
      throws Exception {
    stubRecord(CONFIG_PATH, TableConfigSerDeUtils.toZNRecord(config), 7);
  }

  // Exercise the legacy stored format that this task recommends replacing.
  @SuppressWarnings({"deprecation", "removal"})
  private static TableConfig legacyTable() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setSegmentPushType("APPEND").setSegmentPushFrequency("HOURLY").build();
  }

  // Remove the deprecated property emitted by the builder to reproduce a modern stored config.
  @SuppressWarnings("deprecation")
  private static TableConfig currentTable() {
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    // The builder still emits the old APPEND property by default; a modern definition does not contain it.
    config.getValidationConfig().setSegmentPushType(null);
    return config;
  }

  private static Schema currentSchema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("d", DataType.STRING).build();
  }

  private static IngestionConfig ingestionTransforms(TransformConfig... transforms) {
    IngestionConfig ingestion = new IngestionConfig();
    ingestion.setTransformConfigs(List.of(transforms));
    return ingestion;
  }

  private static Map<String, String> transforms(TableConfig table) {
    return table.getIngestionConfig().getTransformConfigs().stream()
        .collect(Collectors.toMap(TransformConfig::getColumnName, TransformConfig::getTransformFunction));
  }

  @SuppressWarnings("deprecation") // Stored schema transforms are the migration inputs.
  private Schema stubSchemaTransform(TableConfig table)
      throws Exception {
    Schema schema = currentSchema();
    schema.getFieldSpecFor("d").setTransformFunction("lower(sourceD)");
    stubTable(table);
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(schema), 3);
    when(_resourceManager.getExistingTableNamesWithType(RAW_TABLE_NAME, null))
        .thenReturn(List.of(OFFLINE_TABLE_NAME));
    return schema;
  }

  @SuppressWarnings("deprecation") // Stored TIME schemas remain readable and are migration inputs.
  private static Schema legacySchema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "ts"), null).build();
  }

  @Test
  public void testClusterConfigControlsBothDefinitionUpdates()
      throws Exception {
    TableConfig table = legacyTable();
    Schema schema = legacySchema();
    String originalTable = table.toJsonString();
    String originalSchema = schema.toSingleLineJsonString();
    stubTable(table);
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(schema), 3);

    ConfigMigrationManager manager = newManager(false, DefaultConfigMigrationRegistry.create());
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());

    verify(_controllerMetrics)
        .addMeteredTableValue(OFFLINE_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_RECOMMENDED, 1L);
    verify(_controllerMetrics).addMeteredTableValue(RAW_TABLE_NAME, ControllerMeter.SCHEMA_MIGRATION_RECOMMENDED, 1L);
    verify(_resourceManager, never()).setExistingTableConfig(any(), any(), anyInt(), anyBoolean());
    verify(_resourceManager, never()).updateSchema(any(), anyInt());
    verify(_propertyStore, never()).set(any(), any(), anyInt(), anyInt());
    assertEquals(table.toJsonString(), originalTable);
    assertEquals(schema.toSingleLineJsonString(), originalSchema);

    when(_resourceManager.updateSchema(any(), eq(3))).thenReturn(true);
    manager.onChange(Set.of(AUTO_UPDATE_KEY), Map.of(AUTO_UPDATE_KEY, "true"));
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_resourceManager).setExistingTableConfig(any(), any(), eq(7), eq(false));
    verify(_resourceManager).updateSchema(any(), eq(3));

    manager.onChange(Set.of("unrelated"), Map.of());
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_resourceManager, times(2)).setExistingTableConfig(any(), any(), eq(7), eq(false));
    verify(_resourceManager, times(2)).updateSchema(any(), eq(3));

    manager.onChange(Set.of(AUTO_UPDATE_KEY), Map.of(AUTO_UPDATE_KEY, "false"));
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_resourceManager, times(2)).setExistingTableConfig(any(), any(), eq(7), eq(false));
    verify(_resourceManager, times(2)).updateSchema(any(), eq(3));

    manager.onChange(Set.of(AUTO_UPDATE_KEY), Map.of(AUTO_UPDATE_KEY, "true"));
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    manager.onChange(Set.of(AUTO_UPDATE_KEY), Map.of());
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_resourceManager, times(3)).setExistingTableConfig(any(), any(), eq(7), eq(false));
    verify(_resourceManager, times(3)).updateSchema(any(), eq(3));
  }

  @Test
  @SuppressWarnings({"deprecation", "removal"}) // Exercise a template in the legacy property being migrated.
  public void testTableUpdateKeepsRawTemplateAndUsesResolvedRuntimeConfig()
      throws Exception {
    String property = "pinot.migration.test.pushType";
    String previous = System.setProperty(property, "APPEND");
    try (MockedStatic<TableConfigDecoratorRegistry> decorators = mockStatic(TableConfigDecoratorRegistry.class)) {
      decorators.when(() -> TableConfigDecoratorRegistry.applyDecorator(any())).thenAnswer(invocation -> {
        TableConfig decorated = invocation.getArgument(0);
        TenantConfig tenant = decorated.getTenantConfig();
        decorated.setTenantConfig(
            new TenantConfig(tenant.getBroker(), "decoratedTenant", tenant.getTagOverrideConfig()));
        return decorated;
      });
      TableConfig config = legacyTable();
      // Intentionally use the legacy template to verify conversion does not persist the resolved value.
      config.getValidationConfig().setSegmentPushType("${" + property + "}");
      stubTable(config);
      stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(currentSchema()), 3);

      newManager(true, DefaultConfigMigrationRegistry.create())
          .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());

      ArgumentCaptor<TableConfig> raw = ArgumentCaptor.forClass(TableConfig.class);
      ArgumentCaptor<TableConfig> effective = ArgumentCaptor.forClass(TableConfig.class);
      verify(_resourceManager).setExistingTableConfig(raw.capture(), effective.capture(), eq(7), eq(false));
      assertEquals(raw.getValue().getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionType(),
          "${" + property + "}");
      assertEquals(effective.getValue().getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionType(),
          "APPEND");
      assertEquals(effective.getValue().getTenantConfig().getServer(), "decoratedTenant");
      assertEquals(raw.getValue().getTenantConfig().getServer(), config.getTenantConfig().getServer());
      verify(_controllerMetrics).addMeteredTableValue(OFFLINE_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  @Test
  public void testModernUnmarkedDefinitionsProduceNoRecommendation()
      throws Exception {
    stubTable(currentTable());
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(currentSchema()), 3);
    newManager(true, DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verifyNoInteractions(_controllerMetrics);
    verify(_resourceManager, never()).setExistingTableConfig(any(), any(), anyInt(), anyBoolean());
    verify(_resourceManager, never()).updateSchema(any(), anyInt());
  }

  @Test
  public void testTableConflictDoesNotReportSuccess()
      throws Exception {
    stubTable(legacyTable());
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(currentSchema()), 3);
    doThrow(new IllegalStateException("version mismatch")).when(_resourceManager)
        .setExistingTableConfig(any(), any(), eq(7), eq(false));
    newManager(true, DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_controllerMetrics)
        .addMeteredTableValue(OFFLINE_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
    verify(_controllerMetrics, never())
        .addMeteredTableValue(OFFLINE_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
  }

  @Test
  public void testSchemaConflictDoesNotReportSuccess()
      throws Exception {
    stubTable(currentTable());
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(legacySchema()), 3);
    when(_resourceManager.updateSchema(any(), eq(3))).thenReturn(false);
    newManager(true, DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_resourceManager).updateSchema(any(), eq(3));
    verify(_controllerMetrics).addMeteredTableValue(RAW_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
    verify(_controllerMetrics, never())
        .addMeteredTableValue(RAW_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
  }

  @Test
  @SuppressWarnings("deprecation") // Assert that the legacy TIME field was removed.
  public void testSharedSchemaUpdatedOnce()
      throws Exception {
    stubTable(currentTable());
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(legacySchema()), 3);
    when(_resourceManager.updateSchema(any(), eq(3))).thenReturn(true);
    ConfigMigrationManager manager = newManager(true, DefaultConfigMigrationRegistry.create());
    ConfigMigrationManager.Context context = new ConfigMigrationManager.Context();
    manager.processTable(OFFLINE_TABLE_NAME, context);
    manager.processTable(REALTIME_TABLE_NAME, context);
    ArgumentCaptor<Schema> schema = ArgumentCaptor.forClass(Schema.class);
    verify(_resourceManager, times(1)).updateSchema(schema.capture(), eq(3));
    assertNull(schema.getValue().getTimeFieldSpec());
    assertEquals(schema.getValue().getDateTimeFieldSpecs().get(0).getName(), "ts");
    verify(_controllerMetrics).addMeteredTableValue(RAW_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_SUCCESS, 1L);
  }

  @Test
  @SuppressWarnings("deprecation") // Exercise existing schema transforms and remove the builder's legacy push type.
  public void testSchemaTransformsMoveToBothHybridTablesAfterOptIn()
      throws Exception {
    TableConfig offline = currentTable();
    offline.setIngestionConfig(ingestionTransforms(new TransformConfig("d", "trim(sourceD)")));
    Schema schema = stubSchemaTransform(offline);
    schema.addField(new DimensionFieldSpec("copied", DataType.STRING, true));
    schema.getFieldSpecFor("copied").setTransformFunction("lower(sourceCopied)");
    schema.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(schema), 3);
    TableConfig realtime = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName("ts").build();
    realtime.getValidationConfig().setSegmentPushType(null);
    IngestionConfig realtimeIngestion = ingestionTransforms(new TransformConfig("d", "upper(sourceD)"));
    realtimeIngestion.setStreamIngestionConfig(new StreamIngestionConfig(
        List.of(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap())));
    realtime.setIngestionConfig(realtimeIngestion);
    stubRecord("/CONFIGS/TABLE/" + REALTIME_TABLE_NAME, TableConfigSerDeUtils.toZNRecord(realtime), 11);
    when(_resourceManager.getExistingTableNamesWithType(RAW_TABLE_NAME, null))
        .thenReturn(List.of(OFFLINE_TABLE_NAME, REALTIME_TABLE_NAME));
    String originalSchema = schema.toSingleLineJsonString();
    ConfigMigrationManager manager = newManager(false, DefaultConfigMigrationRegistry.create());
    ConfigMigrationManager.Context context = new ConfigMigrationManager.Context();
    manager.processTable(OFFLINE_TABLE_NAME, context);
    manager.processTable(REALTIME_TABLE_NAME, context);
    verify(_resourceManager, never()).updateSchemaAndTableConfigs(any(), anyInt(), any());
    verify(_resourceManager, never()).updateSchema(any(), anyInt());
    verify(_resourceManager, never()).setExistingTableConfig(any(), any(), anyInt(), anyBoolean());

    when(_resourceManager.updateSchemaAndTableConfigs(any(), eq(3), any())).thenReturn(true);
    manager.onChange(Set.of(AUTO_UPDATE_KEY), Map.of(AUTO_UPDATE_KEY, "true"));
    context = new ConfigMigrationManager.Context();
    manager.processTable(OFFLINE_TABLE_NAME, context);
    manager.processTable(REALTIME_TABLE_NAME, context);
    ArgumentCaptor<Schema> migrated = ArgumentCaptor.forClass(Schema.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, Pair<TableConfig, Integer>>> tables = ArgumentCaptor.forClass(Map.class);
    verify(_resourceManager).updateSchemaAndTableConfigs(migrated.capture(), eq(3), tables.capture());
    assertNull(migrated.getValue().getFieldSpecFor("d").getTransformFunction());
    assertNull(migrated.getValue().getFieldSpecFor("copied").getTransformFunction());
    assertEquals(tables.getValue().keySet(), Set.of(OFFLINE_TABLE_NAME, REALTIME_TABLE_NAME));
    assertEquals(tables.getValue().get(OFFLINE_TABLE_NAME).getRight().intValue(), 7);
    assertEquals(tables.getValue().get(REALTIME_TABLE_NAME).getRight().intValue(), 11);
    assertEquals(transforms(tables.getValue().get(OFFLINE_TABLE_NAME).getLeft()),
        Map.of("d", "trim(sourceD)", "copied", "lower(sourceCopied)"));
    assertEquals(transforms(tables.getValue().get(REALTIME_TABLE_NAME).getLeft()),
        Map.of("d", "upper(sourceD)", "copied", "lower(sourceCopied)"));
    assertEquals(schema.toSingleLineJsonString(), originalSchema);
  }

  @Test
  public void testSchemaTransformConflictAndFailureDoNotReportSuccess()
      throws Exception {
    Schema schema = stubSchemaTransform(currentTable());
    String original = schema.toSingleLineJsonString();
    when(_resourceManager.updateSchemaAndTableConfigs(any(), eq(3), any()))
        .thenReturn(false).thenThrow(new IllegalStateException("version mismatch"));
    ConfigMigrationManager manager = newManager(true, DefaultConfigMigrationRegistry.create());
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    manager.processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_resourceManager, times(2)).updateSchemaAndTableConfigs(any(), eq(3), any());
    verify(_resourceManager, never()).updateSchema(any(), anyInt());
    verify(_controllerMetrics, times(2))
        .addMeteredTableValue(RAW_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
    verify(_controllerMetrics, never()).addMeteredTableValue(any(), eq(ControllerMeter.CONFIG_MIGRATION_SUCCESS),
        anyLong());
    assertEquals(schema.toSingleLineJsonString(), original);
  }

  @Test
  @SuppressWarnings("deprecation") // Empty and virtual legacy transforms retain their existing behavior.
  public void testEmptyAndVirtualSchemaTransformsAreUnchanged()
      throws Exception {
    Schema schema = currentSchema();
    schema.getFieldSpecFor("d").setTransformFunction("");
    DimensionFieldSpec virtual = new DimensionFieldSpec("virtual", DataType.STRING, true);
    virtual.setVirtualColumnProvider("testProvider");
    virtual.setTransformFunction("lower(sourceVirtual)");
    schema.addField(virtual);
    stubTable(currentTable());
    stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(schema), 3);
    newManager(true, DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_resourceManager, never()).updateSchemaAndTableConfigs(any(), anyInt(), any());
    verify(_resourceManager, never()).updateSchema(any(), anyInt());
    verifyNoInteractions(_controllerMetrics);
    assertEquals(schema.getFieldSpecFor("d").getTransformFunction(), "");
    assertEquals(schema.getFieldSpecFor("virtual").getTransformFunction(), "lower(sourceVirtual)");
  }

  @DataProvider(name = "effectiveOverrides")
  public Object[][] effectiveOverrides() {
    return new Object[][]{{"template"}, {"schemaDestinationDecorator"}, {"unrelatedDestinationDecorator"}};
  }

  @Test(dataProvider = "effectiveOverrides")
  public void testEffectiveOverrideMismatchLeavesSchemaIntact(String override)
      throws Exception {
    boolean template = override.equals("template");
    boolean unrelated = override.equals("unrelatedDestinationDecorator");
    TableConfig table = currentTable();
    if (template) {
      table.setIngestionConfig(ingestionTransforms(
          new TransformConfig("${pinot.migration.test.column:d}", "upper(sourceD)")));
    }
    Schema schema = stubSchemaTransform(table);
    if (unrelated) {
      schema.addField(new DimensionFieldSpec("country", DataType.STRING, true));
      stubRecord(SCHEMA_PATH, SchemaSerDeUtils.toZNRecord(schema), 3);
    }
    String original = schema.toSingleLineJsonString();
    try (MockedStatic<TableConfigDecoratorRegistry> decorators = mockStatic(TableConfigDecoratorRegistry.class)) {
      decorators.when(() -> TableConfigDecoratorRegistry.applyDecorator(any())).thenAnswer(invocation -> {
        TableConfig decorated = invocation.getArgument(0);
        if (!template && decorated.getIngestionConfig() == null) {
          String column = unrelated ? "country" : "d";
          String transform = unrelated ? "lower(sourceCountry)" : "upper(sourceD)";
          decorated.setIngestionConfig(ingestionTransforms(new TransformConfig(column, transform)));
        }
        return decorated;
      });
      newManager(true, DefaultConfigMigrationRegistry.create())
          .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    }
    verify(_resourceManager, never()).updateSchemaAndTableConfigs(any(), anyInt(), any());
    verify(_resourceManager, never()).updateSchema(any(), anyInt());
    verify(_controllerMetrics).addMeteredTableValue(RAW_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_FAILURE, 1L);
    assertEquals(schema.toSingleLineJsonString(), original);
  }

  @Test
  public void testMissingSchemaStillReportsTableRecommendation()
      throws Exception {
    stubTable(legacyTable());
    newManager(true, DefaultConfigMigrationRegistry.create())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verify(_controllerMetrics)
        .addMeteredTableValue(OFFLINE_TABLE_NAME, ControllerMeter.CONFIG_MIGRATION_RECOMMENDED, 1L);
    verify(_resourceManager, never()).setExistingTableConfig(any(), any(), anyInt(), anyBoolean());
  }

  @Test
  public void testEmptyRegistryDoesNotReadStore() {
    newManager(false, new ConfigMigrationRegistry())
        .processTable(OFFLINE_TABLE_NAME, new ConfigMigrationManager.Context());
    verifyNoInteractions(_propertyStore, _controllerMetrics);
  }
}
