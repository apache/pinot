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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Exercises warning-only and opted-in migration through the real controller resource manager and ZooKeeper store.
// Legacy APIs are deliberately used to reproduce definitions stored by older Pinot versions.
@SuppressWarnings({"deprecation", "removal"})
@Test(groups = "stateless")
public class ConfigMigrationManagerIntegrationTest extends ControllerTest {
  private static final String TABLE_NAME = "migrationDefinition";
  private static final String TABLE_NAME_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REPLICATION_TEMPLATE = "${pinot.migration.test.replication:1}";

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.CONFIG_MIGRATION_INITIAL_DELAY_SECONDS, 3600);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }

  @Test
  public void testWarningOnlyThenOptedInUpdatesBothStoredDefinitions()
      throws Exception {
    Schema legacySchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("country", DataType.STRING).build();
    legacySchema.getFieldSpecFor("country").setTransformFunction("lower(rawCountry)");
    legacySchema.addField(new TimeFieldSpec(
        new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "eventTimeSeconds"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "eventTime")));
    // New REST submissions reject TIME; direct persistence reproduces an existing legacy schema.
    ZKMetadataProvider.setSchema(_propertyStore, legacySchema);
    TableConfig legacyTable = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName("eventTime").setSegmentPushType("APPEND").setSegmentPushFrequency("HOURLY").build();
    _helixResourceManager.addTable(legacyTable);
    legacyTable.getValidationConfig().setReplication(REPLICATION_TEMPLATE);
    assertTrue(ZKMetadataProvider.setTableConfig(_propertyStore, legacyTable, -1));

    String tablePath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(TABLE_NAME_WITH_TYPE);
    String schemaPath = ZKMetadataProvider.constructPropertyStorePathForSchema(TABLE_NAME);
    Stat tableBefore = new Stat();
    Stat schemaBefore = new Stat();
    ZNRecord tableRecord = _propertyStore.get(tablePath, tableBefore, AccessOption.PERSISTENT);
    ZNRecord schemaRecord = _propertyStore.get(schemaPath, schemaBefore, AccessOption.PERSISTENT);
    ControllerMetrics metrics = _controllerStarter.getControllerMetrics();
    long tableRecommendations = metrics.getMeteredTableValue(TABLE_NAME_WITH_TYPE,
        ControllerMeter.CONFIG_MIGRATION_RECOMMENDED).count();
    long schemaRecommendations = metrics.getMeteredTableValue(TABLE_NAME,
        ControllerMeter.SCHEMA_MIGRATION_RECOMMENDED).count();

    TestUtils.waitForCondition(() -> {
      if (metrics.getMeteredTableValue(TABLE_NAME_WITH_TYPE,
          ControllerMeter.CONFIG_MIGRATION_RECOMMENDED).count() > tableRecommendations
          && metrics.getMeteredTableValue(TABLE_NAME,
              ControllerMeter.SCHEMA_MIGRATION_RECOMMENDED).count() > schemaRecommendations) {
        return true;
      }
      runPeriodicTask(ConfigMigrationManager.TASK_NAME, TABLE_NAME, TableType.OFFLINE);
      return false;
    }, 500, 10_000, "Warning-only scan did not report both definitions", null);

    Stat tableAfterWarning = new Stat();
    Stat schemaAfterWarning = new Stat();
    assertEquals(_propertyStore.get(tablePath, tableAfterWarning, AccessOption.PERSISTENT), tableRecord);
    assertEquals(_propertyStore.get(schemaPath, schemaAfterWarning, AccessOption.PERSISTENT), schemaRecord);
    assertEquals(tableAfterWarning.getVersion(), tableBefore.getVersion());
    assertEquals(schemaAfterWarning.getVersion(), schemaBefore.getVersion());
    updateClusterConfig(Map.of(ControllerConf.ControllerPeriodicTasksConf.CONFIG_MIGRATION_AUTO_UPDATE_ENABLED,
        "true"));
    TestUtils.waitForCondition(() -> {
      if (ZKMetadataProvider.getTableConfigWithVersion(_propertyStore, TABLE_NAME_WITH_TYPE, false, false)
          .getRight() > tableBefore.getVersion()
          && ZKMetadataProvider.getSchemaWithVersion(_propertyStore, TABLE_NAME).getRight()
              > schemaBefore.getVersion()) {
        return true;
      }
      runPeriodicTask(ConfigMigrationManager.TASK_NAME, TABLE_NAME, TableType.OFFLINE);
      return false;
    }, 500, 10_000, "Cluster config did not enable schema and table updates without a restart", null);

    var storedTable = ZKMetadataProvider.getTableConfigWithVersion(
        _propertyStore, TABLE_NAME_WITH_TYPE, false, false);
    var storedSchema = ZKMetadataProvider.getSchemaWithVersion(_propertyStore, TABLE_NAME);
    assertNotNull(storedTable);
    assertNotNull(storedSchema);
    assertEquals(storedTable.getRight().intValue(), tableBefore.getVersion() + 1);
    assertEquals(storedSchema.getRight().intValue(), schemaBefore.getVersion() + 1);
    assertEquals(storedTable.getLeft().getValidationConfig().getReplication(), REPLICATION_TEMPLATE);
    assertNull(storedTable.getLeft().getValidationConfig().getSegmentPushType());
    assertEquals(storedTable.getLeft().getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionType(),
        "APPEND");
    assertEquals(storedTable.getLeft().getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionFrequency(),
        "HOURLY");
    assertNull(storedSchema.getLeft().getTimeFieldSpec());
    assertNotNull(storedSchema.getLeft().getDateTimeSpec("eventTime"));
    assertNull(storedSchema.getLeft().getDateTimeSpec("eventTime").getTransformFunction());
    assertNull(storedSchema.getLeft().getFieldSpecFor("country").getTransformFunction());
    assertEquals(storedTable.getLeft().getIngestionConfig().getTransformConfigs().stream()
            .collect(Collectors.toMap(TransformConfig::getColumnName, TransformConfig::getTransformFunction)),
        Map.of("country", "lower(rawCountry)", "eventTime", "fromEpochSeconds(eventTimeSeconds)"));
    assertEquals(_helixResourceManager.getTableConfig(TABLE_NAME_WITH_TYPE).getReplication(), 1);
  }
}
