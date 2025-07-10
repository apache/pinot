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
package org.apache.pinot.controller.helix;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.LogicalTableMetadataCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class LogicalTableMetadataCacheTest {

  private static final ControllerTest INSTANCE = ControllerTest.getInstance();
  private static final LogicalTableMetadataCache CACHE = new LogicalTableMetadataCache();

  private final String _testTable = "testTable";
  private final String _extraTableName = "testExtraTable";
  private final Schema _testTableSchema = ControllerTest.createDummySchema(_testTable);
  private final Schema _extraTableSchema = ControllerTest.createDummySchema(_extraTableName);
  private final TableConfig _offlineTableConfig = ControllerTest.createDummyTableConfig(_testTable, TableType.OFFLINE);
  private final TableConfig _realtimeTableConfig =
      ControllerTest.createDummyTableConfig(_testTable, TableType.REALTIME);
  private final TableConfig _extraOfflineTableConfig =
      ControllerTest.createDummyTableConfig(_extraTableName, TableType.OFFLINE);
  private final TableConfig _extraRealtimeTableConfig =
      ControllerTest.createDummyTableConfig(_extraTableName, TableType.REALTIME);

  @BeforeClass
  public void setUp()
      throws Exception {
    INSTANCE.setupSharedStateAndValidate();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    INSTANCE.stopSharedTestSetup();
  }

  @BeforeMethod
  public void beforeMethod()
      throws IOException {
    ZkHelixPropertyStore<ZNRecord> propertyStore = INSTANCE.getHelixResourceManager().getPropertyStore();
    CACHE.init(propertyStore);

    // Setup schema and table configs in the property store
    INSTANCE.addSchema(_testTableSchema);
    INSTANCE.addSchema(_extraTableSchema);
    INSTANCE.addTableConfig(_offlineTableConfig);
    INSTANCE.addTableConfig(_realtimeTableConfig);
    INSTANCE.addTableConfig(_extraOfflineTableConfig);
    INSTANCE.addTableConfig(_extraRealtimeTableConfig);

    // Ensure the schema and table configs are not loaded into the cache yet
    assertNull(CACHE.getSchema(_testTable));
    assertNull(CACHE.getSchema(_extraTableName));
    assertNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_extraOfflineTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_extraRealtimeTableConfig.getTableName()));
  }

  @AfterMethod
  public void afterMethod() {
    CACHE.shutdown();
    INSTANCE.cleanup();
  }

  @Test
  public void testLogicalTableCacheWithUpdates()
      throws IOException {
    String logicalTableName = "testLogicalTable1";
    LogicalTableConfig logicalTableConfig = addLogicalTableAndValidateCache(
        logicalTableName, List.of(_offlineTableConfig.getTableName(), _realtimeTableConfig.getTableName()));
    Schema logicalTableSchema = ControllerTest.createDummySchema(logicalTableName);

    // Update logical table config and verify the cache is updated
    Map<String, PhysicalTableConfig> physicalTableConfigMap = logicalTableConfig.getPhysicalTableConfigMap();
    physicalTableConfigMap.put(_extraOfflineTableConfig.getTableName(), new PhysicalTableConfig());
    physicalTableConfigMap.put(_extraRealtimeTableConfig.getTableName(), new PhysicalTableConfig());
    assertNotEquals(CACHE.getLogicalTableConfig(logicalTableName), logicalTableConfig);
    INSTANCE.updateLogicalTableConfig(logicalTableConfig);
    TestUtils.waitForCondition(
        aVoid -> CACHE.getLogicalTableConfig(logicalTableName).equals(logicalTableConfig),
        10_000L, "Logical table config not updated in cache");
    assertNull(CACHE.getTableConfig(_extraOfflineTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_extraRealtimeTableConfig.getTableName()));

    // Update logical table schema and verify the cache is updated
    logicalTableSchema.addField(new MetricFieldSpec("newMetric", FieldSpec.DataType.INT));
    assertNotEquals(CACHE.getSchema(logicalTableName), logicalTableSchema);
    INSTANCE.updateSchema(logicalTableSchema);
    TestUtils.waitForCondition(
        aVoid -> CACHE.getSchema(logicalTableName).equals(logicalTableSchema),
        10_000L, "Logical table schema not updated in cache");

    // Update offline table configs and verify the cache is updated (update retention)
    _offlineTableConfig.getValidationConfig().setRetentionTimeValue("10");
    assertNotEquals(
        Objects.requireNonNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()))
            .getValidationConfig()
            .getRetentionTimeValue(),
        _offlineTableConfig.getValidationConfig().getRetentionTimeValue());
    INSTANCE.updateTableConfig(_offlineTableConfig);
    TestUtils.waitForCondition(
        aVoid -> Objects.requireNonNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()))
            .getValidationConfig()
            .getRetentionTimeValue()
            .equals(_offlineTableConfig.getValidationConfig().getRetentionTimeValue()),
        10_000L, "Offline table config not updated in cache");

    // Update realtime table configs and verify the cache is updated (update retention)
    _realtimeTableConfig.getValidationConfig().setRetentionTimeValue("20");
    assertNotEquals(
        Objects.requireNonNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()))
            .getValidationConfig()
            .getRetentionTimeValue(),
        _realtimeTableConfig.getValidationConfig().getRetentionTimeValue());
    INSTANCE.updateTableConfig(_realtimeTableConfig);
    TestUtils.waitForCondition(
        aVoid -> Objects.requireNonNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()))
            .getValidationConfig().getRetentionTimeValue()
            .equals(_realtimeTableConfig.getValidationConfig().getRetentionTimeValue()),
        10_000L, "Realtime table config not updated in cache");

    // Delete logical table config and verify the cache is removed
    INSTANCE.dropLogicalTable(logicalTableName);
    TestUtils.waitForCondition(
        aVoid -> CACHE.getSchema(logicalTableName) == null,
        10_000L, "Logical table schema not removed from cache");
    assertNull(CACHE.getLogicalTableConfig(logicalTableName));
    assertNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()));
  }

  @Test
  public void testLogicalTableUpdateRefTables()
      throws IOException {
    String logicalTableName = "testLogicalTable2";
    LogicalTableConfig logicalTableConfig = addLogicalTableAndValidateCache(
        logicalTableName, List.of(_offlineTableConfig.getTableName(), _realtimeTableConfig.getTableName()));

    // Update logical table config ref tables with extra tables and verify the cache is updated
    logicalTableConfig.setRefOfflineTableName(_extraOfflineTableConfig.getTableName());
    logicalTableConfig.setRefRealtimeTableName(_extraRealtimeTableConfig.getTableName());
    logicalTableConfig.setPhysicalTableConfigMap(
        Map.of(_extraOfflineTableConfig.getTableName(), new PhysicalTableConfig(),
            _extraRealtimeTableConfig.getTableName(), new PhysicalTableConfig())
    );
    assertNotEquals(CACHE.getLogicalTableConfig(logicalTableName), logicalTableConfig);

    INSTANCE.updateLogicalTableConfig(logicalTableConfig);
    TestUtils.waitForCondition(
        aVoid -> Objects.requireNonNull(CACHE.getLogicalTableConfig(logicalTableName)).equals(logicalTableConfig),
        10_000L, "Logical table config not updated in cache");
    assertNotNull(CACHE.getTableConfig(_extraOfflineTableConfig.getTableName()));
    assertNotNull(CACHE.getTableConfig(_extraRealtimeTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()));
    assertNotNull(CACHE.getSchema(logicalTableName));
  }

  @Test
  public void testCacheWithMultipleLogicalTables()
      throws IOException {
    String logicalTableName = "testLogicalTable3";
    LogicalTableConfig logicalTableConfig = addLogicalTableAndValidateCache(
        logicalTableName, List.of(_offlineTableConfig.getTableName(), _realtimeTableConfig.getTableName()));

    String otherLogicalTableName = "otherLogicalTable";
    LogicalTableConfig otherLogicalTableConfig = addLogicalTableAndValidateCache(
        otherLogicalTableName, List.of(_offlineTableConfig.getTableName(), _realtimeTableConfig.getTableName()));

    // Delete one logical table config and verify the other is still present
    INSTANCE.dropLogicalTable(logicalTableName);
    TestUtils.waitForCondition(
        aVoid -> CACHE.getSchema(logicalTableName) == null,
        10_000L, "Logical table schema not removed from cache");
    assertNull(CACHE.getLogicalTableConfig(logicalTableName));
    assertNotNull(CACHE.getLogicalTableConfig(otherLogicalTableName));
    assertNotNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()));
    assertNotNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()));

    // Delete the other logical table config and verify the cache is empty
    INSTANCE.dropLogicalTable(otherLogicalTableName);
    TestUtils.waitForCondition(
        aVoid -> CACHE.getSchema(otherLogicalTableName) == null,
        10_000L, "Logical table schema not removed from cache");
    assertNull(CACHE.getLogicalTableConfig(otherLogicalTableName));
    assertNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()));
  }

  private LogicalTableConfig addLogicalTableAndValidateCache(String logicalTableName, List<String> physicalTableNames)
      throws IOException {
    // Add logical table config
    Schema logicalTableSchema = ControllerTest.createDummySchema(logicalTableName);
    LogicalTableConfig logicalTableConfig = ControllerTest.getDummyLogicalTableConfig(logicalTableName,
        physicalTableNames, "DefaultTenant");
    INSTANCE.addSchema(logicalTableSchema);
    INSTANCE.addLogicalTableConfig(logicalTableConfig);

    // wait for the cache to be updated
    TestUtils.waitForCondition(
        aVoid -> CACHE.getLogicalTableConfig(logicalTableName) != null,
        10_000L, "Logical table config not loaded into cache");

    // Verify that the logical table config is loaded into the cache
    assertNotNull(CACHE.getSchema(logicalTableName));
    assertEquals(CACHE.getSchema(logicalTableName), logicalTableSchema);
    assertNotNull(CACHE.getTableConfig(_offlineTableConfig.getTableName()));
    assertNotNull(CACHE.getTableConfig(_realtimeTableConfig.getTableName()));
    assertNotNull(CACHE.getLogicalTableConfig(logicalTableName));
    assertEquals(CACHE.getLogicalTableConfig(logicalTableName), logicalTableConfig);

    // verify extra schema and table configs are not loaded
    assertNull(CACHE.getSchema(_extraTableName));
    assertNull(CACHE.getTableConfig(_extraOfflineTableConfig.getTableName()));
    assertNull(CACHE.getTableConfig(_extraRealtimeTableConfig.getTableName()));
    return logicalTableConfig;
  }
}
