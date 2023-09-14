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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TableCacheTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String RAW_TABLE_NAME = "cacheTestTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  private static final String MANGLED_RAW_TABLE_NAME = "cAcHeTeStTaBlE";
  private static final String MANGLED_OFFLINE_TABLE_NAME = MANGLED_RAW_TABLE_NAME + "_oFfLiNe";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test(dataProvider = "testTableCacheDataProvider")
  public void testTableCache(boolean isCaseInsensitive)
      throws Exception {
    TableCache tableCache = new TableCache(TEST_INSTANCE.getPropertyStore(), isCaseInsensitive);

    assertNull(tableCache.getSchema(RAW_TABLE_NAME));
    assertNull(tableCache.getColumnNameMap(RAW_TABLE_NAME));
    assertNull(tableCache.getTableConfig(OFFLINE_TABLE_NAME));
    assertNull(tableCache.getActualTableName(RAW_TABLE_NAME));

    // Add a schema
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension("testColumn", DataType.INT)
            .build();
    TEST_INSTANCE.getHelixResourceManager().addSchema(schema, false, false);
    // Wait for at most 10 seconds for the callback to add the schema to the cache
    TestUtils.waitForCondition(aVoid -> tableCache.getSchema(RAW_TABLE_NAME) != null, 10_000L,
        "Failed to add the schema to the cache");
    // Schema can be accessed by the schema name, but not by the table name because table config is not added yet
    Schema expectedSchema =
        new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension("testColumn", DataType.INT)
            .addSingleValueDimension(BuiltInVirtualColumn.DOCID, DataType.INT)
            .addSingleValueDimension(BuiltInVirtualColumn.HOSTNAME, DataType.STRING)
            .addSingleValueDimension(BuiltInVirtualColumn.SEGMENTNAME, DataType.STRING).build();
    Map<String, String> expectedColumnMap = new HashMap<>();
    expectedColumnMap.put(isCaseInsensitive ? "testcolumn" : "testColumn", "testColumn");
    expectedColumnMap.put(isCaseInsensitive ? "$docid" : "$docId", "$docId");
    expectedColumnMap.put(isCaseInsensitive ? "$hostname" : "$hostName", "$hostName");
    expectedColumnMap.put(isCaseInsensitive ? "$segmentname" : "$segmentName", "$segmentName");
    assertEquals(tableCache.getSchema(RAW_TABLE_NAME), expectedSchema);
    assertEquals(tableCache.getColumnNameMap(RAW_TABLE_NAME), expectedColumnMap);
    // Case-insensitive table name are handled based on the table config instead of the schema
    assertNull(tableCache.getActualTableName(RAW_TABLE_NAME));

    // Add a table config
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    TEST_INSTANCE.waitForEVToDisappear(tableConfig.getTableName());
    TEST_INSTANCE.getHelixResourceManager().addTable(tableConfig);
    // Wait for at most 10 seconds for the callback to add the table config to the cache
    TestUtils.waitForCondition(
        aVoid -> tableConfig.equals(tableCache.getTableConfig(OFFLINE_TABLE_NAME)) && RAW_TABLE_NAME.equals(
            tableCache.getActualTableName(RAW_TABLE_NAME)) && OFFLINE_TABLE_NAME.equals(
            tableCache.getActualTableName(OFFLINE_TABLE_NAME)), 10_000L,
        "Failed to add the table config to the cache");
    // It should only add OFFLINE and normal table.
    assertNull(tableCache.getActualTableName(REALTIME_TABLE_NAME));
    // Schema can be accessed by both the schema name and the raw table name
    assertEquals(tableCache.getSchema(RAW_TABLE_NAME), expectedSchema);
    assertEquals(tableCache.getColumnNameMap(RAW_TABLE_NAME), expectedColumnMap);

    // Register the change listeners
    TestTableConfigChangeListener tableConfigChangeListener = new TestTableConfigChangeListener();
    assertTrue(tableCache.registerTableConfigChangeListener(tableConfigChangeListener));
    assertEquals(tableConfigChangeListener._tableConfigList.size(), 1);
    assertEquals(tableConfigChangeListener._tableConfigList.get(0), tableConfig);
    TestSchemaChangeListener schemaChangeListener = new TestSchemaChangeListener();
    assertTrue(tableCache.registerSchemaChangeListener(schemaChangeListener));
    assertEquals(schemaChangeListener._schemaList.size(), 1);
    assertEquals(schemaChangeListener._schemaList.get(0), expectedSchema);
    // Re-register the change listener should fail
    assertFalse(tableCache.registerTableConfigChangeListener(tableConfigChangeListener));
    assertFalse(tableCache.registerSchemaChangeListener(schemaChangeListener));

    // Update the schema
    schema.addField(new DimensionFieldSpec("newColumn", DataType.LONG, true));
    TEST_INSTANCE.getHelixResourceManager().updateSchema(schema, false, false);
    // Wait for at most 10 seconds for the callback to update the schema in the cache
    // NOTE:
    // - Schema should never be null during the transitioning
    // - Schema change listener callback should always contain 1 schema
    // - Verify if the callback is fully done by checking the schema change lister because it is the last step of the
    //   callback handling
    expectedSchema.addField(new DimensionFieldSpec("newColumn", DataType.LONG, true));
    expectedColumnMap.put(isCaseInsensitive ? "newcolumn" : "newColumn", "newColumn");
    TestUtils.waitForCondition(aVoid -> {
      assertNotNull(tableCache.getSchema(RAW_TABLE_NAME));
      assertEquals(schemaChangeListener._schemaList.size(), 1);
      return schemaChangeListener._schemaList.get(0).equals(expectedSchema);
    }, 10_000L, "Failed to update the schema in the cache");
    // Schema can be accessed by both the schema name and the raw table name
    assertEquals(tableCache.getSchema(RAW_TABLE_NAME), expectedSchema);
    assertEquals(tableCache.getColumnNameMap(RAW_TABLE_NAME), expectedColumnMap);

    TEST_INSTANCE.getHelixResourceManager().updateTableConfig(tableConfig);
    // Wait for at most 10 seconds for the callback to update the table config in the cache
    // NOTE:
    // - Table config should never be null during the transitioning
    // - Table config change listener callback should always contain 1 table config
    // - Verify if the callback is fully done by checking the table config change lister because it is the last step of
    //   the callback handling
    TestUtils.waitForCondition(aVoid -> {
      assertNotNull(tableCache.getTableConfig(OFFLINE_TABLE_NAME));
      assertEquals(tableConfigChangeListener._tableConfigList.size(), 1);
      return tableConfigChangeListener._tableConfigList.get(0).equals(tableConfig);
    }, 10_000L, "Failed to update the table config in the cache");
    // After dropping the schema name from the table config, schema can only be accessed by the schema name, but not by
    // the table name
    assertEquals(tableCache.getTableConfig(OFFLINE_TABLE_NAME), tableConfig);
    assertNotNull(tableCache.getSchema(RAW_TABLE_NAME));
    assertNotNull(tableCache.getColumnNameMap(RAW_TABLE_NAME));
    if (isCaseInsensitive) {
      assertEquals(tableCache.getActualTableName(MANGLED_RAW_TABLE_NAME), RAW_TABLE_NAME);
      assertEquals(tableCache.getActualTableName(MANGLED_OFFLINE_TABLE_NAME), OFFLINE_TABLE_NAME);
    } else {
      assertNull(tableCache.getActualTableName(MANGLED_RAW_TABLE_NAME));
      assertNull(tableCache.getActualTableName(MANGLED_OFFLINE_TABLE_NAME));
      assertEquals(tableCache.getActualTableName(RAW_TABLE_NAME), RAW_TABLE_NAME);
      assertEquals(tableCache.getActualTableName(OFFLINE_TABLE_NAME), OFFLINE_TABLE_NAME);
    }
    assertNull(tableCache.getActualTableName(REALTIME_TABLE_NAME));
    assertEquals(tableCache.getSchema(RAW_TABLE_NAME), expectedSchema);
    assertEquals(tableCache.getColumnNameMap(RAW_TABLE_NAME), expectedColumnMap);

    // Wait for external view to appear before deleting the table to prevent external view being created after the
    // waitForEVToDisappear() call
    TEST_INSTANCE.waitForEVToAppear(OFFLINE_TABLE_NAME);

    // Remove the table config
    TEST_INSTANCE.getHelixResourceManager().deleteOfflineTable(RAW_TABLE_NAME);
    // Wait for at most 10 seconds for the callback to remove the table config from the cache
    // NOTE:
    // - Verify if the callback is fully done by checking the table config change lister because it is the last step of
    //   the callback handling
    TestUtils.waitForCondition(aVoid -> tableConfigChangeListener._tableConfigList.isEmpty(), 10_000L,
        "Failed to remove the table config from the cache");
    assertNull(tableCache.getTableConfig(OFFLINE_TABLE_NAME));
    assertNull(tableCache.getActualTableName(RAW_TABLE_NAME));
    assertEquals(tableCache.getSchema(RAW_TABLE_NAME), expectedSchema);
    assertEquals(tableCache.getColumnNameMap(RAW_TABLE_NAME), expectedColumnMap);

    // Remove the schema
    TEST_INSTANCE.getHelixResourceManager().deleteSchema(RAW_TABLE_NAME);
    // Wait for at most 10 seconds for the callback to remove the schema from the cache
    // NOTE:
    // - Verify if the callback is fully done by checking the schema change lister because it is the last step of the
    //   callback handling
    TestUtils.waitForCondition(aVoid -> schemaChangeListener._schemaList.isEmpty(), 10_000L,
        "Failed to remove the schema from the cache");
    assertNull(tableCache.getSchema(RAW_TABLE_NAME));
    assertNull(tableCache.getColumnNameMap(RAW_TABLE_NAME));
    assertNull(tableCache.getSchema(RAW_TABLE_NAME));
    assertNull(tableCache.getColumnNameMap(RAW_TABLE_NAME));
    assertEquals(schemaChangeListener._schemaList.size(), 0);
    assertEquals(tableConfigChangeListener._tableConfigList.size(), 0);

    // Wait for external view to disappear to ensure a clean start for the next test
    TEST_INSTANCE.waitForEVToDisappear(OFFLINE_TABLE_NAME);
  }

  @DataProvider(name = "testTableCacheDataProvider")
  public Object[][] provideCaseInsensitiveSetting() {
    return new Object[][]{new Object[]{true}, new Object[]{false}};
  }

  private static class TestTableConfigChangeListener implements TableConfigChangeListener {
    private volatile List<TableConfig> _tableConfigList;

    @Override
    public void onChange(List<TableConfig> tableConfigList) {
      _tableConfigList = tableConfigList;
    }
  }

  private static class TestSchemaChangeListener implements SchemaChangeListener {
    private volatile List<Schema> _schemaList;

    @Override
    public void onChange(List<Schema> schemaList) {
      _schemaList = schemaList;
    }
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
