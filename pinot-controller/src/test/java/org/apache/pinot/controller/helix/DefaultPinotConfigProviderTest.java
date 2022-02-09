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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.provider.DefaultPinotConfigProvider;
import org.apache.pinot.controller.ControllerTestUtils;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class DefaultPinotConfigProviderTest {
  private static final String SCHEMA_NAME = "cacheTestSchema";
  private static final String RAW_TABLE_NAME = "cacheTestTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  private static final String MANGLED_RAW_TABLE_NAME = "cAcHeTeStTaBlE";
  private static final String MANGLED_OFFLINE_TABLE_NAME = MANGLED_RAW_TABLE_NAME + "_oFfLiNe";

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testDefaultPinotConfigProvider()
      throws Exception {
    DefaultPinotConfigProvider defaultPinotConfigProvider =
        new DefaultPinotConfigProvider(ControllerTestUtils.getPropertyStore(), true);

    assertNull(defaultPinotConfigProvider.getSchema(SCHEMA_NAME));
    assertNull(defaultPinotConfigProvider.getColumnNameMap(SCHEMA_NAME));
    assertNull(defaultPinotConfigProvider.getSchema(RAW_TABLE_NAME));
    assertNull(defaultPinotConfigProvider.getColumnNameMap(RAW_TABLE_NAME));
    assertNull(defaultPinotConfigProvider.getTableConfig(OFFLINE_TABLE_NAME));
    assertNull(defaultPinotConfigProvider.getActualTableName(RAW_TABLE_NAME));

    // Add a schema
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(SCHEMA_NAME).addSingleValueDimension("testColumn", DataType.INT)
            .build();
    ControllerTestUtils.getHelixResourceManager().addSchema(schema, false);
    // Wait for at most 10 seconds for the callback to add the schema to the cache
    TestUtils.waitForCondition(aVoid -> defaultPinotConfigProvider.getSchema(SCHEMA_NAME) != null, 10_000L,
        "Failed to add the schema to the cache");
    // Schema can be accessed by the schema name, but not by the table name because table config is not added yet
    Schema expectedSchema =
        new Schema.SchemaBuilder().setSchemaName(SCHEMA_NAME).addSingleValueDimension("testColumn", DataType.INT)
            .addSingleValueDimension(BuiltInVirtualColumn.DOCID, DataType.INT)
            .addSingleValueDimension(BuiltInVirtualColumn.HOSTNAME, DataType.STRING)
            .addSingleValueDimension(BuiltInVirtualColumn.SEGMENTNAME, DataType.STRING).build();
    Map<String, String> expectedColumnMap = new HashMap<>();
    expectedColumnMap.put("testcolumn", "testColumn");
    expectedColumnMap.put("$docid", "$docId");
    expectedColumnMap.put("$hostname", "$hostName");
    expectedColumnMap.put("$segmentname", "$segmentName");
    assertEquals(defaultPinotConfigProvider.getSchema(SCHEMA_NAME), expectedSchema);
    assertEquals(defaultPinotConfigProvider.getColumnNameMap(SCHEMA_NAME), expectedColumnMap);
    assertNull(defaultPinotConfigProvider.getSchema(RAW_TABLE_NAME));
    assertNull(defaultPinotConfigProvider.getColumnNameMap(RAW_TABLE_NAME));
    // Case-insensitive table name are handled based on the table config instead of the schema
    assertNull(defaultPinotConfigProvider.getActualTableName(RAW_TABLE_NAME));
    TestSchemaChangeListener schemaChangeListener = new TestSchemaChangeListener();
    List<Schema> schemas = defaultPinotConfigProvider.registerSchemaChangeListener(schemaChangeListener);
    Assert.assertNotNull(schemas);
    Assert.assertEquals(schemas.size(), 1);
    Assert.assertEquals(schemas.get(0).getSchemaName(), SCHEMA_NAME);

    // Add a table config
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSchemaName(SCHEMA_NAME).build();
    ControllerTestUtils.getHelixResourceManager().addTable(tableConfig);
    // Wait for at most 10 seconds for the callback to add the table config to the cache
    TestUtils.waitForCondition(aVoid -> defaultPinotConfigProvider.getTableConfig(OFFLINE_TABLE_NAME) != null, 10_000L,
        "Failed to add the table config to the cache");
    assertEquals(defaultPinotConfigProvider.getTableConfig(OFFLINE_TABLE_NAME), tableConfig);
    assertEquals(defaultPinotConfigProvider.getActualTableName(MANGLED_RAW_TABLE_NAME), RAW_TABLE_NAME);
    assertEquals(defaultPinotConfigProvider.getActualTableName(MANGLED_OFFLINE_TABLE_NAME), OFFLINE_TABLE_NAME);
    assertNull(defaultPinotConfigProvider.getActualTableName(REALTIME_TABLE_NAME));
    // Schema can be accessed by both the schema name and the raw table name
    assertEquals(defaultPinotConfigProvider.getSchema(SCHEMA_NAME), expectedSchema);
    assertEquals(defaultPinotConfigProvider.getColumnNameMap(SCHEMA_NAME), expectedColumnMap);
    assertEquals(defaultPinotConfigProvider.getSchema(RAW_TABLE_NAME), expectedSchema);
    assertEquals(defaultPinotConfigProvider.getColumnNameMap(RAW_TABLE_NAME), expectedColumnMap);
    TestTableConfigChangeListener tableConfigChangeListener = new TestTableConfigChangeListener();
    List<TableConfig> tableConfigs =
        defaultPinotConfigProvider.registerTableConfigChangeListener(tableConfigChangeListener);
    Assert.assertNotNull(tableConfigs);
    Assert.assertEquals(tableConfigs.size(), 1);
    Assert.assertEquals(tableConfigs.get(0).getTableName(), OFFLINE_TABLE_NAME);

    // Update the schema
    schema.addField(new DimensionFieldSpec("newColumn", DataType.LONG, true));
    ControllerTestUtils.getHelixResourceManager().updateSchema(schema, false);
    // Wait for at most 10 seconds for the callback to update the schema in the cache
    // NOTE: schema should never be null during the transitioning
    expectedSchema.addField(new DimensionFieldSpec("newColumn", DataType.LONG, true));
    TestUtils.waitForCondition(
        aVoid -> Preconditions.checkNotNull(defaultPinotConfigProvider.getSchema(SCHEMA_NAME)).equals(expectedSchema),
        10_000L, "Failed to update the schema in the cache");
    // Schema can be accessed by both the schema name and the raw table name
    expectedColumnMap.put("newcolumn", "newColumn");
    assertEquals(defaultPinotConfigProvider.getColumnNameMap(SCHEMA_NAME), expectedColumnMap);
    assertEquals(defaultPinotConfigProvider.getSchema(RAW_TABLE_NAME), expectedSchema);
    assertEquals(defaultPinotConfigProvider.getColumnNameMap(RAW_TABLE_NAME), expectedColumnMap);
    Assert.assertNotNull(schemaChangeListener._schemaList);
    Assert.assertEquals(schemaChangeListener._schemaList.size(), 1);
    Assert.assertEquals(schemaChangeListener._schemaList.get(0).getSchemaName(), SCHEMA_NAME);

    // Update the table config and drop the schema name
    tableConfig.getValidationConfig().setSchemaName(null);
    ControllerTestUtils.getHelixResourceManager().updateTableConfig(tableConfig);
    // Wait for at most 10 seconds for the callback to update the table config in the cache
    // NOTE: Table config should never be null during the transitioning
    TestUtils.waitForCondition(
        aVoid -> Preconditions.checkNotNull(defaultPinotConfigProvider.getTableConfig(OFFLINE_TABLE_NAME))
            .equals(tableConfig), 10_000L, "Failed to update the table config in the cache");
    assertEquals(defaultPinotConfigProvider.getActualTableName(MANGLED_RAW_TABLE_NAME), RAW_TABLE_NAME);
    assertEquals(defaultPinotConfigProvider.getActualTableName(MANGLED_OFFLINE_TABLE_NAME), OFFLINE_TABLE_NAME);
    assertNull(defaultPinotConfigProvider.getActualTableName(REALTIME_TABLE_NAME));
    // After dropping the schema name from the table config, schema can only be accessed by the schema name, but not by
    // the table name
    assertEquals(defaultPinotConfigProvider.getSchema(SCHEMA_NAME), expectedSchema);
    assertEquals(defaultPinotConfigProvider.getColumnNameMap(SCHEMA_NAME), expectedColumnMap);
    assertNull(defaultPinotConfigProvider.getSchema(RAW_TABLE_NAME));
    assertNull(defaultPinotConfigProvider.getColumnNameMap(RAW_TABLE_NAME));
    Assert.assertNotNull(tableConfigChangeListener._tableConfigList);
    Assert.assertEquals(tableConfigChangeListener._tableConfigList.size(), 1);
    Assert.assertEquals(tableConfigChangeListener._tableConfigList.get(0).getTableName(), OFFLINE_TABLE_NAME);

    // Remove the table config
    ControllerTestUtils.getHelixResourceManager().deleteOfflineTable(RAW_TABLE_NAME);
    // Wait for at most 10 seconds for the callback to remove the table config from the cache
    TestUtils.waitForCondition(aVoid -> defaultPinotConfigProvider.getTableConfig(OFFLINE_TABLE_NAME) == null, 10_000L,
        "Failed to remove the table config from the cache");
    assertNull(defaultPinotConfigProvider.getActualTableName(RAW_TABLE_NAME));
    // After dropping the table config, schema can only be accessed by the schema name, but not by the table name
    assertEquals(defaultPinotConfigProvider.getSchema(SCHEMA_NAME), expectedSchema);
    assertEquals(defaultPinotConfigProvider.getColumnNameMap(SCHEMA_NAME), expectedColumnMap);
    assertNull(defaultPinotConfigProvider.getSchema(RAW_TABLE_NAME));
    assertNull(defaultPinotConfigProvider.getColumnNameMap(RAW_TABLE_NAME));
    Assert.assertNotNull(schemaChangeListener._schemaList);
    Assert.assertEquals(schemaChangeListener._schemaList.size(), 1);
    Assert.assertEquals(tableConfigChangeListener._tableConfigList.size(), 0);

    // Remove the schema
    ControllerTestUtils.getHelixResourceManager().deleteSchema(schema);
    // Wait for at most 10 seconds for the callback to remove the schema from the cache
    TestUtils.waitForCondition(aVoid -> defaultPinotConfigProvider.getSchema(SCHEMA_NAME) == null, 10_000L,
        "Failed to remove the schema from the cache");
    assertNull(defaultPinotConfigProvider.getSchema(SCHEMA_NAME));
    assertNull(defaultPinotConfigProvider.getColumnNameMap(SCHEMA_NAME));
    assertNull(defaultPinotConfigProvider.getSchema(RAW_TABLE_NAME));
    assertNull(defaultPinotConfigProvider.getColumnNameMap(RAW_TABLE_NAME));
    Assert.assertEquals(schemaChangeListener._schemaList.size(), 0);
    Assert.assertEquals(tableConfigChangeListener._tableConfigList.size(), 0);
  }

  private static class TestTableConfigChangeListener implements TableConfigChangeListener {
    private List<TableConfig> _tableConfigList;

    @Override
    public void onChange(List<TableConfig> tableConfigList) {
      _tableConfigList = tableConfigList;
    }
  }

  private static class TestSchemaChangeListener implements SchemaChangeListener {
    private List<Schema> _schemaList;

    @Override
    public void onChange(List<Schema> schemaList) {
      _schemaList = schemaList;
    }
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
