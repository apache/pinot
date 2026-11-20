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
package org.apache.pinot.common.config.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.ZkPaths;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Checks the Helix adapter's subscription ordering and delivery into the shared typed cache.
public class ZkTableCacheTest {
  private static final String TABLE = "testTable";
  private static final String OFFLINE_TABLE = TABLE + "_OFFLINE";
  private static final String REALTIME_TABLE = TABLE + "_REALTIME";
  private static final String LOGICAL_TABLE = "logicalTable";
  private static final String TABLE_PARENT = "/CONFIGS/TABLE";
  private static final String SCHEMA_PARENT = "/SCHEMAS";

  @Test(dataProvider = "caseModes")
  // Exercise the adapter's existing bulk-read overload to preserve its subscription/read ordering.
  @SuppressWarnings({"unchecked", "deprecation"})
  public void testHydrationAndChangeCallbacks(boolean ignoreCase)
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    TableConfig offline = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build();
    TableConfig realtime = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension("testColumn", DataType.INT).build();
    LogicalTableConfig logical = new LogicalTableConfig();
    logical.setTableName(LOGICAL_TABLE);
    logical.setBrokerTenant("DefaultTenant");
    logical.setPhysicalTableConfigMap(Map.of());
    List<String> tablePaths = List.of(TABLE_PARENT + "/" + OFFLINE_TABLE, TABLE_PARENT + "/" + REALTIME_TABLE);
    List<String> schemaPaths = List.of(SCHEMA_PARENT + "/" + TABLE);
    List<String> logicalPaths = List.of(ZkPaths.LOGICAL_TABLE_PATH_PREFIX + LOGICAL_TABLE);
    when(propertyStore.getChildNames(TABLE_PARENT, AccessOption.PERSISTENT))
        .thenReturn(List.of(OFFLINE_TABLE, REALTIME_TABLE));
    when(propertyStore.get(tablePaths, null, AccessOption.PERSISTENT))
        .thenReturn(List.of(TableConfigSerDeUtils.toZNRecord(offline), TableConfigSerDeUtils.toZNRecord(realtime)));
    when(propertyStore.getChildNames(SCHEMA_PARENT, AccessOption.PERSISTENT)).thenReturn(List.of(TABLE));
    when(propertyStore.get(schemaPaths, null, AccessOption.PERSISTENT))
        .thenReturn(List.of(SchemaSerDeUtils.toZNRecord(schema)));
    when(propertyStore.getChildNames(ZkPaths.LOGICAL_TABLE_PARENT_PATH, AccessOption.PERSISTENT))
        .thenReturn(List.of(LOGICAL_TABLE));
    when(propertyStore.get(logicalPaths, null, AccessOption.PERSISTENT))
        .thenReturn(List.of(LogicalTableConfigUtils.toZNRecord(logical)));

    TableCache cache = new ZkTableCache(propertyStore, ignoreCase);
    IZkDataListener tableListener = verifyHydration(propertyStore, TABLE_PARENT, tablePaths);
    IZkDataListener schemaListener = verifyHydration(propertyStore, SCHEMA_PARENT, schemaPaths);
    IZkDataListener logicalListener = verifyHydration(propertyStore, ZkPaths.LOGICAL_TABLE_PARENT_PATH, logicalPaths);
    assertEquals(cache.getTableConfig(OFFLINE_TABLE), offline);
    assertEquals(cache.getTableConfig(REALTIME_TABLE), realtime);
    assertEquals(cache.getActualTableName(ignoreCase ? "TESTTABLE" : TABLE), TABLE);
    assertEquals(cache.getActualLogicalTableName(ignoreCase ? "LOGICALTABLE" : LOGICAL_TABLE), LOGICAL_TABLE);
    assertEquals(cache.getColumnNameMap(TABLE).get(ignoreCase ? "testcolumn" : "testColumn"), "testColumn");
    assertEquals(cache.getColumnNameMap(TABLE).get(ignoreCase ? "$docid" : "$docId"), "$docId");
    assertEquals(cache.getLogicalTableConfig(LOGICAL_TABLE), logical);

    List<List<TableConfig>> tableChanges = new ArrayList<>();
    assertTrue(cache.registerTableConfigChangeListener(configs -> {
      configs.forEach(config -> assertSame(cache.getTableConfig(config.getTableName()), config));
      assertEquals(cache.getTableNameMap().isEmpty(), configs.isEmpty());
      tableChanges.add(configs);
    }));
    List<List<Schema>> schemaChanges = new ArrayList<>();
    assertTrue(cache.registerSchemaChangeListener(schemas -> {
      schemas.forEach(value -> assertSame(cache.getSchema(value.getSchemaName()), value));
      assertEquals(cache.getColumnNameMap(TABLE) == null, schemas.isEmpty());
      schemaChanges.add(schemas);
    }));
    List<List<LogicalTableConfig>> logicalChanges = new ArrayList<>();
    assertTrue(cache.registerLogicalTableConfigChangeListener(configs -> {
      configs.forEach(config -> assertSame(cache.getLogicalTableConfig(config.getTableName()), config));
      assertEquals(cache.getLogicalTableNameMap().isEmpty(), configs.isEmpty());
      logicalChanges.add(configs);
    }));
    assertEquals(tableChanges.get(0).size(), 2);
    assertEquals(schemaChanges.get(0), List.of(cache.getSchema(TABLE)));
    assertEquals(logicalChanges.get(0), List.of(logical));

    TableConfig updatedOffline = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE)
        .setIsDimTable(true).build();
    tableListener.handleDataChange(tablePaths.get(0), TableConfigSerDeUtils.toZNRecord(updatedOffline));
    assertEquals(cache.getAllDimensionTables(), List.of(OFFLINE_TABLE));
    schema.addField(new DimensionFieldSpec("newColumn", DataType.LONG, true));
    schemaListener.handleDataChange(schemaPaths.get(0), SchemaSerDeUtils.toZNRecord(schema));
    assertEquals(cache.getColumnNameMap(TABLE).get(ignoreCase ? "newcolumn" : "newColumn"), "newColumn");
    logical.setBrokerTenant("UpdatedTenant");
    logicalListener.handleDataChange(logicalPaths.get(0), LogicalTableConfigUtils.toZNRecord(logical));
    assertEquals(logicalChanges.get(1), List.of(logical));

    // Delete notifications contain absolute paths; subscriptions use paths relative to the property store.
    tableListener.handleDataDeleted("/cluster/PROPERTYSTORE" + tablePaths.get(0));
    assertNull(cache.getTableConfig(OFFLINE_TABLE));
    assertEquals(cache.getActualTableName(ignoreCase ? "TESTTABLE" : TABLE), TABLE);
    assertEquals(tableChanges.get(2), List.of(realtime));
    tableListener.handleDataDeleted("/cluster/PROPERTYSTORE" + tablePaths.get(1));
    assertNull(cache.getActualTableName(TABLE));
    assertTrue(tableChanges.get(3).isEmpty());
    schemaListener.handleDataDeleted("/cluster/PROPERTYSTORE" + schemaPaths.get(0));
    logicalListener.handleDataDeleted("/cluster/PROPERTYSTORE" + logicalPaths.get(0));
    assertNull(cache.getSchema(TABLE));
    assertNull(cache.getLogicalTableConfig(LOGICAL_TABLE));
    assertTrue(schemaChanges.get(2).isEmpty());
    assertTrue(logicalChanges.get(2).isEmpty());
    for (String path : tablePaths) {
      verify(propertyStore).unsubscribeDataChanges(path, tableListener);
    }
    verify(propertyStore).unsubscribeDataChanges(schemaPaths.get(0), schemaListener);
    verify(propertyStore).unsubscribeDataChanges(logicalPaths.get(0), logicalListener);
  }

  // Verify the legacy bulk-read overload used by the production Helix adapter.
  @SuppressWarnings("deprecation")
  private static IZkDataListener verifyHydration(ZkHelixPropertyStore<ZNRecord> propertyStore, String parent,
      List<String> paths) {
    InOrder order = inOrder(propertyStore);
    order.verify(propertyStore).subscribeChildChanges(eq(parent), any(IZkChildListener.class));
    order.verify(propertyStore).getChildNames(parent, AccessOption.PERSISTENT);
    ArgumentCaptor<IZkDataListener> listener = ArgumentCaptor.forClass(IZkDataListener.class);
    for (String path : paths) {
      order.verify(propertyStore).subscribeDataChanges(eq(path), listener.capture());
    }
    order.verify(propertyStore).get(paths, null, AccessOption.PERSISTENT);
    return listener.getValue();
  }

  @DataProvider
  public Object[][] caseModes() {
    return new Object[][]{{false}, {true}};
  }
}
