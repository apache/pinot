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
package org.apache.pinot.segment.local.upsert;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TableUpsertMetadataManagerFactoryTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private TableConfig _tableConfig;

  @Test
  public void testCreateForDefaultManagerClass() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setHashFunction(HashFunction.NONE);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING).setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig).build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(_tableConfig, null);
    assertNotNull(tableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager instanceof ConcurrentMapTableUpsertMetadataManager);
    tableUpsertMetadataManager.init(_tableConfig, schema, tableDataManager);
    assertTrue(tableUpsertMetadataManager.getOrCreatePartitionManager(
        0) instanceof ConcurrentMapPartitionUpsertMetadataManager);
  }

  @Test
  public void testCreateForManagerClassWithConsistentDeletes() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setHashFunction(HashFunction.NONE);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING).setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig).build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(_tableConfig, null);
    assertNotNull(tableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager instanceof ConcurrentMapTableUpsertMetadataManager);
    tableUpsertMetadataManager.init(_tableConfig, schema, tableDataManager);
    assertTrue(tableUpsertMetadataManager.getOrCreatePartitionManager(
        0) instanceof ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes);
  }

  @Test
  public void testEnablePreload() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setHashFunction(HashFunction.NONE);
    upsertConfig.setEnablePreload(true);
    upsertConfig.setEnableSnapshot(true);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING).setPrimaryKeyColumns(Lists.newArrayList("myCol"))
        .build();
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig).build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(_tableConfig, null);
    assertNotNull(tableUpsertMetadataManager);

    // Preloading is not enabled even if enablePreload and enableSnapshot flags are true, as no threads for preloading.
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    when(tableDataManager.getSegmentPreloadExecutor()).thenReturn(null);
    tableUpsertMetadataManager.init(_tableConfig, schema, tableDataManager);
    assertFalse(tableUpsertMetadataManager.isEnablePreload());

    // Preloading is enabled if there are threads for preloading and enablePreload and enableSnapshot flags are true.
    tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    when(tableDataManager.getSegmentPreloadExecutor()).thenReturn(mock(ExecutorService.class));
    for (boolean[] flags : new boolean[][]{
        {true, false}, {false, true}, {false, false}, {true, true}
    }) {
      upsertConfig.setEnableSnapshot(flags[0]);
      upsertConfig.setEnablePreload(flags[1]);
      _tableConfig =
          new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig).build();
      tableUpsertMetadataManager = TableUpsertMetadataManagerFactory.create(_tableConfig, null);
      tableUpsertMetadataManager.init(_tableConfig, schema, tableDataManager);
      assertEquals(tableUpsertMetadataManager.isEnablePreload(), flags[0] && flags[1],
          String.format("enableSnapshot: %b, enablePreload: %b", flags[0], flags[1]));
    }
  }
}
