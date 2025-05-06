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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server.Upsert;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TableUpsertMetadataManagerFactoryTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension("myCol", DataType.STRING)
      .addDateTimeField("timeCol", DataType.LONG, "TIMESTAMP", "1:MILLISECONDS")
      .setPrimaryKeyColumns(List.of("myCol"))
      .build();

  private TableConfig createTableConfig(UpsertConfig upsertConfig) {
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setTimeColumnName("timeCol")
        .setUpsertConfig(upsertConfig)
        .build();
  }

  @Test
  public void testCreateForDefaultManagerClass() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), createTableConfig(upsertConfig), SCHEMA,
            tableDataManager);
    assertNotNull(tableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager instanceof ConcurrentMapTableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager.getOrCreatePartitionManager(
        0) instanceof ConcurrentMapPartitionUpsertMetadataManager);
  }

  @Test
  public void testCreateForManagerClassWithConsistentDeletes() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    upsertConfig.setDeletedKeysTTL(1000L);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), createTableConfig(upsertConfig), SCHEMA,
            tableDataManager);
    assertNotNull(tableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager instanceof ConcurrentMapTableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager.getOrCreatePartitionManager(
        0) instanceof ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testEnablePreload()
      throws IOException {
    // Test legacy APIs first
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableSnapshot(true);
    upsertConfig.setEnablePreload(true);
    TableConfig tableConfig = createTableConfig(upsertConfig);

    // Preloading is not enabled as there is no preloading thread.
    PinotConfiguration instanceUpsertConfig = new PinotConfiguration();
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    when(tableDataManager.getSegmentPreloadExecutor()).thenReturn(null);
    verifyPreloadEnabled(instanceUpsertConfig, tableConfig, tableDataManager, false);

    // Preloading is enabled if there are threads for preloading and enablePreload and enableSnapshot flags are true.
    tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File(RAW_TABLE_NAME));
    when(tableDataManager.getSegmentPreloadExecutor()).thenReturn(mock(ExecutorService.class));
    for (boolean[] flags : new boolean[][]{
        {true, false}, {false, true}, {false, false}, {true, true}
    }) {
      // NOTE: Need to reset snapshot and preload enablement first
      upsertConfig.setSnapshot(Enablement.DEFAULT);
      upsertConfig.setEnableSnapshot(flags[0]);
      upsertConfig.setPreload(Enablement.DEFAULT);
      upsertConfig.setEnablePreload(flags[1]);
      verifyPreloadEnabled(instanceUpsertConfig, tableConfig, tableDataManager, flags[0] && flags[1]);
    }

    // Disabled via instance level config
    upsertConfig.setSnapshot(Enablement.ENABLE);
    upsertConfig.setPreload(Enablement.DEFAULT);
    verifyPreloadEnabled(instanceUpsertConfig, tableConfig, tableDataManager, false);

    // Enabled via instance level config
    instanceUpsertConfig.setProperty(Upsert.DEFAULT_ENABLE_PRELOAD, true);
    verifyPreloadEnabled(instanceUpsertConfig, tableConfig, tableDataManager, true);

    // Disabled via table level config override
    upsertConfig.setPreload(Enablement.DISABLE);
    verifyPreloadEnabled(instanceUpsertConfig, tableConfig, tableDataManager, false);
  }

  private void verifyPreloadEnabled(PinotConfiguration instanceUpsertConfig, TableConfig tableConfig,
      TableDataManager tableDataManager, boolean expected)
      throws IOException {
    try (TableUpsertMetadataManager tableUpsertMetadataManager = TableUpsertMetadataManagerFactory.create(
        instanceUpsertConfig, tableConfig, SCHEMA, tableDataManager)) {
      assertEquals(tableUpsertMetadataManager.getContext().isPreloadEnabled(), expected);
    }
  }
}
