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
package org.apache.pinot.segment.local.dedup;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;


public class TableDedupMetadataManagerFactoryTest {
  @Test
  public void testEnablePreload() {
    DedupConfig dedupConfig =
        new DedupConfig(true, HashFunction.MD5, null, Collections.emptyMap(), 10, "timeCol", true);
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName("mytable").addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("mytable").setDedupConfig(dedupConfig).build();

    // Preloading is not enabled as there is no preloading thread.
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File("mytable"));
    when(tableDataManager.getSegmentPreloadExecutor()).thenReturn(null);
    TableDedupMetadataManager tableDedupMetadataManager =
        TableDedupMetadataManagerFactory.create(tableConfig, schema, tableDataManager, null, null);
    assertNotNull(tableDedupMetadataManager);
    assertFalse(tableDedupMetadataManager.isEnablePreload());

    // Enabled as enablePreload is true and there is preloading thread.
    tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(new File("mytable"));
    when(tableDataManager.getSegmentPreloadExecutor()).thenReturn(mock(ExecutorService.class));
    tableDedupMetadataManager = TableDedupMetadataManagerFactory.create(tableConfig, schema, tableDataManager, null,
        null);
    assertNotNull(tableDedupMetadataManager);
  }
}
