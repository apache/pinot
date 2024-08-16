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

import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TableUpsertMetadataManagerFactoryTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private TableConfig _tableConfig;

  @Test
  public void testCreateForDefaultManagerClass() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setHashFunction(HashFunction.NONE);
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig).build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(_tableConfig, null);
    assertNotNull(tableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager instanceof ConcurrentMapTableUpsertMetadataManager);
  }

  @Test
  public void testCreateForManagerClassWithConsistentDeletes() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setHashFunction(HashFunction.NONE);
    upsertConfig.setEnableDeletedKeysCompactionConsistency(true);
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setUpsertConfig(upsertConfig).build();
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(_tableConfig, null);
    assertNotNull(tableUpsertMetadataManager);
    assertTrue(tableUpsertMetadataManager instanceof BaseTableUpsertMetadataManager);
  }
}
