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
package org.apache.pinot.common.systemtable.provider;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.systemtable.SystemTableRequest;
import org.apache.pinot.spi.systemtable.SystemTableResponse;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TablesSystemTableProviderTest {

  @Test
  public void testAdminFetcherPopulatesSizes() {
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(Map.of("mytable_offline", "mytable_OFFLINE"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("mytable")
        .setBrokerTenant("defaultTenant").setServerTenant("defaultTenant").setNumReplicas(1).build();
    when(tableCache.getTableConfig("mytable_OFFLINE")).thenReturn(tableConfig);

    TablesSystemTableProvider.TableSubType subType = new TablesSystemTableProvider.TableSubType();
    TablesSystemTableProvider.SegmentSize seg1 = new TablesSystemTableProvider.SegmentSize();
    seg1._totalDocs = 10;
    TablesSystemTableProvider.SegmentSize seg2 = new TablesSystemTableProvider.SegmentSize();
    seg2._totalDocs = 20;
    subType._segments = Map.of("seg1", seg1, "seg2", seg2);
    TablesSystemTableProvider.TableSize tableSize = new TablesSystemTableProvider.TableSize();
    tableSize._reportedSizeInBytes = 1234L;
    tableSize._estimatedSizeInBytes = 2000L;
    tableSize._offlineSegments = subType;

    Function<String, TablesSystemTableProvider.TableSize> fetcher = tableName -> tableSize;

    TablesSystemTableProvider provider =
        new TablesSystemTableProvider(tableCache, null, null, fetcher, null);
    SystemTableResponse response = provider.getRows(new SystemTableRequest(List.of(), null, 0, 10));
    assertEquals(response.getRows().size(), 1);
    Map<String, Object> row = response.getRows().get(0).getFieldToValueMap();
    assertEquals(row.get("tableName"), "mytable");
    assertEquals(row.get("type"), "OFFLINE");
    assertEquals(row.get("segments"), 2);
    assertEquals(row.get("totalDocs"), 30L);
    assertEquals(row.get("reportedSize"), 1234L);
    assertEquals(row.get("estimatedSize"), 2000L);
    assertEquals(row.get("brokerTenant"), "defaultTenant");
    assertEquals(row.get("serverTenant"), "defaultTenant");
    assertEquals(row.get("replicas"), 1);
    assertTrue(((String) row.get("tableConfig")).contains("mytable"));
  }
}
