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
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
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
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setSelectList(List.of(
        identifier("tableName"), identifier("type"), identifier("status"), identifier("segments"),
        identifier("totalDocs"), identifier("reportedSize"), identifier("estimatedSize"), identifier("storageTier"),
        identifier("brokerTenant"), identifier("serverTenant"), identifier("replicas"), identifier("tableConfig")));
    pinotQuery.setOffset(0);
    pinotQuery.setLimit(10);
    BrokerResponseNative response = provider.getBrokerResponse(pinotQuery);
    assertEquals(response.getResultTable().getRows().size(), 1);
    Object[] values = response.getResultTable().getRows().get(0);
    assertEquals(values[0], "mytable");
    assertEquals(values[1], "OFFLINE");
    assertEquals(values[2], "ONLINE");
    assertEquals(values[3], 2);
    assertEquals(values[4], 30L);
    assertEquals(values[5], 1234L);
    assertEquals(values[6], 2000L);
    assertEquals(values[8], "defaultTenant");
    assertEquals(values[9], "defaultTenant");
    assertEquals(values[10], 1);
    assertTrue(((String) values[11]).contains("mytable"));
  }

  private static org.apache.pinot.common.request.Expression identifier(String name) {
    org.apache.pinot.common.request.Expression expression = new org.apache.pinot.common.request.Expression();
    org.apache.pinot.common.request.Identifier identifier = new org.apache.pinot.common.request.Identifier();
    identifier.setName(name);
    expression.setIdentifier(identifier);
    return expression;
  }
}
