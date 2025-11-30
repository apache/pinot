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
package org.apache.pinot.systemtable.provider;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.segment.spi.IndexSegment;
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
    IndexSegment segment = provider.getDataSource();
    try {
      assertEquals(segment.getSegmentMetadata().getTotalDocs(), 1);
      assertEquals(segment.getValue(0, "tableName"), "mytable");
      assertEquals(segment.getValue(0, "type"), "OFFLINE");
      assertEquals(segment.getValue(0, "status"), "ONLINE");
      assertEquals(segment.getValue(0, "segments"), 2);
      assertEquals(segment.getValue(0, "totalDocs"), 30L);
      assertEquals(segment.getValue(0, "reportedSizeBytes"), 1234L);
      assertEquals(segment.getValue(0, "estimatedSizeBytes"), 2000L);
      assertEquals(segment.getValue(0, "brokerTenant"), "defaultTenant");
      assertEquals(segment.getValue(0, "serverTenant"), "defaultTenant");
      assertEquals(segment.getValue(0, "replicas"), 1);
      assertTrue(((String) segment.getValue(0, "tableConfig")).contains("mytable"));
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testSizeCacheAvoidsRepeatedFetcherCalls() {
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getTableNameMap()).thenReturn(Map.of("mytable_offline", "mytable_OFFLINE"));
    when(tableCache.getTableConfig("mytable_OFFLINE"))
        .thenReturn(new TableConfigBuilder(TableType.OFFLINE).setTableName("mytable").build());

    TablesSystemTableProvider.TableSize tableSize = tableSizeWithOfflineSegments(Map.of("seg1", 10L));
    AtomicInteger calls = new AtomicInteger();
    Function<String, TablesSystemTableProvider.TableSize> fetcher = tableName -> {
      calls.incrementAndGet();
      return tableSize;
    };

    TablesSystemTableProvider provider = new TablesSystemTableProvider(tableCache, null, null, fetcher, null);
    IndexSegment segment1 = provider.getDataSource();
    try {
      segment1.getValue(0, "segments");
    } finally {
      segment1.destroy();
    }

    IndexSegment segment2 = provider.getDataSource();
    try {
      segment2.getValue(0, "segments");
    } finally {
      segment2.destroy();
    }
    assertEquals(calls.get(), 1);
  }

  private static TablesSystemTableProvider.TableSize tableSizeWithOfflineSegments(Map<String, Long> segmentDocs) {
    TablesSystemTableProvider.TableSubType subType = new TablesSystemTableProvider.TableSubType();
    Map<String, TablesSystemTableProvider.SegmentSize> segments = new java.util.HashMap<>();
    for (Map.Entry<String, Long> entry : segmentDocs.entrySet()) {
      TablesSystemTableProvider.SegmentSize seg = new TablesSystemTableProvider.SegmentSize();
      seg._totalDocs = entry.getValue();
      segments.put(entry.getKey(), seg);
    }
    subType._segments = segments;
    TablesSystemTableProvider.TableSize tableSize = new TablesSystemTableProvider.TableSize();
    tableSize._offlineSegments = subType;
    return tableSize;
  }
}
