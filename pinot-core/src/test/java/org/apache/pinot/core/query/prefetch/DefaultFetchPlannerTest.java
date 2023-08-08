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
package org.apache.pinot.core.query.prefetch;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class DefaultFetchPlannerTest {
  @Test
  public void testPlanFetchForProcessing() {
    DefaultFetchPlanner planner = new DefaultFetchPlanner();
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getSegmentName()).thenReturn("s0");
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("c0", "c1", "c2"));
    String query = "SELECT COUNT(*) FROM testTable WHERE c0 = 0 OR (c1 < 10 AND c2 IN (1, 2))";
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    FetchContext fetchContext = planner.planFetchForProcessing(indexSegment, queryContext);
    assertEquals(fetchContext.getSegmentName(), "s0");
    Map<String, List<IndexType<?, ?, ?>>> columns = fetchContext.getColumnToIndexList();
    assertEquals(columns.size(), 3);
    // null means to get all index types created for the column.
    assertNull(columns.get("c0"));
    assertNull(columns.get("c1"));
    assertNull(columns.get("c2"));
  }

  @Test
  public void testPlanFetchForPruning() {
    DefaultFetchPlanner planner = new DefaultFetchPlanner();
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getSegmentName()).thenReturn("s0");
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("c0", "c1", "c2"));
    String query = "SELECT COUNT(*) FROM testTable WHERE c0 = 0 OR (c1 < 10 AND c2 IN (1, 2))";
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    // No Bloomfilter for those columns.
    DataSource ds0 = mock(DataSource.class);
    when(indexSegment.getDataSource("c0")).thenReturn(ds0);
    when(ds0.getBloomFilter()).thenReturn(null);
    DataSource ds2 = mock(DataSource.class);
    when(indexSegment.getDataSource("c2")).thenReturn(ds2);
    when(ds2.getBloomFilter()).thenReturn(null);
    FetchContext fetchContext = planner.planFetchForPruning(indexSegment, queryContext);
    assertTrue(fetchContext.isEmpty());

    // Add Bloomfilter for column c0.
    BloomFilterReader bfReader = mock(BloomFilterReader.class);
    when(ds0.getBloomFilter()).thenReturn(bfReader);
    fetchContext = planner.planFetchForPruning(indexSegment, queryContext);
    assertFalse(fetchContext.isEmpty());
    assertEquals(fetchContext.getSegmentName(), "s0");
    Map<String, List<IndexType<?, ?, ?>>> columns = fetchContext.getColumnToIndexList();
    assertEquals(columns.size(), 1);
    List<IndexType<?, ?, ?>> idxTypes = columns.get("c0");
    assertEquals(idxTypes.size(), 1);
    assertEquals(idxTypes.get(0), StandardIndexes.bloomFilter());
  }
}
