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
package org.apache.pinot.core.query.pruner;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class SelectionQuerySegmentPrunerTest {
  public static final String ORDER_BY_COLUMN = "testColumn";

  private final SelectionQuerySegmentPruner _segmentPruner = new SelectionQuerySegmentPruner();
  private final TableDataManager _tableDataManager = mock(TableDataManager.class);

  @Test
  public void testLimit0() {
    List<SegmentDataManager> segmentDataManagers = Arrays
        .asList(getSegmentDataManager(null, null, 10), getSegmentDataManager(0L, 10L, 10),
            getSegmentDataManager(-5L, 5L, 15));

    // Should keep only the first segment
    ServerQueryRequest queryRequest = getQueryRequest("SELECT * FROM testTable LIMIT 0");
    List<SegmentDataManager> result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), segmentDataManagers.get(0));

    queryRequest = getQueryRequest("SELECT * FROM testTable ORDER BY testColumn LIMIT 0");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), segmentDataManagers.get(0));
  }

  @Test
  public void testSelectionOnly() {
    List<SegmentDataManager> segmentDataManagers = Arrays
        .asList(getSegmentDataManager(null, null, 10), getSegmentDataManager(0L, 10L, 10),
            getSegmentDataManager(-5L, 5L, 15));

    // Should keep enough documents to fulfill the LIMIT requirement
    ServerQueryRequest queryRequest = getQueryRequest("SELECT * FROM testTable LIMIT 5");
    List<SegmentDataManager> result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), segmentDataManagers.get(0));

    queryRequest = getQueryRequest("SELECT * FROM testTable LIMIT 10");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), segmentDataManagers.get(0));

    queryRequest = getQueryRequest("SELECT * FROM testTable LIMIT 15");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 2);
    assertSame(result.get(0), segmentDataManagers.get(0));
    assertSame(result.get(1), segmentDataManagers.get(1));

    queryRequest = getQueryRequest("SELECT * FROM testTable LIMIT 25");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), segmentDataManagers.get(0));
    assertSame(result.get(1), segmentDataManagers.get(1));
    assertSame(result.get(2), segmentDataManagers.get(2));

    queryRequest = getQueryRequest("SELECT * FROM testTable LIMIT 100");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), segmentDataManagers.get(0));
    assertSame(result.get(1), segmentDataManagers.get(1));
    assertSame(result.get(2), segmentDataManagers.get(2));
  }

  @Test
  public void testSelectionOrderBy() {
    List<SegmentDataManager> segmentDataManagers = Arrays.asList( //
        getSegmentDataManager(0L, 10L, 10),     // 0
        getSegmentDataManager(-5L, 5L, 15),     // 1
        getSegmentDataManager(15L, 50L, 30),    // 2
        getSegmentDataManager(5L, 15L, 20),     // 3
        getSegmentDataManager(20L, 30L, 5),     // 4
        getSegmentDataManager(null, null, 5),   // 5
        getSegmentDataManager(5L, 10L, 10),     // 6
        getSegmentDataManager(15L, 30L, 15));   // 7

    // Should keep segments: [null, null], [-5, 5], [0, 10]
    ServerQueryRequest queryRequest = getQueryRequest("SELECT * FROM testTable ORDER BY testColumn LIMIT 5");
    List<SegmentDataManager> result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), segmentDataManagers.get(5));  // [null, null], 5
    assertSame(result.get(1), segmentDataManagers.get(1));  // [-5, 5], 15
    assertSame(result.get(2), segmentDataManagers.get(0));  // [0, 10], 10

    // Should keep segments: [null, null], [-5, 5], [0, 10], [5, 10], [5, 15]
    queryRequest = getQueryRequest("SELECT * FROM testTable ORDER BY testColumn LIMIT 15, 20");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 5);
    assertSame(result.get(0), segmentDataManagers.get(5));  // [null, null], 5
    assertSame(result.get(1), segmentDataManagers.get(1));  // [-5, 5], 15
    // [0, 10], 10 & [5, 10], 10
    assertTrue(result.get(2) == segmentDataManagers.get(0) || result.get(2) == segmentDataManagers.get(6));
    assertTrue(result.get(3) == segmentDataManagers.get(0) || result.get(3) == segmentDataManagers.get(6));
    assertSame(result.get(4), segmentDataManagers.get(3));  // [5, 15], 20

    // Should keep segments: [null, null], [-5, 5], [0, 10], [5, 10], [5, 15], [15, 30], [15, 50]
    queryRequest = getQueryRequest("SELECT * FROM testTable ORDER BY testColumn, foo LIMIT 40");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 7);
    assertSame(result.get(0), segmentDataManagers.get(5));  // [null, null], 5
    assertSame(result.get(1), segmentDataManagers.get(1));  // [-5, 5], 15
    // [0, 10], 10 & [5, 10], 10
    assertTrue(result.get(2) == segmentDataManagers.get(0) || result.get(2) == segmentDataManagers.get(6));
    assertTrue(result.get(3) == segmentDataManagers.get(0) || result.get(3) == segmentDataManagers.get(6));
    assertSame(result.get(4), segmentDataManagers.get(3));  // [5, 15], 20
    assertSame(result.get(5), segmentDataManagers.get(7));  // [15, 30], 15
    assertSame(result.get(6), segmentDataManagers.get(2));  // [15, 50], 30

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30]
    queryRequest = getQueryRequest("SELECT * FROM testTable ORDER BY testColumn DESC LIMIT 5");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 4);
    assertSame(result.get(0), segmentDataManagers.get(5));  // [null, null], 5
    assertSame(result.get(1), segmentDataManagers.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == segmentDataManagers.get(2) || result.get(2) == segmentDataManagers.get(7));
    assertTrue(result.get(3) == segmentDataManagers.get(2) || result.get(3) == segmentDataManagers.get(7));

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30]
    queryRequest = getQueryRequest("SELECT * FROM testTable ORDER BY testColumn DESC LIMIT 5, 30");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 4);
    assertSame(result.get(0), segmentDataManagers.get(5));  // [null, null], 5
    assertSame(result.get(1), segmentDataManagers.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == segmentDataManagers.get(2) || result.get(2) == segmentDataManagers.get(7));
    assertTrue(result.get(3) == segmentDataManagers.get(2) || result.get(3) == segmentDataManagers.get(7));

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30], [5, 15], [5, 10], [0, 10], [-5, 5]
    queryRequest = getQueryRequest("SELECT * FROM testTable ORDER BY testColumn DESC, foo LIMIT 60");
    result = _segmentPruner.prune(_tableDataManager, segmentDataManagers, queryRequest);
    assertEquals(result.size(), 8);
    assertSame(result.get(0), segmentDataManagers.get(5));  // [null, null], 5
    assertSame(result.get(1), segmentDataManagers.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == segmentDataManagers.get(2) || result.get(2) == segmentDataManagers.get(7));
    assertTrue(result.get(3) == segmentDataManagers.get(2) || result.get(3) == segmentDataManagers.get(7));
    // [5, 15], 20 & [5, 10], 10
    assertTrue(result.get(4) == segmentDataManagers.get(3) || result.get(4) == segmentDataManagers.get(6));
    assertTrue(result.get(5) == segmentDataManagers.get(3) || result.get(5) == segmentDataManagers.get(6));
    assertSame(result.get(6), segmentDataManagers.get(0));  // [0, 10], 10
    assertSame(result.get(7), segmentDataManagers.get(1));  // [-5, 5], 15
  }

  private SegmentDataManager getSegmentDataManager(@Nullable Long minValue, @Nullable Long maxValue, int totalDocs) {
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    IndexSegment segment = mock(IndexSegment.class);
    when(segmentDataManager.getSegment()).thenReturn(segment);
    DataSource dataSource = mock(DataSource.class);
    when(segment.getDataSource(ORDER_BY_COLUMN)).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSourceMetadata.getMinValue()).thenReturn(minValue);
    when(dataSourceMetadata.getMaxValue()).thenReturn(maxValue);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    return segmentDataManager;
  }

  private ServerQueryRequest getQueryRequest(String sql) {
    ServerQueryRequest queryRequest = mock(ServerQueryRequest.class);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(sql);
    when(queryRequest.getQueryContext()).thenReturn(queryContext);
    return queryRequest;
  }
}
