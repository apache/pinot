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
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.ThreadSafeMutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class SelectionQuerySegmentPrunerTest {
  public static final String ORDER_BY_COLUMN = "testColumn";

  private final SelectionQuerySegmentPruner _segmentPruner = new SelectionQuerySegmentPruner();

  @Test
  public void testLimit0() {
    List<IndexSegment> indexSegments =
        Arrays.asList(getIndexSegment(null, null, 10), getIndexSegment(0L, 10L, 10), getIndexSegment(-5L, 5L, 15));

    // Should keep only the first segment
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable LIMIT 0");
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));

    queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn LIMIT 0");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));
  }

  @Test
  public void testSelectionOnly() {
    List<IndexSegment> indexSegments =
        Arrays.asList(getIndexSegment(null, null, 10), getIndexSegment(0L, 10L, 10), getIndexSegment(-5L, 5L, 15));

    // Should keep enough documents to fulfill the LIMIT requirement
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable LIMIT 5");
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));

    queryContext = QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable LIMIT 10");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 1);
    assertSame(result.get(0), indexSegments.get(0));

    queryContext = QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable LIMIT 15");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 2);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));

    queryContext = QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable LIMIT 25");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));
    assertSame(result.get(2), indexSegments.get(2));

    queryContext = QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable LIMIT 100");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), indexSegments.get(0));
    assertSame(result.get(1), indexSegments.get(1));
    assertSame(result.get(2), indexSegments.get(2));
  }

  @Test
  public void testSelectionOrderBy() {
    List<IndexSegment> indexSegments = Arrays.asList(
        getIndexSegment(0L, 10L, 10),     // 0
        getIndexSegment(-5L, 5L, 15),     // 1
        getIndexSegment(15L, 50L, 30),    // 2
        getIndexSegment(5L, 15L, 20),     // 3
        getIndexSegment(20L, 30L, 5),     // 4
        getIndexSegment(null, null, 5),   // 5
        getIndexSegment(5L, 10L, 10),     // 6
        getIndexSegment(15L, 30L, 15));   // 7

    // Should keep segments: [null, null], [-5, 5], [0, 10]
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn LIMIT 5");
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(1));  // [-5, 5], 15
    assertSame(result.get(2), indexSegments.get(0));  // [0, 10], 10

    // Should keep segments: [null, null], [-5, 5], [0, 10], [5, 10], [5, 15]
    queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn LIMIT 15, 20");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 5);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(1));  // [-5, 5], 15
    // [0, 10], 10 & [5, 10], 10
    assertTrue(result.get(2) == indexSegments.get(0) || result.get(2) == indexSegments.get(6));
    assertTrue(result.get(3) == indexSegments.get(0) || result.get(3) == indexSegments.get(6));
    assertSame(result.get(4), indexSegments.get(3));  // [5, 15], 20

    // Should keep segments: [null, null], [-5, 5], [0, 10], [5, 10], [5, 15], [15, 30], [15, 50]
    queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn, foo LIMIT 40");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 7);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(1));  // [-5, 5], 15
    // [0, 10], 10 & [5, 10], 10
    assertTrue(result.get(2) == indexSegments.get(0) || result.get(2) == indexSegments.get(6));
    assertTrue(result.get(3) == indexSegments.get(0) || result.get(3) == indexSegments.get(6));
    assertSame(result.get(4), indexSegments.get(3));  // [5, 15], 20
    assertSame(result.get(5), indexSegments.get(7));  // [15, 30], 15
    assertSame(result.get(6), indexSegments.get(2));  // [15, 50], 30

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30]
    queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn DESC LIMIT 5");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 4);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == indexSegments.get(2) || result.get(2) == indexSegments.get(7));
    assertTrue(result.get(3) == indexSegments.get(2) || result.get(3) == indexSegments.get(7));

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30]
    queryContext = QueryContextConverterUtils
        .getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn DESC LIMIT 5, 30");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 4);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == indexSegments.get(2) || result.get(2) == indexSegments.get(7));
    assertTrue(result.get(3) == indexSegments.get(2) || result.get(3) == indexSegments.get(7));

    // Should keep segments: [null, null], [20, 30], [15, 50], [15, 30], [5, 15], [5, 10], [0, 10], [-5, 5]
    queryContext = QueryContextConverterUtils
        .getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn DESC, foo LIMIT 60");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 8);
    assertSame(result.get(0), indexSegments.get(5));  // [null, null], 5
    assertSame(result.get(1), indexSegments.get(4));  // [20, 30], 5
    // [15, 50], 30 & [15, 30], 15
    assertTrue(result.get(2) == indexSegments.get(2) || result.get(2) == indexSegments.get(7));
    assertTrue(result.get(3) == indexSegments.get(2) || result.get(3) == indexSegments.get(7));
    // [5, 15], 20 & [5, 10], 10
    assertTrue(result.get(4) == indexSegments.get(3) || result.get(4) == indexSegments.get(6));
    assertTrue(result.get(5) == indexSegments.get(3) || result.get(5) == indexSegments.get(6));
    assertSame(result.get(6), indexSegments.get(0));  // [0, 10], 10
    assertSame(result.get(7), indexSegments.get(1));  // [-5, 5], 15
  }

  @Test
  public void testUpsertTable() {
    List<IndexSegment> indexSegments = Arrays
        .asList(getIndexSegment(0L, 10L, 10, true), getIndexSegment(20L, 30L, 10, true),
            getIndexSegment(40L, 50L, 10, true));

    // Should not prune any segment for upsert table
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable LIMIT 5");
    List<IndexSegment> result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);

    queryContext =
        QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable ORDER BY testColumn LIMIT 5");
    result = _segmentPruner.prune(indexSegments, queryContext);
    assertEquals(result.size(), 3);
  }

  private IndexSegment getIndexSegment(@Nullable Long minValue, @Nullable Long maxValue, int totalDocs) {
    return getIndexSegment(minValue, maxValue, totalDocs, false);
  }

  private IndexSegment getIndexSegment(@Nullable Long minValue, @Nullable Long maxValue, int totalDocs,
      boolean upsert) {
    IndexSegment indexSegment = mock(IndexSegment.class);
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(ORDER_BY_COLUMN)).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSourceMetadata.getMinValue()).thenReturn(minValue);
    when(dataSourceMetadata.getMaxValue()).thenReturn(maxValue);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);
    if (upsert) {
      ThreadSafeMutableRoaringBitmap validDocIds = mock(ThreadSafeMutableRoaringBitmap.class);
      when(indexSegment.getValidDocIds()).thenReturn(validDocIds);
    }
    return indexSegment;
  }
}
