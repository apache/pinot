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
package org.apache.pinot.core.operator.filter;

import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// Unit tests for [RowLevelSecurityFilterOperator].
public class RowLevelSecurityFilterOperatorTest {
  private static final int NUM_DOCS = 10;

  /// A mutable index can hold documents added after the plan read the number of documents. The RLS bitmap must not
  /// include them.
  @Test
  public void testBoundedByPlanNumDocs() {
    // The RLS operator sees 20 documents, as a mutable index does after more rows arrive
    BaseFilterOperator rowFilter = new BitmapBasedFilterOperator(bitmap(1, 3, 12, 15), false, 20);
    assertEquals(trues(newOperator(rowFilter, new MatchAllFilterOperator(NUM_DOCS), null)), bitmap(1, 3));
  }

  /// Under upsert, the RLS bitmap is ANDed with the valid-document snapshot.
  @Test
  public void testAndWithValidDocIds() {
    BaseFilterOperator rowFilter = new BitmapBasedFilterOperator(bitmap(1, 3, 5, 7), false, NUM_DOCS);
    BaseFilterOperator userFilter = new BitmapBasedFilterOperator(bitmap(1, 2, 3, 5), false, NUM_DOCS);
    assertEquals(trues(newOperator(rowFilter, userFilter, bitmap(3, 5, 6))), bitmap(3, 5));
  }

  /// The RLS bitmap is materialized once, also when the operator is read once per filtered aggregation.
  @Test
  public void testRowFilterMaterializedOnce() {
    AtomicInteger numMaterializations = new AtomicInteger();
    BaseFilterOperator rowFilter = new BitmapBasedFilterOperator(bitmap(1, 3, 5), false, NUM_DOCS) {
      @Override
      public BitmapCollection getBitmaps() {
        numMaterializations.incrementAndGet();
        return super.getBitmaps();
      }
    };
    BaseFilterOperator userFilter = new BitmapBasedFilterOperator(bitmap(3, 5, 7), false, NUM_DOCS);
    RowLevelSecurityFilterOperator operator = newOperator(rowFilter, userFilter, null);
    assertEquals(trues(operator), bitmap(3, 5));
    assertEquals(trues(operator), bitmap(3, 5));
    assertEquals(numMaterializations.get(), 1);
  }

  /// The operator offers no short cut that skips the RLS bitmap.
  @Test
  public void testNoShortCuts() {
    BaseFilterOperator rowFilter = new BitmapBasedFilterOperator(bitmap(1, 3), false, NUM_DOCS);
    RowLevelSecurityFilterOperator operator = newOperator(rowFilter, new MatchAllFilterOperator(NUM_DOCS), null);
    assertFalse(operator.canOptimizeCount());
    assertFalse(operator.canProduceBitmaps());
    assertFalse(operator.isResultMatchingAll());
    assertFalse(operator.isResultEmpty());
  }

  /// The RLS bitmap does not count in `numEntriesScannedInFilter`, and its time goes to the query context.
  @Test
  public void testStats() {
    QueryContext queryContext = spy(QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable"));
    BaseFilterOperator rowFilter = new BitmapBasedFilterOperator(bitmap(1, 3), false, NUM_DOCS);
    RowLevelSecurityFilterOperator operator =
        new RowLevelSecurityFilterOperator(rowFilter, new MatchAllFilterOperator(NUM_DOCS), null, queryContext,
            NUM_DOCS);
    BlockDocIdSet docIdSet = operator.nextBlock().getBlockDocIdSet();
    assertEquals(docIdSet.getNumEntriesScannedInFilter(), 0L);
    verify(queryContext, times(1)).addRowFilterTimeNs(anyLong());
  }

  private static RowLevelSecurityFilterOperator newOperator(BaseFilterOperator rowFilter,
      BaseFilterOperator userFilter, @Nullable ImmutableRoaringBitmap validDocIds) {
    return new RowLevelSecurityFilterOperator(rowFilter, userFilter, validDocIds,
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable"), NUM_DOCS);
  }

  private static MutableRoaringBitmap trues(BaseFilterOperator operator) {
    return BlockDocIdSet.collect(operator.nextBlock().getBlockDocIdSet().iterator());
  }

  private static MutableRoaringBitmap bitmap(int... docIds) {
    return MutableRoaringBitmap.bitmapOf(docIds);
  }
}
