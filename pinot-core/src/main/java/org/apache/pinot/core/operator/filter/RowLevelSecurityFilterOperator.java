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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.AndRestrictionPushdown;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Applies the row-level security (RLS) filter of the query before the user filter.
///
/// On its first block, this operator materializes the result of the RLS filter into a bitmap, bounded by the number of
/// documents that the plan read. It then ANDs that bitmap with the user filter, with the AND restriction push-down on,
/// so the user filter only evaluates scans and transform functions on the documents that the RLS filter matches. The
/// push-down is eager, or streaming for a query that can stop at its LIMIT, see
/// `QueryContext#getAndRestrictionPushdown()`. When
/// the segment has a valid-document snapshot (upsert), the bitmap is also ANDed with that snapshot.
///
/// The bitmap is materialized once and reused for every later block, for example when a filtered aggregation reads
/// this operator once per sub-filter.
///
/// This operator never reports a match-all result, an optimized count, or bitmaps. Then the operators that read the
/// filter result through these short cuts (for example `FastFilteredCountOperator` or
/// `NonScanBasedAggregationOperator`) cannot skip the RLS bitmap.
///
/// Not thread-safe: a segment plan runs on one thread.
public class RowLevelSecurityFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_ROW_LEVEL_SECURITY";

  private final BaseFilterOperator _rowFilterOperator;
  private final BaseFilterOperator _userFilterOperator;
  @Nullable
  private final ImmutableRoaringBitmap _validDocIds;
  private final QueryContext _queryContext;

  @Nullable
  private ImmutableRoaringBitmap _rowFilterDocIds;

  /// @param rowFilterOperator the operator of the RLS filter. It must not be empty or match all documents.
  /// @param userFilterOperator the operator of the WHERE clause. It must not be empty.
  /// @param validDocIds the valid-document snapshot of the segment, or `null` when the segment has none
  /// @param numDocs the number of documents that the plan read
  public RowLevelSecurityFilterOperator(BaseFilterOperator rowFilterOperator, BaseFilterOperator userFilterOperator,
      @Nullable ImmutableRoaringBitmap validDocIds, QueryContext queryContext, int numDocs) {
    super(numDocs, false);
    assert !rowFilterOperator.isResultEmpty() && !rowFilterOperator.isResultMatchingAll()
        && !userFilterOperator.isResultEmpty();
    _rowFilterOperator = rowFilterOperator;
    _userFilterOperator = userFilterOperator;
    _validDocIds = validDocIds;
    _queryContext = queryContext;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    Tracing.activeRecording().setNumChildren(2);
    ImmutableRoaringBitmap rowFilterDocIds = getRowFilterDocIds();
    if (rowFilterDocIds.isEmpty()) {
      return EmptyDocIdSet.unscanned();
    }
    // The cost of the RLS filter is not reported in the numEntriesScannedInFilter of the response: without an index
    // it is the number of documents of the segment, which shows the size of the table to the user. See
    // getRowFilterDocIds() for where it goes instead.
    BlockDocIdSet rowFilterDocIdSet = BitmapDocIdSet.create(rowFilterDocIds, _numDocs);
    if (_userFilterOperator.isResultMatchingAll()) {
      return rowFilterDocIdSet;
    }
    BlockDocIdSet userFilterDocIdSet = _userFilterOperator.nextBlock().getBlockDocIdSet();
    // QueryContext never returns OFF under RLS; keep the RLS invariant even for a context built without a row filter
    AndRestrictionPushdown restrictionPushdown = _queryContext.getAndRestrictionPushdown();
    if (restrictionPushdown == null || !restrictionPushdown.isEnabled()) {
      restrictionPushdown = AndRestrictionPushdown.EAGER;
    }
    return new AndDocIdSet(List.of(rowFilterDocIdSet, userFilterDocIdSet), _queryContext.getQueryOptions(),
        restrictionPushdown);
  }

  /// Returns the documents that the RLS filter matches, ANDed with the valid-document snapshot. Materializes them on
  /// the first call, and adds the time it takes to [QueryContext#addRowFilterTimeNs(long)].
  ///
  /// The entries scanned to materialize them go to the scan cost of the query, so that scan-based query killing sees
  /// them, and to [QueryContext#addRowFilterNumEntriesScanned(long)], for the server query log and metrics.
  private ImmutableRoaringBitmap getRowFilterDocIds() {
    if (_rowFilterDocIds == null) {
      long startTimeNs = System.nanoTime();
      FilteredDocIds filteredDocIds = _rowFilterOperator.getFilteredDocIds();
      ImmutableRoaringBitmap docIds = filteredDocIds.getDocIds();
      long numEntriesScanned = filteredDocIds.getNumEntriesScannedInFilter();
      if (numEntriesScanned > 0) {
        _queryContext.addRowFilterNumEntriesScanned(numEntriesScanned);
        QueryScanCostContext scanCost = getScanCostContext();
        if (scanCost != null) {
          scanCost.addEntriesScannedInFilter(numEntriesScanned);
        }
      }
      assert docIds != null : "The RLS filter operator must not match all documents";
      // A mutable index can hold documents added after the plan read the number of documents. The rest of the plan
      // does not see them, so neither does the RLS filter.
      if (!docIds.isEmpty() && docIds.last() >= _numDocs) {
        MutableRoaringBitmap bounded = docIds.toMutableRoaringBitmap();
        bounded.remove(_numDocs, Integer.toUnsignedLong(docIds.last()) + 1);
        docIds = bounded;
      }
      if (_validDocIds != null) {
        docIds = ImmutableRoaringBitmap.and(docIds, _validDocIds);
      }
      _rowFilterDocIds = docIds;
      _queryContext.addRowFilterTimeNs(System.nanoTime() - startTimeNs);
    }
    return _rowFilterDocIds;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<Operator> getChildOperators() {
    return List.of(_rowFilterOperator, _userFilterOperator);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
