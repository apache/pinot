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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.operator.docidsets.NotDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// The [BaseFilterOperator] class is the base class for all filter operators.
public abstract class BaseFilterOperator extends BaseOperator<FilterBlock> {
  protected final int _numDocs;
  protected final boolean _nullHandlingEnabled;
  @Nullable
  private FilteredDocIds _filteredDocIds;

  public BaseFilterOperator(int numDocs, boolean nullHandlingEnabled) {
    _numDocs = numDocs;
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  /// Returns `true` if the result is always empty, `false` otherwise.
  public boolean isResultEmpty() {
    return false;
  }

  /// Returns `true` if the result matches all the records, `false` otherwise.
  public boolean isResultMatchingAll() {
    return false;
  }

  /// Returns `true` if the filter has an optimized count implementation.
  public boolean canOptimizeCount() {
    return false;
  }

  /// @return the number of matching docs, or throws if it cannot produce this count.
  public int getNumMatchingDocs() {
    throw new UnsupportedOperationException();
  }

  /// @return true if the filter operator can produce a bitmap of docIds
  public boolean canProduceBitmaps() {
    return false;
  }

  /// Returns the true documents as bitmaps. The collection leaves out the documents the filter is UNKNOWN for and
  /// carries them, so that its inversion leaves them out as well.
  public BitmapCollection getBitmaps() {
    throw new UnsupportedOperationException();
  }

  /// Returns whether some documents may be UNKNOWN to this filter, that is whether [#getNulls] may be non-empty. The
  /// answer comes from metadata rather than from evaluating the filter, so it may be pessimistic, and it is `false`
  /// whenever null handling is disabled, since every document then reads as a value. A parent that derives its result
  /// from the two-valued shortcuts alone, such as a negation counting the complement, consults this before doing so.
  public boolean mayHaveNulls() {
    return _nullHandlingEnabled;
  }

  /// Exact filtered docIds for the operator. `null` indicates match-all.
  public static final class FilteredDocIds {
    @Nullable
    private final ImmutableRoaringBitmap _docIds;
    private final long _numEntriesScannedInFilter;

    private FilteredDocIds(@Nullable ImmutableRoaringBitmap docIds, long numEntriesScannedInFilter) {
      _docIds = docIds;
      _numEntriesScannedInFilter = numEntriesScannedInFilter;
    }

    @Nullable
    public ImmutableRoaringBitmap getDocIds() {
      return _docIds;
    }

    public long getNumEntriesScannedInFilter() {
      return _numEntriesScannedInFilter;
    }
  }

  /// Returns the exact filtered docIds for the operator. Implementations that cannot produce a bitmap directly are
  /// materialized once through the filter operator itself so callers can reuse the same primitive.
  public FilteredDocIds getFilteredDocIds() {
    if (_filteredDocIds != null) {
      return _filteredDocIds;
    }

    if (isResultMatchingAll()) {
      _filteredDocIds = new FilteredDocIds(null, 0L);
    } else if (isResultEmpty()) {
      _filteredDocIds = new FilteredDocIds(new MutableRoaringBitmap(), 0L);
    } else if (canProduceBitmaps()) {
      _filteredDocIds = new FilteredDocIds(getBitmaps().reduce(), 0L);
    } else {
      FilterBlock filterBlock = nextBlock();
      BlockDocIdSet blockDocIdSet = filterBlock.getBlockDocIdSet();
      BlockDocIdSet nonScanBlockDocIdSet = filterBlock.getNonScanFilterBLockDocIdSet();
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      BlockDocIdIterator iterator = nonScanBlockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        bitmap.add(docId);
      }
      // Compact the materialized bitmap so repeated downstream set operations remain efficient.
      bitmap.runOptimize();
      _filteredDocIds = new FilteredDocIds(bitmap, blockDocIdSet.getNumEntriesScannedInFilter());
    }
    return _filteredDocIds;
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(getTrues());
  }

  /// @return document IDs in which the predicate evaluates to true.
  protected abstract BlockDocIdSet getTrues();

  /// @return document IDs in which the predicate evaluates to NULL.
  protected BlockDocIdSet getNulls() {
    return EmptyDocIdSet.getInstance();
  }

  /// Returns the document IDs in which the predicate does not evaluate to false: the true ones and, with null
  /// handling enabled, the NULL ones. The result is a [MatchAllDocIdSet] when the predicate is true everywhere and an
  /// [EmptyDocIdSet] when it is false everywhere, so that a parent can short-circuit on either.
  protected BlockDocIdSet getNotFalses() {
    BlockDocIdSet trues = getTrues();
    if (!_nullHandlingEnabled || trues instanceof MatchAllDocIdSet) {
      return trues;
    }
    BlockDocIdSet nulls = getNulls();
    if (nulls instanceof EmptyDocIdSet) {
      return trues;
    }
    return trues instanceof EmptyDocIdSet ? nulls : new OrDocIdSet(List.of(trues, nulls), _numDocs);
  }

  /// Returns the document IDs in which the predicate evaluates to NULL, derived as the ones that are neither false
  /// nor true.
  ///
  /// Only for operators that override [#getNotFalses]. The default [#getNotFalses] reads [#getNulls], so an operator
  /// that keeps it would recurse; a leaf returns its UNKNOWN documents directly instead.
  protected BlockDocIdSet deriveNulls(@Nullable Map<String, String> queryOptions) {
    BlockDocIdSet notFalses = getNotFalses().getOptimizedDocIdSet();
    if (notFalses instanceof EmptyDocIdSet) {
      return EmptyDocIdSet.getInstance();
    }
    BlockDocIdSet trues = getTrues().getOptimizedDocIdSet();
    if (trues instanceof MatchAllDocIdSet) {
      return EmptyDocIdSet.getInstance();
    }
    if (trues instanceof EmptyDocIdSet) {
      return notFalses;
    }
    BlockDocIdSet notTrues = new NotDocIdSet(trues, _numDocs);
    return notFalses instanceof MatchAllDocIdSet ? notTrues
        : new AndDocIdSet(List.of(notFalses, notTrues), queryOptions);
  }

  /// @return document IDs in which the predicate evaluates to false.
  protected BlockDocIdSet getFalses() {
    BlockDocIdSet notFalses = getNotFalses();
    if (notFalses instanceof MatchAllDocIdSet) {
      return EmptyDocIdSet.getInstance();
    }
    if (notFalses instanceof EmptyDocIdSet) {
      return new MatchAllDocIdSet(_numDocs);
    }
    return new NotDocIdSet(notFalses, _numDocs);
  }
}
