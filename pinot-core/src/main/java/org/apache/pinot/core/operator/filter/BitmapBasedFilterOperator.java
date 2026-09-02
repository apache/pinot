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
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Filter over a fixed set of documents.
///
/// `docIds` are the documents the predicate is true for or, when `exclusive`, the documents it is not true for, so
/// that it is true for every other one. Without a null bitmap the predicate is two-valued: the false documents are
/// the complement of the true ones, and [#getNulls] is empty.
///
/// A null bitmap adds the documents the predicate is UNKNOWN for. The true, false and null documents must partition
/// the whole, so the null bitmap has to be disjoint from the true documents: contained in `docIds` when `exclusive`,
/// disjoint from `docIds` otherwise. The caller guarantees it; the operator only asserts it. The two bitmaps are
/// otherwise independent, and [#getNulls] returns the null bitmap as given. The count and the bitmaps leave the null
/// documents out as well, and the bitmaps carry them so that an inversion leaves them out too.
public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_BITMAP";

  private final ImmutableRoaringBitmap _docIds;
  private final boolean _exclusive;
  @Nullable
  private final ImmutableRoaringBitmap _nullBitmap;

  public BitmapBasedFilterOperator(ImmutableRoaringBitmap docIds, boolean exclusive, int numDocs) {
    this(docIds, exclusive, numDocs, null);
  }

  /// Creates a filter over the given documents that also knows the documents the predicate is UNKNOWN for, so that
  /// its complement leaves them out: NOT of UNKNOWN is UNKNOWN, not true.
  ///
  /// `nullBitmap` must be disjoint from the documents the predicate is true for, as described on the class. A
  /// predicate that is true on every non-null document is `(nullBitmap, true, numDocs, nullBitmap)`, and one that is
  /// true on none is `(empty, false, numDocs, nullBitmap)`.
  public BitmapBasedFilterOperator(ImmutableRoaringBitmap docIds, boolean exclusive, int numDocs,
      @Nullable ImmutableRoaringBitmap nullBitmap) {
    super(numDocs, nullBitmap != null);
    assert nullBitmap == null
        || (exclusive
            ? ImmutableRoaringBitmap.andNotCardinality(nullBitmap, docIds) == 0
            : !ImmutableRoaringBitmap.intersects(docIds, nullBitmap))
        : "The null bitmap must be disjoint from the trues";
    _docIds = docIds;
    _exclusive = exclusive;
    _nullBitmap = nullBitmap;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_exclusive) {
      return new BitmapDocIdSet(ImmutableRoaringBitmap.flip(_docIds, 0L, _numDocs), _numDocs);
    } else {
      return new BitmapDocIdSet(_docIds, _numDocs);
    }
  }

  @Override
  protected BlockDocIdSet getNulls() {
    return _nullBitmap != null ? new BitmapDocIdSet(_nullBitmap, _numDocs) : EmptyDocIdSet.getInstance();
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    if (_nullBitmap != null) {
      return getBitmaps().getCardinality();
    }
    int count = _docIds.getCardinality();
    return _exclusive ? _numDocs - count : count;
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, _exclusive, _docIds).excludingNulls(_nullBitmap);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<Operator> getChildOperators() {
    return List.of();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
