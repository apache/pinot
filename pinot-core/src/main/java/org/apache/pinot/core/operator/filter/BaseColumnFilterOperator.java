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
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public abstract class BaseColumnFilterOperator extends BaseFilterOperator {
  protected final QueryContext _queryContext;
  protected final DataSource _dataSource;
  @Nullable
  private final ImmutableRoaringBitmap _nullBitmap;

  protected BaseColumnFilterOperator(QueryContext queryContext, DataSource dataSource, int numDocs) {
    super(numDocs, queryContext.isNullHandlingEnabled());
    _queryContext = queryContext;
    _dataSource = dataSource;
    _nullBitmap = _nullHandlingEnabled ? FilterOperatorUtils.getNullBitmap(dataSource) : null;
  }

  protected abstract BlockDocIdSet getNextBlockWithoutNullHandling();

  @Override
  protected BlockDocIdSet getTrues() {
    if (_nullBitmap != null) {
      return excludeNulls(getNextBlockWithoutNullHandling(), _nullBitmap);
    }
    return getNextBlockWithoutNullHandling();
  }

  @Override
  protected BlockDocIdSet getNulls() {
    return _nullBitmap != null ? new BitmapDocIdSet(_nullBitmap, _numDocs) : EmptyDocIdSet.getInstance();
  }

  /// The not-false documents are the ones matching the predicate over the stored values together with the null ones.
  /// The default implementation reads [#getTrues], which takes the null rows out of the match, only to put them back.
  @Override
  protected BlockDocIdSet getNotFalses() {
    BlockDocIdSet matches = getNextBlockWithoutNullHandling();
    if (_nullBitmap == null) {
      return matches;
    }
    return new OrDocIdSet(List.of(matches, new BitmapDocIdSet(_nullBitmap, _numDocs)), _numDocs);
  }

  @Override
  public boolean mayHaveNulls() {
    return _nullBitmap != null;
  }

  /// Returns the documents the predicate is UNKNOWN for, or `null` when there is none: null handling is disabled, or
  /// the column has no null row. The bitmap is read once, at construction, so that every view of the operator agrees
  /// on the same documents even on a consuming segment, whose vector hands out a fresh copy on each read. An
  /// implementation with a count or bitmap shortcut attaches it to its [BitmapCollection] so that those leave the null
  /// documents out too.
  @Nullable
  protected ImmutableRoaringBitmap getNullBitmap() {
    return _nullBitmap;
  }

  /// Returns how many documents are true when the index finds `numMatchingDocs` documents matching the predicate's
  /// values, `numMatchingNulls` of which are null rows and so UNKNOWN rather than true. When `exclusive`, the index
  /// found the documents that do not match, and the true documents are the rest minus the null rows among the rest.
  protected int toNumTrueDocs(int numMatchingDocs, int numMatchingNulls, boolean exclusive) {
    if (exclusive) {
      int numNulls = _nullBitmap != null ? _nullBitmap.getCardinality() : 0;
      return _numDocs - numMatchingDocs - (numNulls - numMatchingNulls);
    }
    return numMatchingDocs - numMatchingNulls;
  }

  private BlockDocIdSet excludeNulls(BlockDocIdSet blockDocIdSet, ImmutableRoaringBitmap nullBitmap) {
    return new AndDocIdSet(List.of(blockDocIdSet,
        new BitmapDocIdSet(ImmutableRoaringBitmap.flip(nullBitmap, 0, (long) _numDocs), _numDocs)),
        _queryContext.getQueryOptions());
  }
}
