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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.operator.docidsets.ShortCircuitingDocIdSet;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class AndFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_AND";

  private final List<BaseFilterOperator> _filterOperators;
  private final Map<String, String> _queryOptions;

  public AndFilterOperator(List<BaseFilterOperator> filterOperators, @Nullable Map<String, String> queryOptions,
      int numDocs, boolean nullHandlingEnabled) {
    super(numDocs, nullHandlingEnabled);
    _filterOperators = filterOperators;
    _queryOptions = queryOptions;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    Tracing.activeRecording().setNumChildren(_filterOperators.size());
    List<BlockDocIdSet> blockDocIdSets = new ArrayList<>(_filterOperators.size());
    long totalEntriesScanned = 0L;
    for (BaseFilterOperator filterOperator : _filterOperators) {
      BlockDocIdSet blockDocIdSet = filterOperator.getTrues();
      BlockDocIdSet optimizedDocIdSet = blockDocIdSet.getOptimizedDocIdSet();
      totalEntriesScanned += blockDocIdSet.getNumEntriesScannedInFilter();
      if (optimizedDocIdSet instanceof EmptyDocIdSet) {
        return new ShortCircuitingDocIdSet(totalEntriesScanned);
      }
      if (optimizedDocIdSet instanceof MatchAllDocIdSet) {
        continue;
      }
      blockDocIdSets.add(optimizedDocIdSet);
    }
    if (blockDocIdSets.isEmpty()) {
      return new MatchAllDocIdSet(_numDocs);
    }
    return new AndDocIdSet(blockDocIdSets, _queryOptions);
  }

  /// A conjunction is not false where no child is false: the intersection of the children's not-false documents.
  @Override
  protected BlockDocIdSet getNotFalses() {
    List<BlockDocIdSet> notFalses = new ArrayList<>(_filterOperators.size());
    for (BaseFilterOperator filterOperator : _filterOperators) {
      BlockDocIdSet childNotFalses = filterOperator.getNotFalses();
      if (childNotFalses instanceof EmptyDocIdSet) {
        return EmptyDocIdSet.getInstance();
      }
      if (childNotFalses instanceof MatchAllDocIdSet) {
        continue;
      }
      notFalses.add(childNotFalses);
    }
    if (notFalses.isEmpty()) {
      return new MatchAllDocIdSet(_numDocs);
    }
    return notFalses.size() == 1 ? notFalses.get(0) : new AndDocIdSet(notFalses, _queryOptions);
  }

  @Override
  protected BlockDocIdSet getNulls() {
    return mayHaveNulls() ? deriveNulls(_queryOptions) : EmptyDocIdSet.getInstance();
  }

  @Override
  public boolean canOptimizeCount() {
    return canProduceBitmaps();
  }

  @Override
  public int getNumMatchingDocs() {
    if (_filterOperators.size() == 2) {
      return _filterOperators.get(0).getBitmaps().andCardinality(_filterOperators.get(1).getBitmaps());
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[_filterOperators.size()];
    int i = 0;
    for (BaseFilterOperator child : _filterOperators) {
      bitmaps[i++] = child.getBitmaps().reduce();
    }
    return BufferFastAggregation.andCardinality(bitmaps);
  }

  @Override
  public boolean canProduceBitmaps() {
    for (BaseFilterOperator child : _filterOperators) {
      if (!child.canProduceBitmaps()) {
        return false;
      }
    }
    return true;
  }

  /// The true documents are those true for every child. When a child has UNKNOWN documents, so has the result: a
  /// document is UNKNOWN when no child is false for it and some child is UNKNOWN, which is the intersection of the
  /// children's not-false documents minus the intersection of their true ones.
  @Override
  public BitmapCollection getBitmaps() {
    int numChildren = _filterOperators.size();
    ImmutableRoaringBitmap[] trues = new ImmutableRoaringBitmap[numChildren];
    ImmutableRoaringBitmap[] notFalses = null;
    for (int i = 0; i < numChildren; i++) {
      BitmapCollection childBitmaps = _filterOperators.get(i).getBitmaps();
      trues[i] = childBitmaps.reduce();
      ImmutableRoaringBitmap childNulls = childBitmaps.getNullBitmap();
      if (childNulls != null) {
        if (notFalses == null) {
          notFalses = Arrays.copyOf(trues, numChildren);
        }
        notFalses[i] = ImmutableRoaringBitmap.or(trues[i], childNulls);
      } else if (notFalses != null) {
        notFalses[i] = trues[i];
      }
    }
    MutableRoaringBitmap andTrues = BufferFastAggregation.and(trues);
    if (notFalses == null) {
      return new BitmapCollection(_numDocs, false, andTrues);
    }
    MutableRoaringBitmap nulls = BufferFastAggregation.and(notFalses);
    nulls.andNot(andTrues);
    return new BitmapCollection(_numDocs, false, andTrues).excludingNulls(nulls);
  }

  @Override
  public boolean mayHaveNulls() {
    if (_nullHandlingEnabled) {
      for (BaseFilterOperator filterOperator : _filterOperators) {
        if (filterOperator.mayHaveNulls()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public List<Operator> getChildOperators() {
    return new ArrayList<>(_filterOperators);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
