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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;
import org.apache.pinot.core.operator.docidsets.ShortCircuitingDocIdSet;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class OrFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_OR";

  private final List<BaseFilterOperator> _filterOperators;
  private final Map<String, String> _queryOptions;

  public OrFilterOperator(List<BaseFilterOperator> filterOperators, @Nullable Map<String, String> queryOptions,
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
      if (optimizedDocIdSet instanceof MatchAllDocIdSet) {
        return new MatchAllDocIdSet(_numDocs);
      }
      if (optimizedDocIdSet instanceof EmptyDocIdSet) {
        continue;
      }
      blockDocIdSets.add(optimizedDocIdSet);
    }
    if (blockDocIdSets.isEmpty()) {
      return new ShortCircuitingDocIdSet(totalEntriesScanned);
    }
    return new OrDocIdSet(blockDocIdSets, _numDocs);
  }

  /// A disjunction is not false where some child is not false: the union of the children's not-false documents.
  @Override
  protected BlockDocIdSet getNotFalses() {
    List<BlockDocIdSet> notFalses = new ArrayList<>(_filterOperators.size());
    for (BaseFilterOperator filterOperator : _filterOperators) {
      BlockDocIdSet childNotFalses = filterOperator.getNotFalses();
      if (childNotFalses instanceof MatchAllDocIdSet) {
        return new MatchAllDocIdSet(_numDocs);
      }
      if (childNotFalses instanceof EmptyDocIdSet) {
        continue;
      }
      notFalses.add(childNotFalses);
    }
    if (notFalses.isEmpty()) {
      return EmptyDocIdSet.getInstance();
    }
    return notFalses.size() == 1 ? notFalses.get(0) : new OrDocIdSet(notFalses, _numDocs);
  }

  @Override
  protected BlockDocIdSet getNulls() {
    return mayHaveNulls() ? deriveNulls(_queryOptions) : EmptyDocIdSet.getInstance();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return new ArrayList<>(_filterOperators);
  }

  @Override
  public boolean canOptimizeCount() {
    return canProduceBitmaps();
  }

  @Override
  public int getNumMatchingDocs() {
    if (_filterOperators.size() == 2) {
      return _filterOperators.get(0).getBitmaps().orCardinality(_filterOperators.get(1).getBitmaps());
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[_filterOperators.size()];
    for (int i = 0; i < _filterOperators.size(); i++) {
      bitmaps[i] = _filterOperators.get(i).getBitmaps().reduce();
    }
    return BufferFastAggregation.orCardinality(bitmaps);
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

  /// The true documents are those true for some child. When a child has UNKNOWN documents, so may the result: a
  /// document is UNKNOWN when no child is true for it and some child is UNKNOWN, which is the union of the children's
  /// UNKNOWN documents minus the union of their true ones.
  @Override
  public BitmapCollection getBitmaps() {
    int numChildren = _filterOperators.size();
    ImmutableRoaringBitmap[] trues = new ImmutableRoaringBitmap[numChildren];
    MutableRoaringBitmap nulls = null;
    for (int i = 0; i < numChildren; i++) {
      BitmapCollection childBitmaps = _filterOperators.get(i).getBitmaps();
      trues[i] = childBitmaps.reduce();
      ImmutableRoaringBitmap childNulls = childBitmaps.getNullBitmap();
      if (childNulls != null) {
        if (nulls == null) {
          nulls = new MutableRoaringBitmap();
        }
        nulls.or(childNulls);
      }
    }
    MutableRoaringBitmap orTrues = BufferFastAggregation.or(trues);
    if (nulls == null) {
      return new BitmapCollection(_numDocs, false, orTrues);
    }
    nulls.andNot(orTrues);
    return new BitmapCollection(_numDocs, false, orTrues).excludingNulls(nulls);
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
}
