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
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


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
    for (BaseFilterOperator filterOperator : _filterOperators) {
      blockDocIdSets.add(filterOperator.getTrues());
    }
    return new AndDocIdSet(blockDocIdSets, _queryOptions);
  }

  @Override
  protected BlockDocIdSet getFalses() {
    List<BlockDocIdSet> blockDocIdSets = new ArrayList<>(_filterOperators.size());
    for (BaseFilterOperator filterOperator : _filterOperators) {
      if (filterOperator.isResultEmpty()) {
        blockDocIdSets.add(new MatchAllDocIdSet(_numDocs));
      } else {
        blockDocIdSets.add(filterOperator.getFalses());
      }
    }
    return new OrDocIdSet(blockDocIdSets, _numDocs);
  }

  @Override
  public boolean canOptimizeCount() {
    boolean allChildrenCanProduceBitmaps = true;
    for (BaseFilterOperator child : _filterOperators) {
      allChildrenCanProduceBitmaps &= child.canProduceBitmaps();
    }
    return allChildrenCanProduceBitmaps;
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
  public List<Operator> getChildOperators() {
    return new ArrayList<>(_filterOperators);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
