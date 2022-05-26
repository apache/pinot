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
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class OrFilterOperator extends BaseFilterOperator {

  private static final String EXPLAIN_NAME = "FILTER_OR";
  private final List<BaseFilterOperator> _filterOperators;
  private final int _numDocs;

  public OrFilterOperator(List<BaseFilterOperator> filterOperators, int numDocs) {
    _filterOperators = filterOperators;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    Tracing.activeRecording().setNumChildren(_filterOperators.size());
    List<FilterBlockDocIdSet> filterBlockDocIdSets = new ArrayList<>(_filterOperators.size());
    for (BaseFilterOperator filterOperator : _filterOperators) {
      filterBlockDocIdSets.add(filterOperator.nextBlock().getBlockDocIdSet());
    }
    return new FilterBlock(new OrDocIdSet(filterBlockDocIdSets, _numDocs));
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
    boolean allChildrenProduceBitmaps = true;
    for (BaseFilterOperator child : _filterOperators) {
      allChildrenProduceBitmaps &= child.canProduceBitmaps();
    }
    return allChildrenProduceBitmaps;
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
}
