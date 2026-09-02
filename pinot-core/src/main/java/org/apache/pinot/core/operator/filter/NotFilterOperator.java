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
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.operator.docidsets.NotDocIdSet;


public class NotFilterOperator extends BaseFilterOperator {

  private static final String EXPLAIN_NAME = "FILTER_NOT";
  private final BaseFilterOperator _filterOperator;

  public NotFilterOperator(BaseFilterOperator filterOperator, int numDocs, boolean nullHandlingEnabled) {
    super(numDocs, nullHandlingEnabled);
    _filterOperator = filterOperator;
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of(_filterOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_filterOperator.isResultEmpty()) {
      return new MatchAllDocIdSet(_numDocs);
    } else {
      return _filterOperator.getFalses();
    }
  }

  @Override
  protected BlockDocIdSet getFalses() {
    return _filterOperator.getTrues();
  }

  /// NOT of UNKNOWN is UNKNOWN: a negation is UNKNOWN exactly where its child is.
  @Override
  protected BlockDocIdSet getNulls() {
    return _filterOperator.getNulls();
  }

  /// A negation is not false where its child is not true.
  @Override
  protected BlockDocIdSet getNotFalses() {
    BlockDocIdSet childTrues = _filterOperator.getTrues();
    if (childTrues instanceof MatchAllDocIdSet) {
      return EmptyDocIdSet.getInstance();
    }
    if (childTrues instanceof EmptyDocIdSet) {
      return new MatchAllDocIdSet(_numDocs);
    }
    return new NotDocIdSet(childTrues, _numDocs);
  }

  /// The complement of the child's count is its false documents only when none is UNKNOWN. Otherwise the count comes
  /// from the child's bitmaps, which know the UNKNOWN documents and keep them out of the inversion.
  @Override
  public boolean canOptimizeCount() {
    return _filterOperator.mayHaveNulls() ? _filterOperator.canProduceBitmaps() : _filterOperator.canOptimizeCount();
  }

  @Override
  public int getNumMatchingDocs() {
    if (_filterOperator.mayHaveNulls()) {
      return getBitmaps().getCardinality();
    }
    return _numDocs - _filterOperator.getNumMatchingDocs();
  }

  @Override
  public boolean canProduceBitmaps() {
    return _filterOperator.canProduceBitmaps();
  }

  @Override
  public BitmapCollection getBitmaps() {
    return _filterOperator.getBitmaps().invert();
  }

  @Override
  public boolean mayHaveNulls() {
    return _filterOperator.mayHaveNulls();
  }

  public BaseFilterOperator getChildFilterOperator() {
    return _filterOperator;
  }
}
