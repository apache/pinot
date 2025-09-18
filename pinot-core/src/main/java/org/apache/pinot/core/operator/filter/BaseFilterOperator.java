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

import java.util.Arrays;
import java.util.Iterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.operator.docidsets.NotDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;


/**
 * The {@link BaseFilterOperator} class is the base class for all filter operators.
 *
 * Unlike other operators, the {#nextBlock()} method of these operators is expected to be called only once.
 * This single call returns the {@link FilterBlock} containing the {@link BlockDocIdSet} with all the matching
 * documents for the given filter and the segment.
 */
public abstract class BaseFilterOperator extends BaseOperator<FilterBlock> {
  protected final int _numDocs;
  protected final boolean _nullHandlingEnabled;
  protected final boolean _ascending;

  public BaseFilterOperator(int numDocs, boolean nullHandlingEnabled, boolean ascending) {
    _numDocs = numDocs;
    _nullHandlingEnabled = nullHandlingEnabled;
    _ascending = ascending;
  }

  /**
   * Returns {@code true} if the result is always empty, {@code false} otherwise.
   */
  public boolean isResultEmpty() {
    return false;
  }

  /**
   * Returns {@code true} if the result matches all the records, {@code false} otherwise.
   */
  public boolean isResultMatchingAll() {
    return false;
  }

  /**
   * Returns {@code true} if the filter has an optimized count implementation.
   */
  public boolean canOptimizeCount() {
    return false;
  }

  /**
   * @return the number of matching docs, or throws if it cannot produce this count.
   */
  public int getNumMatchingDocs() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return true if the filter operator can produce a bitmap of docIds
   */
  public boolean canProduceBitmaps() {
    return false;
  }

  /**
   * @return bitmaps of matching docIds
   */
  public BitmapCollection getBitmaps() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FilterBlock getNextBlock() {
    return new FilterBlock(getTrues());
  }

  /**
   * @return document IDs in which the predicate evaluates to true.
   */
  protected abstract BlockDocIdSet getTrues();

  /**
   * @return document IDs in which the predicate evaluates to NULL.
   */
  protected BlockDocIdSet getNulls() {
    return EmptyDocIdSet.getInstance();
  }

  /**
   * @return document IDs in which the predicate evaluates to false.
   */
  protected BlockDocIdSet getFalses() {
    BlockDocIdSet trues = getTrues();
    if (trues instanceof MatchAllDocIdSet) {
      return EmptyDocIdSet.getInstance();
    }
    if (_nullHandlingEnabled) {
      BlockDocIdSet nulls = getNulls();
      if (!(nulls instanceof EmptyDocIdSet)) {
        return new NotDocIdSet(new OrDocIdSet(Arrays.asList(trues, nulls), _numDocs),
            _numDocs);
      }
    }
    if (trues instanceof EmptyDocIdSet) {
      return MatchAllDocIdSet.create(_numDocs, _ascending);
    }
    return new NotDocIdSet(trues, _numDocs);
  }

  /// Returns the order between rows in a block.
  ///
  /// Most BaseFilterOperators are ascending. Remember that a empty filter is considered to be both ascending
  /// and descending.
  public boolean isAscending() {
    return _ascending || isResultEmpty();
  }

  /// Returns the order between rows in a block.
  ///
  /// Most BaseFilterOperators are ascending. Remember that a empty filter is considered to be both ascending
  /// and descending.
  public boolean isDescending() {
    return !_ascending || isResultEmpty();
  }

  /// Returns a reversed version of this filter operator.
  protected abstract BaseFilterOperator reverse()
      throws UnsupportedOperationException;

  /// Returns a [BaseFilterOperator] that is ascending or descending based on the input parameter.
  ///
  /// Remember that an empty filter is considered to be both ascending and descending.
  public BaseFilterOperator withOrder(boolean ascending)
      throws UnsupportedOperationException {
    if (isResultEmpty()) {
      return this;
    }
    if (ascending == _ascending) {
      return this;
    }
    return reverse();
  }

  protected static boolean getCommonAscending(Iterable<BaseFilterOperator> filterOperators) {
    Iterator<BaseFilterOperator> iterator = filterOperators.iterator();
    boolean ascendingSoFar = true;
    boolean descendingSoFar = true;
    while (iterator.hasNext() && (ascendingSoFar || descendingSoFar)) {
      BaseFilterOperator filterOperator = iterator.next();
      if (!filterOperator.isAscending() && !filterOperator.isDescending()) {
        // this should only happen on test mocks
        continue;
      }
      ascendingSoFar &= filterOperator.isAscending();
      descendingSoFar &= filterOperator.isDescending();
    }
    if (ascendingSoFar) {
      return true;
    }
    if (descendingSoFar) {
      return false;
    }
    throw new IllegalStateException("All filter operators must have the same order");
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putLong("numDocs", _numDocs);
    attributeBuilder.putString("order", isAscending() ? "asc" : "desc");
  }
}
