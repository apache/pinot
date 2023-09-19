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
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.NotDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;


/**
 * The {@link BaseFilterOperator} class is the base class for all filter operators.
 */
public abstract class BaseFilterOperator extends BaseOperator<FilterBlock> {
  protected final int _numDocs;
  protected final boolean _nullHandlingEnabled;

  public BaseFilterOperator(int numDocs, boolean nullHandlingEnabled) {
    _numDocs = numDocs;
    _nullHandlingEnabled = nullHandlingEnabled;
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
    if (_nullHandlingEnabled) {
      return new NotDocIdSet(new OrDocIdSet(Arrays.asList(getTrues(), getNulls()), _numDocs),
          _numDocs);
    } else {
      return new NotDocIdSet(getTrues(), _numDocs);
    }
  }
}
