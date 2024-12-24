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
package org.apache.pinot.core.query.distinct;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Base implementation of {@link DistinctExecutor} for single column.
 */
public abstract class BaseSingleColumnDistinctExecutor<T extends DistinctTable, S, M> implements DistinctExecutor {
  protected final ExpressionContext _expression;
  protected final T _distinctTable;

  public BaseSingleColumnDistinctExecutor(ExpressionContext expression, T distinctTable) {
    _expression = expression;
    _distinctTable = distinctTable;
  }

  @Override
  public boolean process(ValueBlock valueBlock) {
    BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expression);
    int numDocs = valueBlock.getNumDocs();
    if (_distinctTable.isNullHandlingEnabled() && blockValueSet.isSingleValue()) {
      RoaringBitmap nullBitmap = blockValueSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        return processWithNull(blockValueSet, numDocs, nullBitmap);
      } else {
        return processWithoutNull(blockValueSet, numDocs);
      }
    } else {
      return processWithoutNull(blockValueSet, numDocs);
    }
  }

  private boolean processWithNull(BlockValSet blockValueSet, int numDocs, RoaringBitmap nullBitmap) {
    _distinctTable.addNull();
    S values = getValuesSV(blockValueSet);
    PeekableIntIterator nullIterator = nullBitmap.getIntIterator();
    int prev = 0;
    while (nullIterator.hasNext()) {
      int nextNull = nullIterator.next();
      if (nextNull > prev) {
        if (processSV(values, prev, nextNull)) {
          return true;
        }
      }
      prev = nextNull + 1;
    }
    if (prev < numDocs) {
      return processSV(values, prev, numDocs);
    }
    return false;
  }

  private boolean processWithoutNull(BlockValSet blockValueSet, int numDocs) {
    if (blockValueSet.isSingleValue()) {
      return processSV(getValuesSV(blockValueSet), 0, numDocs);
    } else {
      return processMV(getValuesMV(blockValueSet), 0, numDocs);
    }
  }

  /**
   * Reads the single-value values from the block value set.
   */
  protected abstract S getValuesSV(BlockValSet blockValSet);

  /**
   * Reads the multi-value values from the block value set.
   */
  protected abstract M getValuesMV(BlockValSet blockValSet);

  /**
   * Processes the single-value values for the given range.
   */
  protected abstract boolean processSV(S values, int from, int to);

  /**
   * Processes the multi-value values for the given range.
   */
  protected abstract boolean processMV(M values, int from, int to);

  @Override
  public DistinctTable getResult() {
    return _distinctTable;
  }
}
