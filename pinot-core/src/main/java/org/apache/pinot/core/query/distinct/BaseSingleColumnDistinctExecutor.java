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

import java.util.function.LongSupplier;
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
  private final DistinctEarlyTerminationContext _earlyTerminationContext = new DistinctEarlyTerminationContext();

  public BaseSingleColumnDistinctExecutor(ExpressionContext expression, T distinctTable) {
    _expression = expression;
    _distinctTable = distinctTable;
  }

  @Override
  public void setMaxRowsToProcess(int maxRows) {
    _earlyTerminationContext.setMaxRowsToProcess(maxRows);
  }

  @Override
  public void setNumRowsWithoutChangeInDistinct(int numRowsWithoutChangeInDistinct) {
    _earlyTerminationContext.setNumRowsWithoutChangeInDistinct(numRowsWithoutChangeInDistinct);
  }

  @Override
  public void setTimeSupplier(LongSupplier timeSupplier) {
    _earlyTerminationContext.setTimeSupplier(timeSupplier);
  }

  @Override
  public void setRemainingTimeNanos(long remainingTimeNanos) {
    _earlyTerminationContext.setRemainingTimeNanos(remainingTimeNanos);
  }

  @Override
  public boolean isNumRowsWithoutChangeLimitReached() {
    return _earlyTerminationContext.isNumRowsWithoutChangeLimitReached();
  }

  @Override
  public int getNumRowsProcessed() {
    return _earlyTerminationContext.getNumRowsProcessed();
  }

  @Override
  public boolean process(ValueBlock valueBlock) {
    if (!_earlyTerminationContext.isTrackingEnabled()) {
      BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expression);
      return processWithoutTracking(blockValueSet, valueBlock.getNumDocs());
    }
    if (shouldStopProcessing()) {
      return true;
    }
    BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expression);
    int numDocs = clampToRemaining(valueBlock.getNumDocs());
    if (numDocs <= 0) {
      return true;
    }
    boolean trackDistinctChange = _earlyTerminationContext.isDistinctChangeTrackingEnabled();
    boolean limitReached = processWithTracking(blockValueSet, numDocs, trackDistinctChange);
    return limitReached || shouldStopProcessing();
  }

  private boolean processWithTracking(BlockValSet blockValueSet, int numDocs, boolean trackDistinctChange) {
    if (blockValueSet.isSingleValue()) {
      RoaringBitmap nullBitmap = null;
      if (_distinctTable.isNullHandlingEnabled()) {
        nullBitmap = blockValueSet.getNullBitmap();
      }
      S values = getValuesSV(blockValueSet);
      return processSVWithTracking(values, 0, numDocs, nullBitmap, trackDistinctChange);
    } else {
      M values = getValuesMV(blockValueSet);
      return processMVWithTracking(values, 0, numDocs, trackDistinctChange);
    }
  }

  private boolean processWithoutTracking(BlockValSet blockValueSet, int numDocs) {
    if (_distinctTable.isNullHandlingEnabled() && blockValueSet.isSingleValue()) {
      RoaringBitmap nullBitmap = blockValueSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        return processWithNull(blockValueSet, numDocs, nullBitmap);
      }
    }
    return processWithoutNull(blockValueSet, numDocs);
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

  @Override
  public int getNumDistinctRowsCollected() {
    return _distinctTable.size();
  }

  @Override
  public int getRemainingRowsToProcess() {
    return _earlyTerminationContext.getRemainingRowsToProcess();
  }

  private int clampToRemaining(int numDocs) {
    return _earlyTerminationContext.clampToRemaining(numDocs);
  }

  private void recordRowProcessed(boolean distinctChanged) {
    _earlyTerminationContext.recordRowProcessed(distinctChanged);
  }

  private boolean shouldStopProcessing() {
    return _earlyTerminationContext.shouldStopProcessing();
  }

  private boolean shouldStopProcessingWithoutTime() {
    return _earlyTerminationContext.shouldStopProcessingWithoutTime();
  }

  private boolean processSVWithTracking(S values, int from, int to, RoaringBitmap nullBitmap,
      boolean trackDistinctChange) {
    boolean limitReached = false;
    boolean nullAdded = _distinctTable.hasNull();
    for (int docId = from; docId < to; docId++) {
      boolean distinctChanged = false;
      if (nullBitmap != null && nullBitmap.contains(docId)) {
        if (!nullAdded) {
          _distinctTable.addNull();
          nullAdded = true;
          distinctChanged = true;
        }
      } else {
        if (trackDistinctChange) {
          int sizeBefore = _distinctTable.size();
          limitReached = processSV(values, docId, docId + 1);
          distinctChanged = _distinctTable.size() > sizeBefore;
        } else {
          limitReached = processSV(values, docId, docId + 1);
        }
      }
      recordRowProcessed(trackDistinctChange && distinctChanged);
      if (limitReached || shouldStopProcessingWithoutTime()) {
        break;
      }
    }
    return limitReached;
  }

  private boolean processMVWithTracking(M values, int from, int to, boolean trackDistinctChange) {
    boolean limitReached = false;
    for (int docId = from; docId < to; docId++) {
      boolean distinctChanged = false;
      if (trackDistinctChange) {
        int sizeBefore = _distinctTable.size();
        limitReached = processMV(values, docId, docId + 1);
        distinctChanged = _distinctTable.size() > sizeBefore;
      } else {
        limitReached = processMV(values, docId, docId + 1);
      }
      recordRowProcessed(trackDistinctChange && distinctChanged);
      if (limitReached || shouldStopProcessingWithoutTime()) {
        break;
      }
    }
    return limitReached;
  }
}
