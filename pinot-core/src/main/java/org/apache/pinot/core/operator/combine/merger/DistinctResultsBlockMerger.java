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
package org.apache.pinot.core.operator.combine.merger;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;


public class DistinctResultsBlockMerger implements ResultsBlockMerger<DistinctResultsBlock> {
  private static final int UNLIMITED_ROWS = Integer.MAX_VALUE;
  private static final long UNLIMITED_TIME_NANOS = Long.MAX_VALUE;

  private final int _maxRowsAcrossSegments;
  private final int _numRowsWithoutChangeLimit;
  private long _rowsWithoutChange;
  private boolean _rowBudgetReached;
  private boolean _noChangeLimitReached;
  private boolean _timeLimitReached;
  private final long _maxExecutionTimeNs;
  private final long _startTimeNs;
  private BaseResultsBlock.EarlyTerminationReason _earlyTerminationReason =
      BaseResultsBlock.EarlyTerminationReason.NONE;

  public DistinctResultsBlockMerger(QueryContext queryContext) {
    Integer maxRows = QueryOptionsUtils.getMaxRowsInDistinct(queryContext.getQueryOptions());
    _maxRowsAcrossSegments = maxRows != null ? maxRows : UNLIMITED_ROWS;
    Integer numRowsWithoutChange = QueryOptionsUtils.getNumRowsWithoutChangeInDistinct(queryContext.getQueryOptions());
    _numRowsWithoutChangeLimit = numRowsWithoutChange != null ? numRowsWithoutChange : UNLIMITED_ROWS;
    Long maxExecutionTimeMs = QueryOptionsUtils.getMaxExecutionTimeMsInDistinct(queryContext.getQueryOptions());
    _maxExecutionTimeNs = maxExecutionTimeMs != null ? TimeUnit.MILLISECONDS.toNanos(maxExecutionTimeMs)
        : UNLIMITED_TIME_NANOS;
    _startTimeNs = System.nanoTime();
  }

  @Override
  public boolean isQuerySatisfied(DistinctResultsBlock resultsBlock) {
    if (_rowBudgetReached || _noChangeLimitReached) {
      applyEarlyTerminationReasonIfNeeded(resultsBlock);
      return true;
    }
    if (resultsBlock.getDistinctTable().isSatisfied()) {
      return true;
    }
    BaseResultsBlock.EarlyTerminationReason blockReason = resultsBlock.getEarlyTerminationReason();
    if (blockReason == BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT) {
      applyBlockEarlyTerminationReason(blockReason);
      applyEarlyTerminationReasonIfNeeded(resultsBlock);
      return true;
    }
    if (_timeLimitReached || hasExceededTimeLimit()) {
      _timeLimitReached = true;
      updateEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);
      applyEarlyTerminationReasonIfNeeded(resultsBlock);
      return true;
    }
    return false;
  }

  @Override
  public void mergeResultsBlocks(DistinctResultsBlock mergedBlock, DistinctResultsBlock blockToMerge) {
    int sizeBefore = mergedBlock.getDistinctTable().size();
    mergedBlock.getDistinctTable().mergeDistinctTable(blockToMerge.getDistinctTable());
    int sizeAfter = mergedBlock.getDistinctTable().size();
    mergedBlock.setNumDocsScanned(mergedBlock.getNumDocsScanned() + blockToMerge.getNumDocsScanned());
    if (_numRowsWithoutChangeLimit != UNLIMITED_ROWS) {
      if (sizeBefore == sizeAfter) {
        _rowsWithoutChange += blockToMerge.getNumDocsScanned();
        if (_rowsWithoutChange >= _numRowsWithoutChangeLimit) {
          _noChangeLimitReached = true;
          updateEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_NO_NEW_VALUES);
        }
      } else {
        _rowsWithoutChange = 0;
      }
    }
    if (_maxRowsAcrossSegments != UNLIMITED_ROWS
        && mergedBlock.getNumDocsScanned() >= _maxRowsAcrossSegments) {
      _rowBudgetReached = true;
      updateEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_MAX_ROWS);
    }
    BaseResultsBlock.EarlyTerminationReason blockReason = blockToMerge.getEarlyTerminationReason();
    if (blockReason == BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT) {
      applyBlockEarlyTerminationReason(blockReason);
    }
    if (!_rowBudgetReached && !_noChangeLimitReached && (_timeLimitReached || hasExceededTimeLimit())) {
      _timeLimitReached = true;
      updateEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);
    }
    applyEarlyTerminationReasonIfNeeded(mergedBlock);
  }

  private boolean hasExceededTimeLimit() {
    return _maxExecutionTimeNs != UNLIMITED_TIME_NANOS && System.nanoTime() - _startTimeNs >= _maxExecutionTimeNs;
  }

  private void applyBlockEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason reason) {
    switch (reason) {
      case DISTINCT_TIME_LIMIT:
        _timeLimitReached = true;
        updateEarlyTerminationReason(reason);
        break;
      default:
        break;
    }
  }

  private void updateEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason reason) {
    if (reason == BaseResultsBlock.EarlyTerminationReason.NONE || _earlyTerminationReason == reason) {
      return;
    }
    if (_earlyTerminationReason == BaseResultsBlock.EarlyTerminationReason.NONE
        || (_earlyTerminationReason == BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT
            && reason != BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT)) {
      _earlyTerminationReason = reason;
    }
  }

  private void applyEarlyTerminationReasonIfNeeded(DistinctResultsBlock resultsBlock) {
    if (_earlyTerminationReason == BaseResultsBlock.EarlyTerminationReason.NONE) {
      return;
    }
    BaseResultsBlock.EarlyTerminationReason currentReason = resultsBlock.getEarlyTerminationReason();
    if (currentReason == BaseResultsBlock.EarlyTerminationReason.NONE
        || (currentReason == BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT
            && _earlyTerminationReason != BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT)) {
      resultsBlock.setEarlyTerminationReason(_earlyTerminationReason);
    }
  }
}
