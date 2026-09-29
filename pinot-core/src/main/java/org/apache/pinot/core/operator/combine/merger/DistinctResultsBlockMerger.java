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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock.EarlyTerminationReason;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;


public class DistinctResultsBlockMerger implements ResultsBlockMerger<DistinctResultsBlock> {
  private static final int UNLIMITED = Integer.MAX_VALUE;
  private static final long UNLIMITED_TIME_NS = Long.MAX_VALUE;

  private final int _maxRows;
  private final int _maxRowsWithoutChange;
  private final long _deadlineNs;

  private long _numRowsWithoutChange;

  public DistinctResultsBlockMerger(QueryContext queryContext) {
    Integer maxRows = QueryOptionsUtils.getMaxRowsInDistinct(queryContext.getQueryOptions());
    _maxRows = maxRows != null ? maxRows : UNLIMITED;
    Integer maxRowsWithoutChange =
        QueryOptionsUtils.getMaxRowsWithoutChangeInDistinct(queryContext.getQueryOptions());
    _maxRowsWithoutChange = maxRowsWithoutChange != null ? maxRowsWithoutChange : UNLIMITED;
    _deadlineNs = computeDeadlineNs(
        QueryOptionsUtils.getMaxExecutionTimeMsInDistinct(queryContext.getQueryOptions()));
  }

  @Override
  public boolean isQuerySatisfied(DistinctResultsBlock resultsBlock) {
    return resultsBlock.getEarlyTerminationReason() != EarlyTerminationReason.NONE
        || resultsBlock.getDistinctTable().isSatisfied();
  }

  @Override
  public void mergeResultsBlocks(DistinctResultsBlock mergedBlock, DistinctResultsBlock blockToMerge) {
    int sizeBefore = mergedBlock.getDistinctTable().size();
    mergedBlock.getDistinctTable().mergeDistinctTable(blockToMerge.getDistinctTable());
    int sizeAfter = mergedBlock.getDistinctTable().size();
    mergedBlock.setNumDocsScanned(mergedBlock.getNumDocsScanned() + blockToMerge.getNumDocsScanned());

    if (mergedBlock.getDistinctTable().isSatisfied()) {
      return;
    }
    if (_maxRows != UNLIMITED && mergedBlock.getNumDocsScanned() >= _maxRows) {
      mergedBlock.setEarlyTerminationReason(EarlyTerminationReason.DISTINCT_MAX_ROWS);
      return;
    }
    if (_maxRowsWithoutChange != UNLIMITED) {
      if (sizeBefore == sizeAfter) {
        _numRowsWithoutChange += blockToMerge.getNumDocsScanned();
        if (_numRowsWithoutChange >= _maxRowsWithoutChange) {
          mergedBlock.setEarlyTerminationReason(EarlyTerminationReason.DISTINCT_MAX_ROWS_WITHOUT_CHANGE);
          return;
        }
      } else {
        _numRowsWithoutChange = 0;
      }
    }
    if (_deadlineNs != UNLIMITED_TIME_NS && System.nanoTime() >= _deadlineNs) {
      mergedBlock.setEarlyTerminationReason(EarlyTerminationReason.DISTINCT_MAX_EXECUTION_TIME);
    }
  }

  private static long computeDeadlineNs(@Nullable Long maxExecutionTimeMs) {
    if (maxExecutionTimeMs == null) {
      return UNLIMITED_TIME_NS;
    }
    try {
      return Math.addExact(System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(maxExecutionTimeMs));
    } catch (ArithmeticException e) {
      return UNLIMITED_TIME_NS;
    }
  }
}
