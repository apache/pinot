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

import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;


public class DistinctResultsBlockMerger implements ResultsBlockMerger<DistinctResultsBlock> {
  private final int _maxRowsAcrossSegments;
  private boolean _rowBudgetReached;
  private boolean _timeLimitReached;

  public DistinctResultsBlockMerger(QueryContext queryContext) {
    if (queryContext.getQueryOptions() != null) {
      Integer maxRows = QueryOptionsUtils.getMaxRowsInDistinct(queryContext.getQueryOptions());
      _maxRowsAcrossSegments = maxRows != null ? maxRows : Integer.MAX_VALUE;
    } else {
      _maxRowsAcrossSegments = Integer.MAX_VALUE;
    }
  }

  @Override
  public boolean isQuerySatisfied(DistinctResultsBlock resultsBlock) {
    if (_timeLimitReached) {
      return true;
    }
    if (resultsBlock.getEarlyTerminationReason() == BaseResultsBlock.EarlyTerminationReason.TIME_LIMIT) {
      _timeLimitReached = true;
      return true;
    }
    if (_rowBudgetReached) {
      return true;
    }
    if (_maxRowsAcrossSegments != Integer.MAX_VALUE
        && resultsBlock.getNumDocsScanned() >= _maxRowsAcrossSegments) {
      if (resultsBlock.getEarlyTerminationReason() == BaseResultsBlock.EarlyTerminationReason.NONE) {
        resultsBlock.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_MAX_ROWS);
      }
      _rowBudgetReached = true;
      return true;
    }
    return resultsBlock.getDistinctTable().isSatisfied();
  }

  @Override
  public void mergeResultsBlocks(DistinctResultsBlock mergedBlock, DistinctResultsBlock blockToMerge) {
    if (_rowBudgetReached || _timeLimitReached) {
      return;
    }
    boolean timeLimitReached =
        blockToMerge.getEarlyTerminationReason() == BaseResultsBlock.EarlyTerminationReason.TIME_LIMIT;
    mergedBlock.setNumDocsScanned(mergedBlock.getNumDocsScanned() + blockToMerge.getNumDocsScanned());
    mergedBlock.getDistinctTable().mergeDistinctTable(blockToMerge.getDistinctTable());
    if (_maxRowsAcrossSegments != Integer.MAX_VALUE
        && mergedBlock.getNumDocsScanned() >= _maxRowsAcrossSegments) {
      if (mergedBlock.getEarlyTerminationReason() == BaseResultsBlock.EarlyTerminationReason.NONE) {
        mergedBlock.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_MAX_ROWS);
      }
      _rowBudgetReached = true;
    } else if (timeLimitReached) {
      if (mergedBlock.getEarlyTerminationReason() == BaseResultsBlock.EarlyTerminationReason.NONE) {
        mergedBlock.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.TIME_LIMIT);
      }
      _timeLimitReached = true;
    }
  }
}
