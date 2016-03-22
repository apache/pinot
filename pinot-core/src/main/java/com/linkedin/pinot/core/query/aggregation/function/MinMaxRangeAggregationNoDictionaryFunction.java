/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;

/**
 * This function will take a column and do sum on that.
 */
public class MinMaxRangeAggregationNoDictionaryFunction extends MinMaxRangeAggregationFunction {

  @Override
  public MinMaxRangePair aggregate(Block docIdSetBlock, Block[] block) {
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    int docId = 0;
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      if (blockValIterator.skipTo(docId)) {
        double nextDoubleVal = blockValIterator.nextDoubleVal();
        min = Math.min(min, nextDoubleVal);
        max = Math.max(max, nextDoubleVal);
      }
    }
    return new MinMaxRangePair(min, max);
  }

  @Override
  public MinMaxRangePair aggregate(MinMaxRangePair mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      double nextDoubleVal = blockValIterator.nextDoubleVal();
      if (mergedResult == null) {
        return new MinMaxRangePair(nextDoubleVal, nextDoubleVal);
      }
      return new MinMaxRangePair(
          Math.min(mergedResult.getFirst(), nextDoubleVal),
          Math.max(mergedResult.getSecond(), nextDoubleVal));
    }
    return mergedResult;
  }
}
