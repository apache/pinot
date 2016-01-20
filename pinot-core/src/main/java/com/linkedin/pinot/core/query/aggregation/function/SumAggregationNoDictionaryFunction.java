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
 *
 */
public class SumAggregationNoDictionaryFunction extends SumAggregationFunction {

  @Override
  public Double aggregate(Block docIdSetBlock, Block[] blocks) {
    double ret = 0;
    int docId = 0;
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) blocks[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      if (blockValIterator.skipTo(docId)) {
        double nextDoubleVal = blockValIterator.nextDoubleVal();
        ret += nextDoubleVal;
      }
    }
    return ret;
  }

  @Override
  public Double aggregate(Double mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    if (blockValIterator.skipTo(docId)) {
      if (mergedResult == null) {
        return blockValIterator.nextDoubleVal();
      }
      return mergedResult + blockValIterator.nextDoubleVal();
    }
    return mergedResult;
  }

}
