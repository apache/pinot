package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.segment.index.readers.DictionaryReader;


/**
 * This function will take a column and do sum on that.
 *
 */
public class SumAggregationNoDictionaryFunction extends SumAggregationFunction {

  @Override
  public Double aggregate(Block docIdSetBlock, Block[] block) {
    double ret = 0;
    int docId = 0;
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      blockValIterator.skipTo(docId);
      ret += blockValIterator.nextDoubleVal();
    }
    return ret;
  }

  @Override
  public Double aggregate(Double mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    blockValIterator.skipTo(docId);
    if (mergedResult == null) {
      return blockValIterator.nextDoubleVal();
    }
    return mergedResult + blockValIterator.nextDoubleVal();
  }

}
