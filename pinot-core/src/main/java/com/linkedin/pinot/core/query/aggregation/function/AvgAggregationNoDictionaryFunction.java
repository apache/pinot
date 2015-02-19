package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;


/**
 * This function will take a column and do sum on that.
 *
 */
public class AvgAggregationNoDictionaryFunction extends AvgAggregationFunction {

  @Override
  public AvgPair aggregate(Block docIdSetBlock, Block[] block) {
    double ret = 0;
    long cnt = 0;
    int docId = 0;
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      blockValIterator.skipTo(docId);
      ret += blockValIterator.nextDoubleVal();
      cnt++;
    }
    return new AvgPair(ret, cnt);
  }

  @Override
  public AvgPair aggregate(AvgPair mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    blockValIterator.skipTo(docId);
    if (mergedResult == null) {
      return new AvgPair(blockValIterator.nextDoubleVal(), (long) 1);
    }
    return new AvgPair(mergedResult.getFirst() + blockValIterator.nextDoubleVal(), mergedResult.getSecond() + 1);
  }
}
