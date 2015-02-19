package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;


public class MinAggregationNoDictionaryFunction extends MinAggregationFunction {

  @Override
  public Double aggregate(Block docIdSetBlock, Block[] block) {
    double ret = Double.POSITIVE_INFINITY;
    double tmp = 0;
    int docId = 0;
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      blockValIterator.skipTo(docId);
      tmp = blockValIterator.nextDoubleVal();
      if (tmp < ret) {
        ret = tmp;
      }
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
    double tmp = blockValIterator.nextDoubleVal();
    if (tmp < mergedResult) {
      return tmp;
    }
    return mergedResult;
  }

}
