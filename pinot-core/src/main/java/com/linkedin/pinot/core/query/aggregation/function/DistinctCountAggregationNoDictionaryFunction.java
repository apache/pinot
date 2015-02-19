package com.linkedin.pinot.core.query.aggregation.function;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;


public class DistinctCountAggregationNoDictionaryFunction extends DistinctCountAggregationFunction {

  @Override
  public IntOpenHashSet aggregate(Block docIdSetBlock, Block[] block) {
    IntOpenHashSet ret = new IntOpenHashSet();
    int docId = 0;
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    // Assume dictionary is always there for String data type.
    // If data type is String, we shouldn't hit here.
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      blockValIterator.skipTo(docId);
      ret.add(blockValIterator.nextIntVal());
    }

    return ret;
  }

  @Override
  public IntOpenHashSet aggregate(IntOpenHashSet mergedResult, int docId, Block[] block) {
    if (mergedResult == null) {
      mergedResult = new IntOpenHashSet();
    }
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    blockValIterator.skipTo(docId);
    if (block[0].getMetadata().getDataType() == DataType.STRING) {
      mergedResult.add(block[0].getMetadata().getDictionary().get(blockValIterator.nextIntVal()).hashCode());
    } else {
      mergedResult.add(((Number) block[0].getMetadata().getDictionary().get(blockValIterator.nextIntVal())).intValue());
    }
    return mergedResult;
  }

}
