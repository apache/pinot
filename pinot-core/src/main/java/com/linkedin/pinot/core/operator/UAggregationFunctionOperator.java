package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.block.aggregation.AggregationResultBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


public class UAggregationFunctionOperator implements Operator {

  private AggregationFunction _aggregationFunction;
  private final UProjectionOperator _projectionOperators;

  public UAggregationFunctionOperator(AggregationInfo aggregationInfo, Operator projectionOperator) {
    _aggregationFunction = AggregationFunctionFactory.get(aggregationInfo);
    _projectionOperators = (UProjectionOperator) projectionOperator;
  }

  @Override
  public boolean open() {
    _projectionOperators.open();
    return true;
  }

  @Override
  public boolean close() {
    _projectionOperators.close();
    return true;
  }

  @Override
  public Block nextBlock() {
    Block[] blocks = _projectionOperators.getDataBlock(_aggregationFunction.getColumns());
    if (blocks[0] != null) {
      BlockValIterator[] blockValIterators = new BlockValIterator[blocks.length];
      for (int i = 0; i < blocks.length; ++i) {
        blockValIterators[i] = blocks[i].getBlockValueSet().iterator();
      }
      return new AggregationResultBlock(_aggregationFunction.aggregate(blockValIterators));
    }
    return null;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in UAggregationFunctionOperator");
  }

  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

}
