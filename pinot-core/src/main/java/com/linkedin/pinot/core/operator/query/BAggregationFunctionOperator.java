package com.linkedin.pinot.core.operator.query;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.block.query.AggregationResultBlock;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


/**
 * AggregationFunction may need multiple data sources.
 * nextBlock() will take the Iterators from data sources and send to AggregationFunction.
 * 
 * @author xiafu
 *
 */
public class BAggregationFunctionOperator implements Operator {

  private final AggregationFunction _aggregationFunction;
  private final Block[] _blocks;
  private final BlockValIterator[] _blockValIterators;
  private final String _columns[];
  private final Operator _projectionOperator;

  public BAggregationFunctionOperator(AggregationInfo aggregationInfo, Operator projectionOperator) {
    _aggregationFunction = AggregationFunctionFactory.get(aggregationInfo);
    _projectionOperator = projectionOperator;
    if (aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
      _columns = new String[1];
      _columns[0] = null;
    } else {
      String columns = aggregationInfo.getAggregationParams().get("column").trim();
      _columns = columns.split(",");
    }
    _blocks = new Block[_columns.length];
    _blockValIterators = new BlockValIterator[_columns.length];
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    return true;
  }

  @Override
  public Block nextBlock() {
    ProjectionBlock block = (ProjectionBlock) _projectionOperator.nextBlock();
    if (block != null) {
      for (int i = 0; i < _blocks.length; ++i) {
        _blockValIterators[i] = block.getBlockValueSetIterator(_columns[i]);
        //_blockValIterators[i] = block.getBlock(_columns[i]).getBlockValueSet().iterator();
      }
      return new AggregationResultBlock(_aggregationFunction.aggregate(_blockValIterators));
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
