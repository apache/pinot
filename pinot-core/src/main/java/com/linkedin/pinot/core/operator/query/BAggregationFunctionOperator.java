package com.linkedin.pinot.core.operator.query;

import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.block.query.AggregationResultBlock;
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
  private final List<Operator> _dataSourceOps;
  private final Block[] _blocks;
  private final BlockValIterator[] _blockValIterators;

  public BAggregationFunctionOperator(AggregationInfo aggregationInfo, List<Operator> dataSourceOps) {
    _aggregationFunction = AggregationFunctionFactory.get(aggregationInfo);
    _dataSourceOps = dataSourceOps;
    _blocks = new Block[_dataSourceOps.size()];
    _blockValIterators = new BlockValIterator[_dataSourceOps.size()];
  }

  @Override
  public boolean open() {
    for (Operator op : _dataSourceOps) {
      op.open();
    }
    return true;
  }

  @Override
  public boolean close() {
    for (Operator op : _dataSourceOps) {
      op.close();
    }
    return true;
  }

  @Override
  public Block nextBlock() {
    for (int i = 0; i < _dataSourceOps.size(); ++i) {
      _blocks[i] = _dataSourceOps.get(i).nextBlock();
    }
    if (_blocks[0] != null) {
      for (int i = 0; i < _blocks.length; ++i) {
        _blockValIterators[i] = _blocks[i].getBlockValueSet().iterator();
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
