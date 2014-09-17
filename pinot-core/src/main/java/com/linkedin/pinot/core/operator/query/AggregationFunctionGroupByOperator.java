package com.linkedin.pinot.core.operator.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


/**
 * AggregationFunctionGroupByOperator will take all the needed info for the implementations.
 * 
 * @author xiafu
 *
 */
public abstract class AggregationFunctionGroupByOperator implements Operator {

  protected final AggregationFunction _aggregationFunction;
  protected final List<Operator> _dataSourceOpsList;
  protected final BlockValIterator[] _aggregationFunctionBlockValIterators;
  protected final BlockValIterator[] _groupByBlockValIterators;
  protected final GroupBy _groupBy;
  protected final Map<String, Serializable> _aggregateGroupedValue = new HashMap<String, Serializable>();

  public AggregationFunctionGroupByOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      List<Operator> dataSourceOpsList) {
    _aggregationFunction = AggregationFunctionFactory.get(aggregationInfo);
    _dataSourceOpsList = dataSourceOpsList;
    _groupBy = groupBy;
    _aggregationFunctionBlockValIterators = new BlockValIterator[_dataSourceOpsList.size() - _groupBy.getColumnsSize()];
    _groupByBlockValIterators = new BlockValIterator[_groupBy.getColumnsSize()];
  }

  @Override
  public boolean open() {
    for (Operator op : _dataSourceOpsList) {
      op.open();
    }
    return true;
  }

  @Override
  public abstract Block nextBlock();

  @Override
  public abstract Block nextBlock(BlockId BlockId);

  @Override
  public boolean close() {
    for (Operator op : _dataSourceOpsList) {
      op.close();
    }
    return true;
  }

  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

  public Map<String, Serializable> getAggregationGroupByResult() {
    return _aggregateGroupedValue;
  }

  public GroupBy getGroupBy() {
    return _groupBy;
  }
}
