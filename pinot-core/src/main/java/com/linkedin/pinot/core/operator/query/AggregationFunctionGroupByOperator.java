package com.linkedin.pinot.core.operator.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.CountAggregationFunction;


/**
 * AggregationFunctionGroupByOperator will take all the needed info for the implementations.
 * 
 * @author xiafu
 *
 */
public abstract class AggregationFunctionGroupByOperator implements Operator {

  protected final AggregationFunction _aggregationFunction;
  protected final Operator _projectionOperator;
  protected final BlockValIterator[] _aggregationFunctionBlockValIterators;
  protected final BlockValIterator[] _groupByBlockValIterators;
  protected final GroupBy _groupBy;
  protected final String[] _aggregationColumns;
  protected final Map<String, Serializable> _aggregateGroupedValue = new HashMap<String, Serializable>();

  public AggregationFunctionGroupByOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator) {
    _aggregationFunction = AggregationFunctionFactory.get(aggregationInfo);
    _projectionOperator = projectionOperator;
    _groupBy = groupBy;
    if (_aggregationFunction instanceof CountAggregationFunction) {
      _aggregationColumns = new String[0];
    } else {
      String columns = aggregationInfo.getAggregationParams().get("column").trim();
      _aggregationColumns = columns.split(",");
    }

    _aggregationFunctionBlockValIterators = new BlockValIterator[_aggregationColumns.length];

    _groupByBlockValIterators = new BlockValIterator[_groupBy.getColumnsSize()];
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public abstract Block nextBlock();

  @Override
  public abstract Block nextBlock(BlockId BlockId);

  @Override
  public boolean close() {
    _projectionOperator.close();
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
