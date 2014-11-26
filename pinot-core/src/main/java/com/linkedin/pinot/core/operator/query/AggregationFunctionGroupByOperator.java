package com.linkedin.pinot.core.operator.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
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
  protected final Block[] _aggregationFunctionBlocks;
  protected final Block[] _groupByBlocks;
  protected final boolean[] _isSingleValueGroupByColumn;
  protected final GroupBy _groupBy;
  protected final String[] _aggregationColumns;
  protected final Map<String, Serializable> _aggregateGroupedValue = new HashMap<String, Serializable>();
  protected boolean _isGroupByColumnsContainMultiValueColumn = false;

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

    _aggregationFunctionBlocks = new Block[_aggregationColumns.length];
    for (int i = 0; i < _aggregationColumns.length; ++i) {
      String aggregationColumn = _aggregationColumns[i];
      _aggregationFunctionBlocks[i] =
          ((UReplicatedProjectionOperator) _projectionOperator).getProjectionOperator()
              .getDataSource(aggregationColumn).nextBlock(new BlockId(0));
    }
    _groupByBlocks = new Block[_groupBy.getColumnsSize()];
    _isSingleValueGroupByColumn = new boolean[_groupBy.getColumnsSize()];
    for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
      String groupByColumn = _groupBy.getColumns().get(i);
      _groupByBlocks[i] =
          ((UReplicatedProjectionOperator) _projectionOperator).getProjectionOperator().getDataSource(groupByColumn)
              .nextBlock(new BlockId(0));
      _isSingleValueGroupByColumn[i] = _groupByBlocks[i].getMetadata().isSingleValue();
      if (!_isSingleValueGroupByColumn[i]) {
        _isGroupByColumnsContainMultiValueColumn = true;
      }
    }
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
