/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.CountAggregationFunction;


/**
 * AggregationFunctionGroupByOperator will take all the needed info for the implementations.
 *
 *
 */
public abstract class AggregationFunctionGroupByOperator extends BaseOperator {

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
      Operator projectionOperator, boolean hasDictionary) {
    _aggregationFunction = AggregationFunctionFactory.get(aggregationInfo, hasDictionary);
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
  public abstract Block getNextBlock();

  @Override
  public abstract Block getNextBlock(BlockId BlockId);

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
