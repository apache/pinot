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

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.block.query.AggregationResultBlock;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


/**
 * AggregationFunction may need multiple data sources.
 * nextBlock() will take the Iterators from data sources and send to AggregationFunction.
 *
 *
 */
public class BAggregationFunctionOperator extends BaseOperator {

  private final AggregationFunction _aggregationFunction;
  private final Block[] _blocks;
  // private final BlockValIterator[] _blockValIterators;
  private final String _columns[];
  private final Operator _projectionOperator;

  public BAggregationFunctionOperator(AggregationInfo aggregationInfo, Operator projectionOperator, boolean hasDictionary) {
    _aggregationFunction = AggregationFunctionFactory.get(aggregationInfo, hasDictionary);
    _projectionOperator = projectionOperator;
    if (aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
      _columns = new String[1];
      _columns[0] = null;
    } else {
      String columns = aggregationInfo.getAggregationParams().get("column").trim();
      _columns = columns.split(",");
    }
    _blocks = new Block[_columns.length];
    // _blockValIterators = new BlockValIterator[_columns.length];
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
  public Block getNextBlock() {
    ProjectionBlock block = (ProjectionBlock) _projectionOperator.nextBlock();
    if (block != null) {
      for (int i = 0; i < _blocks.length; ++i) {
        _blocks[i] = block.getBlock(_columns[i]);
      }
      return new AggregationResultBlock(_aggregationFunction.aggregate(block.getDocIdSetBlock(), _blocks));
    }
    return null;
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in UAggregationFunctionOperator");
  }

  @Override
  public String getOperatorName() {
    return "BAggregationFunctionOperator";
  }

  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

}
