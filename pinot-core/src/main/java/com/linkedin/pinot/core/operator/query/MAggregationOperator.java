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
import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.block.query.AggregationResultBlock;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.DocIdSetBlock;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


/**
 * This MAggregationOperator will take care of applying multiple aggregation functions
 * to a given IndexSegment.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 *
 */
public class MAggregationOperator extends BaseOperator {

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfoList;
  private final MProjectionOperator _projectionOperator;

  private List<BAggregationFunctionOperator> _aggregationFunctionOperatorList;

  public MAggregationOperator(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList,
      MProjectionOperator projectionOperator, List<BAggregationFunctionOperator> aggregationFunctionOperatorList) {
    _aggregationInfoList = aggregationInfoList;
    _indexSegment = indexSegment;
    _projectionOperator = projectionOperator;
    _aggregationFunctionOperatorList = aggregationFunctionOperatorList;
  }

  @Override
  public boolean open() {
    if (_projectionOperator != null) {
      _projectionOperator.open();
    }
    for (BAggregationFunctionOperator op : _aggregationFunctionOperatorList) {
      op.open();
    }
    return true;
  }

  @Override
  public Block getNextBlock() {
    List<Serializable> aggregationResults = new ArrayList<Serializable>();
    for (int i = 0; i < _aggregationFunctionOperatorList.size(); ++i) {
      aggregationResults.add(AggregationFunctionFactory.get(_aggregationInfoList.get(i), true).getDefaultValue());
    }
    final long startTime = System.currentTimeMillis();
    long numDocsScanned = 0;
    while (_projectionOperator.nextBlock() != null) {
      for (int i = 0; i < _aggregationFunctionOperatorList.size(); ++i) {
        AggregationResultBlock block = (AggregationResultBlock) _aggregationFunctionOperatorList.get(i).nextBlock();
        if (block != null) {
          aggregationResults.set(
              i,
              _aggregationFunctionOperatorList.get(i).getAggregationFunction()
                  .combineTwoValues(aggregationResults.get(i), block.getAggregationResult()));
        }
      }
      numDocsScanned +=
          ((DocIdSetBlock) (_projectionOperator.getCurrentBlock().getDocIdSetBlock())).getSearchableLength();
    }

    final IntermediateResultsBlock resultBlock =
        new IntermediateResultsBlock(AggregationFunctionFactory.getAggregationFunction(_aggregationInfoList),
            aggregationResults);
    resultBlock.setNumDocsScanned(numDocsScanned);
    resultBlock.setTotalDocs(_indexSegment.getTotalDocs());
    resultBlock.setTimeUsedMs(System.currentTimeMillis() - startTime);
    return resultBlock;
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    return "MAggregationOperator";
  }

  @Override
  public boolean close() {
    if (_projectionOperator != null) {
      _projectionOperator.close();
    }
    for (BAggregationFunctionOperator op : _aggregationFunctionOperatorList) {
      op.close();
    }
    return true;
  }

}
