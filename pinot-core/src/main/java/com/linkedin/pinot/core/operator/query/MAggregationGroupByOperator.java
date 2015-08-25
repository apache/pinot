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
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
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
 * MAggregationGroupByOperator will apply AggregationInfos and GroupBy query to a given IndexSegment.
 * For each aggregation function, there will be a corresponding AggregationFunctionGroupByOperator just focus on it.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 *
 */
public class MAggregationGroupByOperator extends BaseOperator {

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfoList;
  private final MProjectionOperator _projectionOperator;
  private final GroupBy _groupBy;

  private List<AggregationFunctionGroupByOperator> _aggregationFunctionGroupByOperatorList;

  public MAggregationGroupByOperator(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList,
      GroupBy groupBy, Operator projectionOperator,
      List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList) {
    _aggregationInfoList = aggregationInfoList;
    _indexSegment = indexSegment;
    _groupBy = groupBy;
    _projectionOperator = (MProjectionOperator) projectionOperator;
    _aggregationFunctionGroupByOperatorList = aggregationFunctionGroupByOperatorList;
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    for (AggregationFunctionGroupByOperator op : _aggregationFunctionGroupByOperatorList) {
      op.open();
    }
    return true;
  }

  @Override
  public Block getNextBlock() {
    final long startTime = System.currentTimeMillis();
    List<Map<String, Serializable>> aggregationGroupByResults = new ArrayList<Map<String, Serializable>>();

    long numDocsScanned = 0;
    while (_projectionOperator.nextBlock() != null) {
      for (int i = 0; i < _aggregationFunctionGroupByOperatorList.size(); ++i) {
        _aggregationFunctionGroupByOperatorList.get(i).nextBlock();
      }
      numDocsScanned +=
          ((DocIdSetBlock) (_projectionOperator.getCurrentBlock().getDocIdSetBlock())).getSearchableLength();
    }

    for (int i = 0; i < _aggregationFunctionGroupByOperatorList.size(); ++i) {
      aggregationGroupByResults.add(_aggregationFunctionGroupByOperatorList.get(i).getAggregationGroupByResult());
    }
    final IntermediateResultsBlock resultBlock =
        new IntermediateResultsBlock(AggregationFunctionFactory.getAggregationFunction(_aggregationInfoList),
            aggregationGroupByResults, true);
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
    return "MAggregationGroupByOperator";
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    for (AggregationFunctionGroupByOperator op : _aggregationFunctionGroupByOperatorList) {
      op.close();
    }
    return true;
  }

}
