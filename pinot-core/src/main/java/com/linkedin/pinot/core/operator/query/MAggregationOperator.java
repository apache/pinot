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
import com.linkedin.pinot.core.operator.DocIdSetBlock;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


/**
 * This MAggregationOperator will take care of applying multiple aggregation functions
 * to a given IndexSegment.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 * @author xiafu
 *
 */
public class MAggregationOperator implements Operator {

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
  public Block nextBlock() {
    List<Serializable> aggregationResults = new ArrayList<Serializable>();
    for (int i = 0; i < _aggregationFunctionOperatorList.size(); ++i) {
      aggregationResults.add(null);
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
    resultBlock.setTotalDocs(_indexSegment.getSegmentMetadata().getTotalDocs());
    resultBlock.setTimeUsedMs(System.currentTimeMillis() - startTime);
    return resultBlock;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
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
