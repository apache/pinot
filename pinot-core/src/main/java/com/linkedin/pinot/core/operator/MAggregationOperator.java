package com.linkedin.pinot.core.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.block.aggregation.AggregationResultBlock;
import com.linkedin.pinot.core.block.aggregation.IntermediateResultsBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


/**
 * This UAggregationAndSelectionOperator will take care of applying a request
 * with both aggregation and selection to one IndexSegment.
 *
 * @author xiafu
 *
 */
public class MAggregationOperator implements Operator {

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfoList;
  private final BIndexSegmentProjectionOperator _projectionOperator;

  private List<BAggregationFunctionOperator> _aggregationFunctionOperatorList;

  public MAggregationOperator(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList,
      BIndexSegmentProjectionOperator projectionOperator,
      List<BAggregationFunctionOperator> aggregationFunctionOperatorList) {
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
        BAggregationFunctionOperator aggregationFunctionOperator = _aggregationFunctionOperatorList.get(i);
        AggregationResultBlock block = (AggregationResultBlock) aggregationFunctionOperator.nextBlock();
        aggregationResults.set(
            i,
            aggregationFunctionOperator.getAggregationFunction().combineTwoValues(aggregationResults.get(i),
                block.getAggregationResult()));
      }
      numDocsScanned += _projectionOperator.getCurrentBlockSize();
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
