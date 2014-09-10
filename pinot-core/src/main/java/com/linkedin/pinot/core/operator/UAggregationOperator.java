package com.linkedin.pinot.core.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
public class UAggregationOperator implements Operator {

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfoList;
  private final UProjectionOperator _projectionOperator;

  private ArrayList<UAggregationFunctionOperator> _aggregationFunctionOperatorList;

  public UAggregationOperator(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList,
      Operator filterOperator) {
    _aggregationInfoList = aggregationInfoList;
    _indexSegment = indexSegment;
    _aggregationFunctionOperatorList = new ArrayList<UAggregationFunctionOperator>();

    _projectionOperator = new UProjectionOperator(filterOperator, _indexSegment);
    for (AggregationInfo aggregationInfo : _aggregationInfoList) {
      _aggregationFunctionOperatorList.add(new UAggregationFunctionOperator(aggregationInfo, _projectionOperator));
    }
  }

  private List<String> getProjectedColumns(List<AggregationInfo> aggregationsInfo) {
    List<String> projectedColumns = new ArrayList<String>();
    for (AggregationInfo aggInfo : aggregationsInfo) {
      projectedColumns.addAll(Arrays.asList(aggInfo.getAggregationParams().get("column").trim().split(",")));
    }
    return projectedColumns;
  }

  @Override
  public boolean open() {
    if (_projectionOperator != null) {
      _projectionOperator.open();
    }
    for (UAggregationFunctionOperator op : _aggregationFunctionOperatorList) {
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
        UAggregationFunctionOperator aggregationFunctionOperator = _aggregationFunctionOperatorList.get(i);
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
    for (UAggregationFunctionOperator op : _aggregationFunctionOperatorList) {
      op.close();
    }
    return true;
  }

}
