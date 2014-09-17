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
import com.linkedin.pinot.core.operator.BDocIdSetOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;


/**
 * MAggregationGroupByOperator will apply AggregationInfos and GroupBy query to a given IndexSegment.
 * For each aggregation function, there will be a corresponding AggregationFunctionGroupByOperator just focus on it.
 * nextBlock() will return an IntermediateResultBlock for the given IndexSegment.
 *
 * @author xiafu
 *
 */
public class MAggregationGroupByOperator implements Operator {

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfoList;
  private final BDocIdSetOperator _docIdSetOperator;
  private final GroupBy _groupBy;

  private List<AggregationFunctionGroupByOperator> _aggregationFunctionGroupByOperatorList;

  public MAggregationGroupByOperator(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList,
      GroupBy groupBy, BDocIdSetOperator docIdSetOperator,
      List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList) {
    _aggregationInfoList = aggregationInfoList;
    _indexSegment = indexSegment;
    _groupBy = groupBy;
    _docIdSetOperator = docIdSetOperator;
    _aggregationFunctionGroupByOperatorList = aggregationFunctionGroupByOperatorList;
  }

  @Override
  public boolean open() {
    if (_docIdSetOperator != null) {
      _docIdSetOperator.open();
    }
    for (AggregationFunctionGroupByOperator op : _aggregationFunctionGroupByOperatorList) {
      op.open();
    }
    return true;
  }

  @Override
  public Block nextBlock() {
    final long startTime = System.currentTimeMillis();
    List<Map<String, Serializable>> aggregationGroupByResults = new ArrayList<Map<String, Serializable>>();

    long numDocsScanned = 0;
    while (_docIdSetOperator.nextBlock() != null) {
      for (int i = 0; i < _aggregationFunctionGroupByOperatorList.size(); ++i) {
        _aggregationFunctionGroupByOperatorList.get(i).nextBlock();
      }
      numDocsScanned += _docIdSetOperator.getCurrentBlockSize();
    }

    for (int i = 0; i < _aggregationFunctionGroupByOperatorList.size(); ++i) {
      aggregationGroupByResults.add(_aggregationFunctionGroupByOperatorList.get(i).getAggregationGroupByResult());
    }
    final IntermediateResultsBlock resultBlock =
        new IntermediateResultsBlock(AggregationFunctionFactory.getAggregationFunction(_aggregationInfoList),
            aggregationGroupByResults, true);
    resultBlock.setNumDocsScanned(numDocsScanned);
    resultBlock.setTotalDocs(_indexSegment.getSegmentMetadata().getTotalDocs());
    resultBlock.setTimeUsedMs(System.currentTimeMillis() - startTime);
    //    System.out.println("TimeUsed to search segment " + _indexSegment.getSegmentName() + " is "
    //        + resultBlock.getTimeUsedMs());
    return resultBlock;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    if (_docIdSetOperator != null) {
      _docIdSetOperator.close();
    }
    for (AggregationFunctionGroupByOperator op : _aggregationFunctionGroupByOperatorList) {
      op.close();
    }
    return true;
  }

}
