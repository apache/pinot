package com.linkedin.pinot.core.query.aggregation.function;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.DictionaryReader;


/**
 * This function will take a column and do sum on that.
 *
 */
public class AvgAggregationFunction implements AggregationFunction<AvgPair, Double> {

  private String _avgByColumn;

  public AvgAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _avgByColumn = aggregationInfo.getAggregationParams().get("column");

  }

  @Override
  public AvgPair aggregate(Block docIdSetBlock, Block[] block) {
    double ret = 0;
    long cnt = 0;
    int docId = 0;
    DictionaryReader dictionaryReader = block[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      blockValIterator.skipTo(docId);
      ret += dictionaryReader.getDoubleValue(blockValIterator.nextIntVal());
      cnt++;
    }
    return new AvgPair(ret, cnt);
  }

  @Override
  public AvgPair aggregate(AvgPair mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    blockValIterator.skipTo(docId);
    if (mergedResult == null) {
      return new AvgPair(block[0].getMetadata().getDictionary().getDoubleValue(blockValIterator.nextIntVal()), (long) 1);
    }
    return new AvgPair(mergedResult.getFirst() + block[0].getMetadata().getDictionary().getDoubleValue(blockValIterator.nextIntVal()), mergedResult.getSecond() + 1);
  }

  @Override
  public AvgPair aggregate(BlockValIterator[] blockValIterators) {
    double ret = 0;
    long cnt = 0;
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) blockValIterators[0];
    while (blockValIterator.hasNext()) {
      ret += blockValIterator.nextDoubleVal();
      cnt++;
    }
    return new AvgPair(ret, cnt);
  }

  @Override
  public AvgPair aggregate(AvgPair oldValue, BlockValIterator[] blockValIterators) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) blockValIterators[0];
    if (oldValue == null) {
      return new AvgPair(blockValIterator.nextDoubleVal(), (long) 1);
    }
    return new AvgPair(oldValue.getFirst() + blockValIterator.nextDoubleVal(), oldValue.getSecond() + 1);
  }

  @Override
  public List<AvgPair> combine(List<AvgPair> aggregationResultList, CombineLevel combineLevel) {
    double combinedSumResult = 0;
    long combinedCntResult = 0;
    for (AvgPair aggregationResult : aggregationResultList) {
      combinedSumResult += aggregationResult.getFirst();
      combinedCntResult += aggregationResult.getSecond();
    }
    aggregationResultList.clear();
    aggregationResultList.add(new AvgPair(combinedSumResult, combinedCntResult));
    return aggregationResultList;
  }

  @Override
  public AvgPair combineTwoValues(AvgPair aggregationResult0, AvgPair aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return new AvgPair(aggregationResult0.getFirst() + aggregationResult1.getFirst(), aggregationResult0.getSecond()
        + aggregationResult1.getSecond());
  }

  @Override
  public Double reduce(List<AvgPair> combinedResult) {

    double reducedSumResult = 0;
    long reducedCntResult = 0;
    for (AvgPair combineResult : combinedResult) {
      reducedSumResult += combineResult.getFirst();
      reducedCntResult += combineResult.getSecond();
    }
    if (reducedCntResult > 0) {
      double avgResult = reducedSumResult / reducedCntResult;
      return avgResult;
    } else {
      return Double.NaN;
    }
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      if ((finalAggregationResult == null) || (finalAggregationResult == Double.NaN)) {
        return new JSONObject().put("value", finalAggregationResult);
      }
      return new JSONObject().put("value", String.format("%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.OBJECT;
  }

  @Override
  public String getFunctionName() {
    return "avg_" + _avgByColumn;
  }

  public class AvgPair extends Pair<Double, Long> implements Comparable<AvgPair>, Serializable {
    public AvgPair(Double first, Long second) {
      super(first, second);
    }

    @Override
    public int compareTo(AvgPair o) {
      if (getSecond() == 0) {
        return -1;
      }
      if (o.getSecond() == 0) {
        return 1;
      }
      if ((getFirst() / getSecond()) > (o.getFirst() / o.getSecond())) {
        return 1;
      }
      if ((getFirst() / getSecond()) < (o.getFirst() / o.getSecond())) {
        return -1;
      }
      return 0;
    }

    @Override
    public String toString() {
      return new DecimalFormat("####################.##########").format((getFirst() / getSecond()));
    }
  }

  public AvgPair getAvgPair(double first, long second) {
    return new AvgPair(first, second);
  }
}
