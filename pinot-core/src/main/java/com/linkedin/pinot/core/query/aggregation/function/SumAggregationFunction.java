package com.linkedin.pinot.core.query.aggregation.function;

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
import com.linkedin.pinot.core.segment.index.readers.DictionaryReader;


/**
 * This function will take a column and do sum on that.
 *
 */
public class SumAggregationFunction implements AggregationFunction<Double, Double> {

  private String _sumByColumn;

  public SumAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _sumByColumn = aggregationInfo.getAggregationParams().get("column");

  }

  @Override
  public Double aggregate(Block docIdSetBlock, Block[] block) {
    double ret = 0;
    int docId = 0;
    DictionaryReader dictionaryReader = block[0].getMetadata().getDictionary();
    BlockDocIdIterator docIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();

    while ((docId = docIdIterator.next()) != Constants.EOF) {
      blockValIterator.skipTo(docId);
      ret += dictionaryReader.getDoubleValue(blockValIterator.nextIntVal());
    }
    return ret;
  }

  @Override
  public Double aggregate(Double mergedResult, int docId, Block[] block) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) block[0].getBlockValueSet().iterator();
    blockValIterator.skipTo(docId);
    if (mergedResult == null) {
      return block[0].getMetadata().getDictionary().getDoubleValue(blockValIterator.nextIntVal());
    }
    return mergedResult + block[0].getMetadata().getDictionary().getDoubleValue(blockValIterator.nextIntVal());
  }

  @Override
  public Double aggregate(BlockValIterator[] blockValIterators) {
    double ret = 0;
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) blockValIterators[0];
    while (blockValIterator.hasNext()) {
      ret += blockValIterator.nextDoubleVal();
    }
    return ret;
  }

  @Override
  public Double aggregate(Double oldValue, BlockValIterator[] blockValIterators) {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) blockValIterators[0];
    if (oldValue == null) {
      return blockValIterator.nextDoubleVal();
    }
    return oldValue + blockValIterator.nextDoubleVal();
  }

  @Override
  public List<Double> combine(List<Double> aggregationResultList, CombineLevel combineLevel) {
    double combinedResult = 0;
    for (double aggregationResult : aggregationResultList) {
      combinedResult += aggregationResult;
    }
    aggregationResultList.clear();
    aggregationResultList.add(combinedResult);
    return aggregationResultList;
  }

  @Override
  public Double combineTwoValues(Double aggregationResult0, Double aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return aggregationResult0 + aggregationResult1;
  }

  @Override
  public Double reduce(List<Double> combinedResult) {
    double reducedResult = 0;
    for (double combineResult : combinedResult) {
      reducedResult += combineResult;
    }
    return reducedResult;
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        finalAggregationResult = 0.0;
      }
      return new JSONObject().put("value", String.format("%.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.DOUBLE;
  }

  @Override
  public String getFunctionName() {
    return "sum_" + _sumByColumn;
  }

}
