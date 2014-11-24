package com.linkedin.pinot.core.query.aggregation.function;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.block.intarray.DocIdSetBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;


/**
 * This function will take a column and do sum on that.
 *
 */
public class CountAggregationFunction implements AggregationFunction<Long, Long> {

  public CountAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {

  }

  @Override
  public Long aggregate(Block docIdSetBlock, Block[] block) {
    return (long) ((DocIdSetBlock) docIdSetBlock).getSearchableLength();
  }

  @Override
  public Long aggregate(Long mergedResult, Block[] block) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Long aggregate(BlockValIterator[] blockValIterators) {
    return (long) blockValIterators[0].size();
  }

  @Override
  public Long aggregate(Long oldValue, BlockValIterator[] _blockValIterators) {
    if (oldValue == null) {
      return (long) 1;
    } else {
      return (oldValue + 1);
    }
  }

  @Override
  public List<Long> combine(List<Long> aggregationResultList, CombineLevel combineLevel) {
    long combinedValue = 0;
    for (Long value : aggregationResultList) {
      combinedValue += value;
    }
    aggregationResultList.clear();
    aggregationResultList.add(combinedValue);
    return aggregationResultList;
  }

  @Override
  public Long combineTwoValues(Long aggregationResult0, Long aggregationResult1) {
    if (aggregationResult0 == null) {
      return aggregationResult1;
    }
    if (aggregationResult1 == null) {
      return aggregationResult0;
    }
    return aggregationResult0 + aggregationResult1;
  }

  @Override
  public Long reduce(List<Long> combinedResultList) {
    long reducedValue = 0;
    for (Long value : combinedResultList) {
      reducedValue += value;
    }
    return reducedValue;
  }

  @Override
  public JSONObject render(Long reduceResult) {
    try {
      if (reduceResult == null) {
        reduceResult = new Long(0);
      }
      return new JSONObject().put("value", reduceResult.toString());
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataType aggregateResultDataType() {
    return DataType.LONG;
  }

  @Override
  public String getFunctionName() {
    return "count_star";
  }

}
