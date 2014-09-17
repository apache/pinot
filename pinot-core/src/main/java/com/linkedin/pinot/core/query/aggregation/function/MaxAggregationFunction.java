package com.linkedin.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.NoSuchElementException;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;


public class MaxAggregationFunction implements AggregationFunction<Double, Double> {

  private String _maxColumnName;

  public MaxAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _maxColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public Double aggregate(BlockValIterator[] blockValIterators) {
    double ret = Double.NEGATIVE_INFINITY;
    double tmp = 0;
    while (blockValIterators[0].hasNext()) {
      tmp = blockValIterators[0].nextDoubleVal();
      if (tmp > ret) {
        ret = tmp;
      }
    }
    return ret;
  }

  @Override
  public Double aggregate(Double oldValue, BlockValIterator[] blockValIterators) {
    if (oldValue == null) {
      return blockValIterators[0].nextDoubleVal();
    }
    double tmp = blockValIterators[0].nextDoubleVal();
    if (tmp > oldValue) {
      return tmp;
    }
    return oldValue;
  }

  @Override
  public List<Double> combine(List<Double> aggregationResultList, CombineLevel combineLevel) {
    double maxValue = Double.NEGATIVE_INFINITY;
    for (double aggregationResult : aggregationResultList) {
      if (maxValue < aggregationResult) {
        maxValue = aggregationResult;
      }
    }
    aggregationResultList.clear();
    aggregationResultList.add(maxValue);
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
    return (aggregationResult0 > aggregationResult1) ? aggregationResult0 : aggregationResult1;
  }

  @Override
  public Double reduce(List<Double> combinedResultList) {
    double maxValue = Double.NEGATIVE_INFINITY;
    for (double combinedResult : combinedResultList) {
      if (maxValue < combinedResult) {
        maxValue = combinedResult;
      }
    }
    return maxValue;
  }

  @Override
  public JSONObject render(Double finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        throw new NoSuchElementException("Final result is null!");
      }
      return new JSONObject().put("max", String.format("%1.5f", finalAggregationResult));
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
    return "max_" + _maxColumnName;
  }

}
