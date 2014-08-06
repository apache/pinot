package com.linkedin.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.NoSuchElementException;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.query.response.AggregationResult;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.data.DoubleContainer;
import com.linkedin.pinot.core.query.utils.IntArray;


public class MaxAggregationFunction implements AggregationFunction {

  private String _maxColumnName;

  public MaxAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _maxColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public DoubleContainer aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    double maxValue = Double.NEGATIVE_INFINITY;
    double tempValue;
    ColumnarReader columnarReader = indexSegment.getColumnarReader(_maxColumnName);
    for (int i = 0; i < docIdCount; ++i) {
      tempValue = columnarReader.getDoubleValue(docIds.get(i));
      if (tempValue > maxValue) {
        maxValue = tempValue;
      }
    }
    return new DoubleContainer(maxValue);
  }

  @Override
  public List<AggregationResult> combine(List<AggregationResult> aggregationResultList, CombineLevel combineLevel) {
    AggregationResult result = reduce(aggregationResultList);
    aggregationResultList.clear();
    aggregationResultList.add(result);
    return aggregationResultList;
  }

  @Override
  public AggregationResult reduce(List<AggregationResult> aggregationResultList) {
    DoubleContainer maxValue = (DoubleContainer) aggregationResultList.get(0);
    for (int i = 1; i < aggregationResultList.size(); ++i) {
      if (((DoubleContainer) aggregationResultList.get(i)).get() > maxValue.get()) {
        maxValue = (DoubleContainer) aggregationResultList.get(i);
      }
    }
    return maxValue;
  }

  @Override
  public JSONObject render(AggregationResult finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        throw new NoSuchElementException("Final result is null!");
      }
      return new JSONObject().put("max", String.format("%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
