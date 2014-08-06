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


public class MinAggregationFunction implements AggregationFunction {
  private String _minColumnName;

  public MinAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _minColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public AggregationResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    double minValue = Double.POSITIVE_INFINITY;
    double tempValue;
    ColumnarReader columnarReader = indexSegment.getColumnarReader(_minColumnName);
    for (int i = 0; i < docIdCount; ++i) {
      tempValue = columnarReader.getDoubleValue(docIds.get(i));
      if (tempValue < minValue) {
        minValue = tempValue;
      }
    }
    return new DoubleContainer(minValue);
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
    DoubleContainer result = (DoubleContainer) (aggregationResultList.get(0));

    for (int i = 1; i < aggregationResultList.size(); ++i) {
      if (((DoubleContainer) aggregationResultList.get(i)).get() < result.get()) {
        result = (DoubleContainer) aggregationResultList.get(i);
      }
    }
    return result;
  }

  @Override
  public JSONObject render(AggregationResult finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        throw new NoSuchElementException("Final result is null!");
      }
      return new JSONObject().put("min", String.format("%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
