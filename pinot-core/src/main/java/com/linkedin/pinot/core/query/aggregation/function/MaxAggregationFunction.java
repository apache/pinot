package com.linkedin.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.NoSuchElementException;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.AggregationResult._Fields;
import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
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
  public AggregationResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    double maxValue = Double.NEGATIVE_INFINITY;
    double tempValue;
    ColumnarReader columnarReader = indexSegment.getColumnarReader(_maxColumnName);
    for (int i = 0; i < docIdCount; ++i) {
      tempValue = columnarReader.getDoubleValue(docIds.get(i));
      if (tempValue > maxValue) {
        maxValue = tempValue;
      }
    }
    return new AggregationResult(_Fields.DOUBLE_VAL, maxValue);
  }

  @Override
  public AggregationResult combine(List<AggregationResult> aggregationResultList, CombineLevel combineLevel) {
    return reduce(aggregationResultList);
  }

  @Override
  public AggregationResult reduce(List<AggregationResult> aggregationResultList) {
    AggregationResult maxValue = aggregationResultList.get(0);
    for (int i = 1; i < aggregationResultList.size(); ++i) {
      if ((aggregationResultList.get(i)).getDoubleVal() > maxValue.getDoubleVal()) {
        maxValue = aggregationResultList.get(i);
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
