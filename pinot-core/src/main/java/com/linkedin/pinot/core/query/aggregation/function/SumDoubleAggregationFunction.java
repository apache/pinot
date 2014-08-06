package com.linkedin.pinot.core.query.aggregation.function;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.AggregationResult._Fields;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.utils.IntArray;


public class SumDoubleAggregationFunction implements AggregationFunction {
  private String _sumByColumn;

  public SumDoubleAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _sumByColumn = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public AggregationResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    double result = 0;
    for (int i = 0; i < docIdCount; ++i) {
      result += indexSegment.getColumnarReader(_sumByColumn).getDoubleValue(docIds.get(i));
    }
    return new AggregationResult(_Fields.DOUBLE_VAL, result);
  }

  @Override
  public AggregationResult combine(List<AggregationResult> aggregationResultList, CombineLevel combineLevel) {
    return reduce(aggregationResultList);
  }

  @Override
  public AggregationResult reduce(List<AggregationResult> aggregationResultList) {
    double result = aggregationResultList.get(0).getDoubleVal();

    for (int i = 1; i < aggregationResultList.size(); ++i) {
      result += aggregationResultList.get(i).getDoubleVal();
    }
    return new AggregationResult(_Fields.DOUBLE_VAL, result);
  }

  @Override
  public JSONObject render(AggregationResult finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        finalAggregationResult = new AggregationResult(_Fields.DOUBLE_VAL, 0);
      }
      return new JSONObject().put("sum", String.format("%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
