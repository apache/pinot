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


/**
 * This function will take a column and do sum on that.
 *
 */
public class CountAggregationFunction implements AggregationFunction {

  public CountAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {

  }

  @Override
  public AggregationResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    return new AggregationResult(_Fields.LONG_VAL, (long) docIdCount);
  }

  @Override
  public AggregationResult combine(List<AggregationResult> aggregationResultList, CombineLevel combineLevel) {
    return reduce(aggregationResultList);
  }

  @Override
  public AggregationResult reduce(List<AggregationResult> aggregationResultList) {
    long result = 0;
    for (AggregationResult aggregationResult : aggregationResultList) {
      result += aggregationResult.getLongVal();
    }
    return new AggregationResult(_Fields.LONG_VAL, result);
  }

  @Override
  public JSONObject render(AggregationResult finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        finalAggregationResult = new AggregationResult(_Fields.LONG_VAL, 0);
      }
      return new JSONObject().put("count", String.format("%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
