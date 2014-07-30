package com.linkedin.pinot.query.aggregation.function;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.query.aggregation.AggregationFunction;
import com.linkedin.pinot.query.aggregation.AggregationResult;
import com.linkedin.pinot.query.aggregation.CombineLevel;
import com.linkedin.pinot.query.aggregation.data.LongContainer;
import com.linkedin.pinot.query.request.AggregationInfo;
import com.linkedin.pinot.query.utils.IntArray;


/**
 * This function will take a column and do sum on that.
 *
 */
public class CountAggregationFunction implements AggregationFunction {

  public CountAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo){

  }

  @Override
  public LongContainer aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    return new LongContainer(docIdCount);
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
    LongContainer result = (LongContainer) (aggregationResultList.get(0));

    for (int i = 1; i < aggregationResultList.size(); ++i) {
      result.increment((LongContainer) aggregationResultList.get(i));
    }
    return result;
  }

  @Override
  public JSONObject render(AggregationResult finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        finalAggregationResult = new LongContainer(0);
      }
      return new JSONObject().put("count", String.format("%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
