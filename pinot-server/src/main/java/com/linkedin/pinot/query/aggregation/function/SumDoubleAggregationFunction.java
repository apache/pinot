package com.linkedin.pinot.query.aggregation.function;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.query.aggregation.AggregationFunction;
import com.linkedin.pinot.query.aggregation.AggregationResult;
import com.linkedin.pinot.query.aggregation.CombineLevel;
import com.linkedin.pinot.query.aggregation.data.DoubleContainer;
import com.linkedin.pinot.query.request.AggregationInfo;
import com.linkedin.pinot.query.utils.IntArray;


public class SumDoubleAggregationFunction implements AggregationFunction {
  private String _sumByColumn;

  public SumDoubleAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo){
    _sumByColumn = aggregationInfo.getParams().get("column");
  }

  @Override
  public AggregationResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    double result = 0;
    for (int i = 0; i < docIdCount; ++i) {
      result += indexSegment.getColumnarReader(_sumByColumn).getDoubleValue(docIds.get(i));
    }
    return new DoubleContainer(result);
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
      result.increment((DoubleContainer) aggregationResultList.get(i));
    }
    return result;
  }

  @Override
  public JSONObject render(AggregationResult finalAggregationResult) {
    try {
      if (finalAggregationResult == null) {
        finalAggregationResult = new DoubleContainer(0);
      }
      return new JSONObject().put("sum", String.format("%1.5f", finalAggregationResult));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
