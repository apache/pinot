package com.linkedin.pinot.core.query.aggregation.function;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.utils.IntArray;


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
  public Double aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    double result = 0;

    ColumnarReader columnarReader = indexSegment.getColumnarReader(_sumByColumn);
    for (int i = 0; i < docIdCount; ++i) {
      result += columnarReader.getDoubleValue(docIds.get(i));
    }
    return result;
  }

  @Override
  public Double aggregate(Double currentResult, int docId, IndexSegment indexSegment) {
    ColumnarReader columnarReader = indexSegment.getColumnarReader(_sumByColumn);
    if (currentResult == null) {
      currentResult = new Double(0);
    }
    currentResult += columnarReader.getDoubleValue(docId);
    return currentResult;
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
      return new JSONObject().put("sum", String.format("%.5f", finalAggregationResult));
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
