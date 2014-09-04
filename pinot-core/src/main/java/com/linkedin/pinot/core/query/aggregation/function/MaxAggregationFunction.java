package com.linkedin.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.NoSuchElementException;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.utils.IntArray;


public class MaxAggregationFunction implements AggregationFunction<Double, Double> {

  private String _maxColumnName;

  public MaxAggregationFunction() {

  }

  @Override
  public void init(AggregationInfo aggregationInfo) {
    _maxColumnName = aggregationInfo.getAggregationParams().get("column");
  }

  @Override
  public Double aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment) {
    double maxValue = Double.NEGATIVE_INFINITY;
    double tempValue;
    ColumnarReader columnarReader = indexSegment.getColumnarReader(_maxColumnName);
    for (int i = 0; i < docIdCount; ++i) {
      tempValue = columnarReader.getDoubleValue(docIds.get(i));
      if (tempValue > maxValue) {
        maxValue = tempValue;
      }
    }
    return maxValue;
  }

  @Override
  public Double aggregate(Double currentResult, int docId, IndexSegment indexSegment) {
    ColumnarReader columnarReader = indexSegment.getColumnarReader(_maxColumnName);
    double tempValue = columnarReader.getDoubleValue(docId);
    if (currentResult == null) {
      currentResult = new Double(tempValue);
      return currentResult;
    }
    if (tempValue > currentResult) {
      currentResult = tempValue;
    }
    return currentResult;
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
