package com.linkedin.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;


public class AggregationFunctionFactory {

  public static AggregationFunction get(AggregationInfo aggregationInfo, boolean hasDictionary) {
    try {
      String aggregationKey = aggregationInfo.getAggregationType();
      if (hasDictionary) {
        AggregationFunction aggregationFunction = AggregationFunctionRegistry.get(aggregationKey);
        aggregationFunction.init(aggregationInfo);
        return aggregationFunction;
      } else {
        AggregationFunction aggregationFunction = AggregationFunctionRegistry.getAggregationNoDictionaryFunction(aggregationKey);
        aggregationFunction.init(aggregationInfo);
        return aggregationFunction;
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static List<AggregationFunction> getAggregationFunction(BrokerRequest query) {
    List<AggregationFunction> aggregationFunctions = new ArrayList<AggregationFunction>();
    for (com.linkedin.pinot.common.request.AggregationInfo agg : query.getAggregationsInfo()) {
      AggregationFunction agg1 = AggregationFunctionFactory.get(agg, true);
      aggregationFunctions.add(agg1);
    }
    return aggregationFunctions;
  }

  public static List<AggregationFunction> getAggregationFunction(List<AggregationInfo> aggregationInfos) {
    List<AggregationFunction> aggregationFunctions = new ArrayList<AggregationFunction>();
    for (AggregationInfo agg : aggregationInfos) {
      AggregationFunction agg1 = AggregationFunctionFactory.get(agg, true);
      aggregationFunctions.add(agg1);
    }
    return aggregationFunctions;
  }

}
