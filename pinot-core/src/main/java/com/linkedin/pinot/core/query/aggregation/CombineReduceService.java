package com.linkedin.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.query.response.AggregationResult;


/**
 * CombineReduceService will take a list of intermediate results and merge them.
 *
 */
public class CombineReduceService {

  private static Logger LOGGER = LoggerFactory.getLogger(CombineReduceService.class);

  public static Map<AggregationFunction, List<AggregationResult>> combine(
      Map<AggregationFunction, List<AggregationResult>> aggregationResultsMap, CombineLevel combineLevel) {
    for (AggregationFunction aggregationFunction : aggregationResultsMap.keySet()) {
      List<AggregationResult> combinedResults =
          aggregationFunction.combine(aggregationResultsMap.get(aggregationFunction), combineLevel);
      aggregationResultsMap.put(aggregationFunction, combinedResults);
    }
    return aggregationResultsMap;
  }

  public static List<List<AggregationResult>> combine(List<AggregationFunction> aggregationFunctionList,
      List<List<AggregationResult>> aggregationResultsList, CombineLevel combineLevel) {
    List<List<AggregationResult>> combinedResultsList = new ArrayList<List<AggregationResult>>();
    if (aggregationResultsList == null) {
      return null;
    }
    for (int i = 0; i < aggregationFunctionList.size(); ++i) {
      if (aggregationResultsList.get(i) != null) {
        AggregationFunction aggregationFunction = aggregationFunctionList.get(i);
        List<AggregationResult> combinedResults =
            aggregationFunction.combine(aggregationResultsList.get(i), combineLevel);
        combinedResultsList.add(combinedResults);
      } else {
        combinedResultsList.add(null);
      }
    }
    return combinedResultsList;
  }

  public static Map<AggregationFunction, AggregationResult> reduce(
      Map<AggregationFunction, List<AggregationResult>> aggregationResultsMap) {
    Map<AggregationFunction, AggregationResult> reduceResults = new HashMap<AggregationFunction, AggregationResult>();
    for (AggregationFunction aggregationFunction : aggregationResultsMap.keySet()) {
      reduceResults
          .put(aggregationFunction, aggregationFunction.reduce(aggregationResultsMap.get(aggregationFunction)));
    }
    return reduceResults;
  }

  public static List<AggregationResult> reduce(List<AggregationFunction> aggregationFunctionList,
      List<List<AggregationResult>> aggregationResultsList) {
    List<AggregationResult> reduceResults = new ArrayList<AggregationResult>();
    for (int i = 0; i < aggregationFunctionList.size(); ++i) {
      reduceResults.add(aggregationFunctionList.get(i).reduce(aggregationResultsList.get(i)));
    }
    return reduceResults;
  }
}
