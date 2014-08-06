package com.linkedin.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.response.AggregationResult;


/**
 * CombineReduceService will take a list of intermediate results and merge them.
 *
 */
public class CombineService {

  private static Logger LOGGER = LoggerFactory.getLogger(CombineService.class);

  public static List<AggregationResult> combine(List<AggregationFunction> aggregationFunctionList,
      List<List<AggregationResult>> aggregationResultsList, CombineLevel combineLevel) {
    List<AggregationResult> combinedResultsList = new ArrayList<AggregationResult>();
    if (aggregationResultsList == null) {
      return null;
    }
    for (int i = 0; i < aggregationFunctionList.size(); ++i) {
      if (aggregationResultsList.get(i) != null) {
        AggregationFunction aggregationFunction = aggregationFunctionList.get(i);
        AggregationResult combinedResults = aggregationFunction.combine(aggregationResultsList.get(i), combineLevel);
        combinedResultsList.add(combinedResults);
      } else {
        combinedResultsList.add(null);
      }
    }
    return combinedResultsList;
  }

}
