package com.linkedin.thirdeye.dashboard.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.collections4.CollectionUtils;

import com.linkedin.thirdeye.dashboard.api.QueryResult;

public class QueryUtils {

  /**
   * Resolves all queries and returns a map with the returned data.
   * @param resultFutures
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public static Map<String, QueryResult> waitForQueries(
      Map<String, Future<QueryResult>> resultFutures)
          throws InterruptedException, ExecutionException {
    Map<String, QueryResult> results = new HashMap<>(resultFutures.size());
    for (Map.Entry<String, Future<QueryResult>> entry : resultFutures.entrySet()) {
      results.put(entry.getKey(), entry.getValue().get());
    }
    return results;
  }

  /**
   * Resolves all queries and returns a map with the returned data.
   * @param resultFutures Each key will have a list of futures
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public static Map<String, QueryResult> waitForAndMergeMultipleResults(
      Map<String, List<Future<QueryResult>>> resultFutures) throws InterruptedException,
      ExecutionException {

    Map<String, QueryResult> results = new HashMap<>(resultFutures.size());

    for (Map.Entry<String, List<Future<QueryResult>>> entry : resultFutures.entrySet()) {

      QueryResult finalQueryResult = null;
      for (Future<QueryResult> futureQueryResult : entry.getValue()) {

        QueryResult queryResult = futureQueryResult.get();
          if (finalQueryResult == null) {
            finalQueryResult = queryResult;
          } else {
            finalQueryResult = mergeQueryResults(finalQueryResult, queryResult);
          }
      }

      results.put(entry.getKey(), finalQueryResult);
    }

    return results;
  }

  public static QueryResult waitForAndMergeMultipleResults(List<Future<QueryResult>> resultFutures)
      throws InterruptedException, ExecutionException {

    QueryResult finalQueryResult = null;
    for (Future<QueryResult> futureQueryResult : resultFutures) {
      QueryResult queryResult = futureQueryResult.get();

      if (finalQueryResult == null) {
        finalQueryResult = queryResult;
      } else {
        finalQueryResult = mergeQueryResults(finalQueryResult, queryResult);
      }

    }

    return finalQueryResult;
  }

  /**
   * Joins the two QueryResult objects chronologically. This method assumes that QueryResults are
   * identical except for the time window of data that they return (eg the dimension combinations
   * returned are identical in both results)
   */
  public static QueryResult mergeQueryResults(QueryResult first, QueryResult second) {
    if (first == null || second == null) {
      throw new IllegalArgumentException("Query result cannot be null");
    }
    QueryResult merged = new QueryResult();
    merged.setDimensions(first.getDimensions());
    merged.setMetrics(first.getMetrics());

    Map<String, Map<String, Number[]>> firstDimensionData = first.getData();
    Map<String, Map<String, Number[]>> secondDimensionData = second.getData();
    Map<String, Map<String, Number[]>> mergedDimensionData =
        new HashMap<>(firstDimensionData.size());
    for (String dimensionKey : CollectionUtils.union(firstDimensionData.keySet(),
        secondDimensionData.keySet())) {
      Map<String, Number[]> firstData = firstDimensionData.get(dimensionKey);
      Map<String, Number[]> secondData = secondDimensionData.get(dimensionKey);
      Map<String, Number[]> mergedData = new HashMap<>((firstData == null ? 0 : firstData.size())
          + (secondData == null ? 0 : secondData.size()));
      if (firstData != null) {
        mergedData.putAll(firstData);
      }
      if (secondData != null) {
        mergedData.putAll(secondData);
      }

      mergedDimensionData.put(dimensionKey, mergedData);
    }
    merged.setData(mergedDimensionData);

    return merged;
  }

  /**
   * Joins the two QueryResult objects chronologically. This method assumes that QueryResults are
   * identical except for the time window of data that they return (eg the dimension combinations
   * returned are identical in both results)
   */
  public static Map<String, QueryResult> mergeQueryResultMaps(Map<String, QueryResult> first,
      Map<String, QueryResult> second) {
    if (first == null || second == null) {
      throw new IllegalArgumentException("Query result cannot be null");
    }
    Map<String, QueryResult> mergedResults = new HashMap<>(first.size());
    for (String dimensionKey : first.keySet()) {
      QueryResult firstResult = first.get(dimensionKey);
      QueryResult secondResult = second.get(dimensionKey);
      mergedResults.put(dimensionKey, mergeQueryResults(firstResult, secondResult));
    }
    return mergedResults;
  }

}
