/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.google.common.base.Preconditions;
import com.google.common.collect.MinMaxPriorityQueue;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GroupByAggregationService is initialized by aggregation functions and groupBys.
 *
 *
 */
public class AggregationGroupByOperatorService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationGroupByOperatorService.class);
  private static final String MIN_PREFIX = "min_";
  private final List<String> _groupByColumns;
  private final int _groupByTopN;
  private final int _trimThreshold;
  private final int _trimSize;
  private final List<AggregationFunction> _aggregationFunctionList;

  public AggregationGroupByOperatorService(List<AggregationInfo> aggregationInfos, GroupBy groupByQuery) {
    _aggregationFunctionList = AggregationFunctionFactory.getAggregationFunction(aggregationInfos);
    _groupByColumns = groupByQuery.getColumns();
    _groupByTopN = (int) groupByQuery.getTopN();

    // _minTrimSize is the larger of _groupByTopN or 1000.
    // _trimThreshold determines whether or not results should be trimmed and is 20 times of _minTrimSize.
    // If result size is < _trimThreshold, return the results without sorting or trimming.
    // Else, sort the results, and trim the result set size to 5 times of _minTrimSize.
    int minTrimSize = Math.max(_groupByTopN, 1000);

    // In case of int overflow, default to Integer.MAX_VALUE. These cannot be long because MinMaxPriorityQueue class
    // can only handle int size.
    boolean overFlow = (Integer.MAX_VALUE / 20) <= minTrimSize;
    _trimThreshold = overFlow ? Integer.MAX_VALUE : (minTrimSize * 20);

    overFlow = (Integer.MAX_VALUE / 5) <= minTrimSize;
    _trimSize = overFlow ? Integer.MAX_VALUE : (minTrimSize * 5);
  }

  public static List<Map<String, Serializable>> transformDataTableToGroupByResult(DataTable dataTable) {
    List<Map<String, Serializable>> aggregationGroupByResults = new ArrayList<Map<String, Serializable>>();
    for (int i = 0; i < dataTable.getNumberOfRows(); i++) {
      String key = dataTable.getString(i, 0);
      Map<String, Serializable> hashMap = (Map<String, Serializable>) dataTable.getObject(i, 1);
      aggregationGroupByResults.add(hashMap);
    }
    return aggregationGroupByResults;
  }

  public List<Map<String, Serializable>> reduceGroupByOperators(Map<ServerInstance, DataTable> instanceResponseMap) {
    if ((instanceResponseMap == null) || instanceResponseMap.isEmpty()) {
      return null;
    }
    List<Map<String, Serializable>> reducedResult = null;
    for (DataTable toBeReducedGroupByResults : instanceResponseMap.values()) {
      if (reducedResult == null) {
        if (toBeReducedGroupByResults != null) {
          reducedResult = transformDataTableToGroupByResult(toBeReducedGroupByResults);
        }
      } else {
        List<Map<String, Serializable>> toBeReducedResult =
            transformDataTableToGroupByResult(toBeReducedGroupByResults);
        for (int i = 0; i < reducedResult.size(); ++i) {
          for (String key : toBeReducedResult.get(i).keySet()) {
            if (reducedResult.get(i).containsKey(key)) {
              reducedResult.get(i).put(key, _aggregationFunctionList.get(i)
                  .combineTwoValues(reducedResult.get(i).get(key), toBeReducedResult.get(i).get(key)));
            } else {
              reducedResult.get(i).put(key, toBeReducedResult.get(i).get(key));
            }
          }
        }
      }
    }
    if (reducedResult != null) {
      for (int i = 0; i < reducedResult.size(); ++i) {
        Map<String, Serializable> functionLevelReducedResult = reducedResult.get(i);
        for (String key : functionLevelReducedResult.keySet()) {
          if (functionLevelReducedResult.get(key) != null) {
            functionLevelReducedResult
                .put(key, _aggregationFunctionList.get(i).reduce(Arrays.asList(functionLevelReducedResult.get(key))));
          }
        }
      }
    }
    return reducedResult;
  }

  /**
   * Translate the reducedGroupByResults (output of broker's reduce) to AggregationResult object
   * to be used to build the BrokerResponse.
   *
   * @param reducedGroupByResults
   * @return
   */
  public List<AggregationResult> renderAggregationGroupByResult(List<Map<String, Serializable>> reducedGroupByResults) {
    if (reducedGroupByResults == null || reducedGroupByResults.size() != _aggregationFunctionList.size()) {
      return null;
    }

    List<AggregationResult> aggregationResults = new ArrayList<AggregationResult>();
    for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
      int groupSize = _groupByColumns.size();

      Map<String, Serializable> reducedGroupByResult = reducedGroupByResults.get(i);
      AggregationFunction aggregationFunction = _aggregationFunctionList.get(i);

      String functionName = aggregationFunction.getFunctionName();
      List<GroupByResult> groupByResults = new ArrayList<GroupByResult>();

      if (!reducedGroupByResult.isEmpty()) {
        /* Reverse sort order for min functions. */
        boolean reverseOrder = aggregationFunction.getFunctionName().startsWith(MIN_PREFIX);

        // The MinMaxPriorityQueue will only add TOP N
        MinMaxPriorityQueue<ImmutablePair<Serializable, String>> minMaxPriorityQueue =
            getMinMaxPriorityQueue(reducedGroupByResult.values().iterator().next(), _groupByTopN, reverseOrder);

        if (minMaxPriorityQueue != null) {
          for (String groupedKey : reducedGroupByResult.keySet()) {
            minMaxPriorityQueue.add(new ImmutablePair(reducedGroupByResult.get(groupedKey), groupedKey));
          }

          ImmutablePair res;
          while ((res = (ImmutablePair) minMaxPriorityQueue.pollFirst()) != null) {
            String groupByColumnsString = (String) res.getRight();
            List<String> groupByColumns = Arrays.asList(groupByColumnsString
                .split(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString(), groupSize));

            Serializable value = (Serializable) res.getLeft();
            GroupByResult groupValue = new GroupByResult();

            groupValue.setGroup(groupByColumns);
            groupValue.setValue(formatValue(value));
            groupByResults.add(groupValue);
          }
        }
      }

      AggregationResult aggregationResult = new AggregationResult(groupByResults, _groupByColumns, functionName);
      aggregationResults.add(aggregationResult);
    }

    return aggregationResults;
  }

  /**
   * Format the {@link Serializable} value into {@link String}.
   * <p>For {@link Double} value, format it into the form ###################0.0#########.
   *
   * @param value value to be formatted.
   * @return formatted value.
   */
  @Nonnull
  public static String formatValue(@Nonnull Serializable value) {
    if (value instanceof Double) {
      return String.format(Locale.US, "%1.5f", (Double) value);
    } else {
      return value.toString();
    }
  }

  /**
   * Given a map from group by keys to results for multiple aggregation functions, trim the results to desired size and
   * put them into a list of group by results. This will make it compatible to the old group by code for the upper
   * layer.
   *
   * @param aggrGroupByResults Map from group by keys to result arrays.
   * @param numAggrFunctions Number of aggregation functions.
   * @return Trimmed list of maps containing group by results.
   */
  public List<Map<String, Serializable>> trimToSize(Map<String, Serializable[]> aggrGroupByResults,
      int numAggrFunctions) {
    Preconditions.checkNotNull(aggrGroupByResults);

    List<Map<String, Serializable>> trimmedResults = new ArrayList<>(numAggrFunctions);
    for (int i = 0; i < numAggrFunctions; i++) {
      trimmedResults.add(new HashMap<String, Serializable>());
    }

    if (aggrGroupByResults.size() > _trimThreshold) {
      trimToSize(_aggregationFunctionList, aggrGroupByResults, trimmedResults, numAggrFunctions, _trimSize);
    } else {
      convertGroupByResultsFromMapToList(aggrGroupByResults, trimmedResults, numAggrFunctions);
    }

    return trimmedResults;
  }

  /**
   * Given a map from group by keys to results for multiple aggregation functions, convert it to a list of group by
   * results, each of them according to one aggregation function.
   *
   * @param aggrGroupByResults Map from group by keys to result arrays.
   * @param aggrGroupByResultList List of maps containing group by results returned.
   * @param numAggrFunctions Number of aggregation functions.
   */
  private static void convertGroupByResultsFromMapToList(Map<String, Serializable[]> aggrGroupByResults,
      List<Map<String, Serializable>> aggrGroupByResultList, int numAggrFunctions) {
    for (String key : aggrGroupByResults.keySet()) {
      Serializable[] results = aggrGroupByResults.get(key);
      for (int i = 0; i < numAggrFunctions; i++) {
        aggrGroupByResultList.get(i).put(key, results[i]);
      }
    }
  }

  /**
   * Given a map from group by keys to results for multiple aggregation functions, trim the results to desired size and
   * put them into a list of group by results.
   *
   * @param aggrFuncList List of aggregation functions.
   * @param aggrGroupByResults Map from group by keys to result arrays.
   * @param trimmedGroupByResultList List of maps containing group by results returned.
   * @param numAggrFunctions Number of aggregation functions.
   * @param trimSize Desired trim size.
   */
  @SuppressWarnings("unchecked")
  private static void trimToSize(List<AggregationFunction> aggrFuncList, Map<String, Serializable[]> aggrGroupByResults,
      List<Map<String, Serializable>> trimmedGroupByResultList, int numAggrFunctions, int trimSize) {
    MinMaxPriorityQueue<ImmutablePair<Serializable, String>>[] heaps = new MinMaxPriorityQueue[numAggrFunctions];
    for (int i = 0; i < numAggrFunctions; i++) {
      boolean reverseOrder = aggrFuncList.get(i).getFunctionName().startsWith(MIN_PREFIX);
      heaps[i] = getMinMaxPriorityQueue(aggrGroupByResults.values().iterator().next()[i], trimSize, reverseOrder);
    }

    for (String key : aggrGroupByResults.keySet()) {
      Serializable[] results = aggrGroupByResults.get(key);
      for (int i = 0; i < numAggrFunctions; i++) {
        Serializable result = results[i];
        MinMaxPriorityQueue<ImmutablePair<Serializable, String>> heap = heaps[i];
        if (heap == null) {
          trimmedGroupByResultList.get(i).put(key, result);
        } else {
          heap.add(new ImmutablePair(result, key));
        }
      }
    }

    for (int i = 0; i < numAggrFunctions; i++) {
      MinMaxPriorityQueue<ImmutablePair<Serializable, String>> heap = heaps[i];
      ImmutablePair<Serializable, String> pair;
      if (heap != null) {
        while ((pair = heap.pollFirst()) != null) {
          trimmedGroupByResultList.get(i).put(pair.getRight(), pair.getLeft());
        }
      }
    }
  }

  /**
   * Given a group by result, return a group by result trimmed to provided size.
   * Sorting ordering is determined based on aggregation function.
   *
   * @param aggregationFunction
   * @param aggregationGroupByResult
   * @param trimSize
   * @return
   */
  private Map<String, Serializable> trimToSize(AggregationFunction aggregationFunction,
      Map<String, Serializable> aggregationGroupByResult, int trimSize) {

    boolean reverseOrder = aggregationFunction.getFunctionName().startsWith(MIN_PREFIX);
    MinMaxPriorityQueue<ImmutablePair<Serializable, String>> minMaxPriorityQueue =
        getMinMaxPriorityQueue(aggregationGroupByResult.values().iterator().next(), trimSize, reverseOrder);

    if (minMaxPriorityQueue == null) {
      return aggregationGroupByResult;
    }

    // The MinMaxPriorityQueue will add only the TOP N elements.
    for (String groupedKey : aggregationGroupByResult.keySet()) {
      minMaxPriorityQueue.add(new ImmutablePair(aggregationGroupByResult.get(groupedKey), groupedKey));
    }

    Map<String, Serializable> trimmedResult = new HashMap<>();
    ImmutablePair<Serializable, String> pair;
    while ((pair = (ImmutablePair) minMaxPriorityQueue.pollFirst()) != null) {
      trimmedResult.put(pair.getRight(), pair.getLeft());
    }
    return trimmedResult;
  }

  /**
   * Returns a MinMaxPriorityQueue with the given size limit, and ordering.
   * Will return null if the value to be inserted is not an instance of comparable.
   *
   * @param sampleObject To determine if the object is Comparable.
   * @param maxSize The max size for the heap.
   * @param reverseOrder True if sorting order to be reversed.
   * @return
   */
  private static MinMaxPriorityQueue<ImmutablePair<Serializable, String>> getMinMaxPriorityQueue(Serializable sampleObject,
      int maxSize, boolean reverseOrder) {
    if (!(sampleObject instanceof Comparable)) {
      return null;
    }

    Comparator<ImmutablePair<Serializable, String>> comparator =
        new GroupByResultComparator<ImmutablePair<Serializable, String>>().newComparator(reverseOrder);

    MinMaxPriorityQueue.Builder<ImmutablePair<Serializable, String>> minMaxPriorityQueueBuilder =
        MinMaxPriorityQueue.orderedBy(comparator).maximumSize(maxSize);

    return minMaxPriorityQueueBuilder.create();
  }

  /**
   * This class provides custom comparator to compare two groups of a groupByResult.
   * @param <T>
   */
  static class GroupByResultComparator<T extends Comparable & Serializable> {

    Comparator<T> newComparator(final boolean reverseOrder) {
      return new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
          int cmp = ((ImmutablePair<T, String>) o1).getLeft().compareTo(((ImmutablePair<T, String>) o2).getLeft());
          if (cmp < 0) {
            return ((reverseOrder) ? -1 : 1);
          } else if (cmp > 0) {
            return ((reverseOrder) ? 1 : -1);
          }
          return 0;
        }
      };
    }
  }
}
