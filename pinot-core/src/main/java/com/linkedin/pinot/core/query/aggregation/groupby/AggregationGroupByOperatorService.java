/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

  public List<AggregationFunction> getAggregationFunctionList() {
    return _aggregationFunctionList;
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

  public List<JSONObject> renderGroupByOperators(List<Map<String, Serializable>> finalAggregationResult) {
    try {
      if (finalAggregationResult == null || finalAggregationResult.size() != _aggregationFunctionList.size()) {
        return null;
      }
      List<JSONObject> retJsonResultList = new ArrayList<JSONObject>();
      for (int i = 0; i < _aggregationFunctionList.size(); ++i) {
        JSONArray groupByResultsArray = new JSONArray();

        int groupSize = _groupByColumns.size();
        Map<String, Serializable> reducedGroupByResult = finalAggregationResult.get(i);
        AggregationFunction aggregationFunction = _aggregationFunctionList.get(i);

        if (!reducedGroupByResult.isEmpty()) {
          boolean reverseOrder = aggregationFunction.getFunctionName().startsWith(MIN_PREFIX);

          MinMaxPriorityQueue<ImmutablePair<Serializable, String>> minMaxPriorityQueue =
              getMinMaxPriorityQueue(reducedGroupByResult.values().iterator().next(), _groupByTopN, reverseOrder);

          if (minMaxPriorityQueue != null) {
            // The MinMaxPriorityQueue will only add TOP N
            for (String groupedKey : reducedGroupByResult.keySet()) {
              minMaxPriorityQueue.add(new ImmutablePair(reducedGroupByResult.get(groupedKey), groupedKey));
            }

            ImmutablePair res;
            while ((res = (ImmutablePair) minMaxPriorityQueue.pollFirst()) != null) {
              JSONObject groupByResultObject = new JSONObject();
              groupByResultObject.put("group", new JSONArray(((String) res.getRight())
                  .split(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString(), groupSize)));
              //          if (res.getFirst() instanceof Number) {
              //            groupByResultObject.put("value", df.format(res.getFirst()));
              //          } else {
              //            groupByResultObject.put("value", res.getFirst());
              //          }
              //          groupByResultsArray.put(realGroupSize - 1 - j, groupByResultObject);
              groupByResultObject.put("value", aggregationFunction.render((Serializable) res.getLeft()).get("value"));
              groupByResultsArray.put(groupByResultObject);
            }
          }
        }

        JSONObject result = new JSONObject();
        result.put("function", aggregationFunction.getFunctionName());
        result.put("groupByResult", groupByResultsArray);
        result.put("groupByColumns", new JSONArray(_groupByColumns));
        retJsonResultList.add(result);
      }
      return retJsonResultList;
    } catch (JSONException e) {
      LOGGER.error("Caught exception while processing group by aggregation", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
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

  private Serializable formatValue(Serializable value) {
    return (value instanceof Float || value instanceof Double) ? String.format(Locale.US, "%1.5f", value)
        : value.toString();
  }

  /**
   * Given a list of group by results, trim each one of them to desired size.
   * Desired size is computed to be five times that of the TOP N in the query.
   *
   * @param aggregationGroupByResultList List of trimmed group by results.
   * @return
   */
  public List<Map<String, Serializable>> trimToSize(List<Map<String, Serializable>> aggregationGroupByResultList) {
    if (aggregationGroupByResultList == null || aggregationGroupByResultList.isEmpty()) {
      return aggregationGroupByResultList;
    }

    List<Map<String, Serializable>> trimmedResults = new ArrayList<>();
    for (int i = 0; i < aggregationGroupByResultList.size(); ++i) {
      if (aggregationGroupByResultList.get(i).size() > _trimThreshold) {
        trimmedResults.add(trimToSize(_aggregationFunctionList.get(i), aggregationGroupByResultList.get(i), _trimSize));
      } else {
        trimmedResults.add(aggregationGroupByResultList.get(i));
      }
    }

    return trimmedResults;
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
  MinMaxPriorityQueue<ImmutablePair<Serializable, String>> getMinMaxPriorityQueue(Serializable sampleObject,
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
  class GroupByResultComparator<T extends Comparable & Serializable> {

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
