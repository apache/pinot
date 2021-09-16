/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.MinAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.request.context.ExpressionContext;


/**
 * The <code>AggregationGroupByTrimmingService</code> class provides trimming service for aggregation group-by queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationGroupByTrimmingService {
  private final AggregationFunction[] _aggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _limit;
  private final int _trimSize;
  private final int _trimThreshold;

  public AggregationGroupByTrimmingService(QueryContext queryContext) {
    _aggregationFunctions = queryContext.getAggregationFunctions();
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numGroupByExpressions = groupByExpressions.size();
    _limit = queryContext.getLimit();
    assert _limit > 0;

    // To keep the precision, _trimSize is max(_limit * 5, 5000)
    _trimSize = GroupByUtils.getTableCapacity(_limit);

    // To trigger the trimming, number of groups should be larger than _trimThreshold which is (_trimSize * 4)
    _trimThreshold = _trimSize * 4;
  }

  /**
   * Given a map from group key to the intermediate results for multiple aggregation functions, trim the results to
   * desired size and put them into a list of maps from group key to intermediate result for each aggregation function.
   */
  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> trimIntermediateResultsMap(Map<String, Object[]> intermediateResultsMap) {
    int numAggregationFunctions = _aggregationFunctions.length;
    Map<String, Object>[] trimmedResultMaps = new Map[numAggregationFunctions];

    int numGroups = intermediateResultsMap.size();
    if (numGroups > _trimThreshold) {
      // Trim the result only if number of groups is larger than the threshold

      Sorter[] sorters = new Sorter[numAggregationFunctions];
      for (int i = 0; i < numAggregationFunctions; i++) {
        AggregationFunction aggregationFunction = _aggregationFunctions[i];
        sorters[i] = getSorter(_trimSize, aggregationFunction, aggregationFunction.isIntermediateResultComparable());
      }

      // Add results into sorters
      for (Map.Entry<String, Object[]> entry : intermediateResultsMap.entrySet()) {
        String groupKey = entry.getKey();
        Object[] intermediateResults = entry.getValue();
        for (int i = 0; i < numAggregationFunctions; i++) {
          sorters[i].add(groupKey, intermediateResults[i]);
        }
      }

      // Dump trimmed results into maps
      for (int i = 0; i < numAggregationFunctions; i++) {
        Map<String, Object> trimmedResultMap = new HashMap<>(_trimSize);
        sorters[i].dumpToMap(trimmedResultMap);
        trimmedResultMaps[i] = trimmedResultMap;
      }
    } else {
      // Simply put results from intermediateResultsMap into trimmedResults

      for (int i = 0; i < numAggregationFunctions; i++) {
        trimmedResultMaps[i] = new HashMap<>(numGroups);
      }
      for (Map.Entry<String, Object[]> entry : intermediateResultsMap.entrySet()) {
        String groupKey = entry.getKey();
        Object[] intermediateResults = entry.getValue();
        for (int i = 0; i < numAggregationFunctions; i++) {
          trimmedResultMaps[i].put(groupKey, intermediateResults[i]);
        }
      }
    }

    return Arrays.asList(trimmedResultMaps);
  }

  /**
   * Given an array of maps from group key to final result for each aggregation function, trim the results to topN size.
   */
  @SuppressWarnings("unchecked")
  public List<GroupByResult>[] trimFinalResults(Map<String, Comparable>[] finalResultMaps) {
    int numAggregationFunctions = _aggregationFunctions.length;
    List<GroupByResult>[] trimmedResults = new List[numAggregationFunctions];

    for (int i = 0; i < numAggregationFunctions; i++) {
      LinkedList<GroupByResult> groupByResults = new LinkedList<>();
      trimmedResults[i] = groupByResults;

      Map<String, Comparable> finalResultMap = finalResultMaps[i];
      if (finalResultMap.isEmpty()) {
        continue;
      }

      // Final result is always comparable
      Sorter sorter = getSorter(_limit, _aggregationFunctions[i], true);

      // Add results into sorter
      for (Map.Entry<String, Comparable> entry : finalResultMap.entrySet()) {
        sorter.add(entry.getKey(), entry.getValue());
      }

      // Dump trimmed results into list
      sorter.dumpToGroupByResults(groupByResults, _numGroupByExpressions);
    }

    return trimmedResults;
  }

  private interface Sorter {
    void add(String groupKey, Object result);

    void dumpToMap(Map<String, Object> dest);

    void dumpToGroupByResults(LinkedList<GroupByResult> dest, int numGroupByExpressions);
  }

  private static Sorter getSorter(int trimSize, AggregationFunction aggregationFunction, boolean isComparable) {
    // This will cover both MIN and MINMV
    boolean minOrder = aggregationFunction instanceof MinAggregationFunction;

    if (isComparable) {
      if (minOrder) {
        return new ComparableSorter(trimSize, Collections.reverseOrder());
      } else {
        return new ComparableSorter(trimSize, new ComparableComparator());
      }
    } else {
      // Reverse the comparator so that keys are ordered in descending order
      if (minOrder) {
        return new NonComparableSorter(trimSize, new ComparableComparator(), aggregationFunction);
      } else {
        return new NonComparableSorter(trimSize, Collections.reverseOrder(), aggregationFunction);
      }
    }
  }

  /**
   * Helper class based on {@link PriorityQueue} to sort on comparable values:
   * <ul>
   *   <li>
   *     If the heap size is less than the trim size, simply add the groupKey-result pair into the heap
   *   </li>
   *   <li>
   *     If the heap size is equal to the trim size, compare the given groupKey-result pair against the min
   *     groupKey-result pair from the heap. If the given groupKey-result pair is bigger, remove the min groupKey-result
   *     pair and insert the new one to keep the heap size bounded
   *   </li>
   * </ul>
   */
  private static class ComparableSorter implements Sorter {
    private final int _trimSize;
    private final Comparator<? super Comparable> _comparator;
    private final PriorityQueue<GroupKeyResultPair> _heap;

    public ComparableSorter(int trimSize, Comparator<? super Comparable> comparator) {
      _trimSize = trimSize;
      _comparator = comparator;
      _heap = new PriorityQueue<>(_trimSize, comparator);
    }

    @Override
    public void add(String groupKey, Object result) {
      GroupKeyResultPair newGroupKeyResultPair = new GroupKeyResultPair(groupKey, (Comparable) result);
      if (_heap.size() == _trimSize) {
        GroupKeyResultPair minGroupKeyResultPair = _heap.peek();
        if (_comparator.compare(newGroupKeyResultPair, minGroupKeyResultPair) > 0) {
          _heap.poll();
          _heap.add(newGroupKeyResultPair);
        }
      } else {
        _heap.add(newGroupKeyResultPair);
      }
    }

    @Override
    public void dumpToMap(Map<String, Object> dest) {
      GroupKeyResultPair groupKeyResultPair;
      while ((groupKeyResultPair = _heap.poll()) != null) {
        dest.put(groupKeyResultPair._groupKey, groupKeyResultPair._result);
      }
    }

    @Override
    public void dumpToGroupByResults(LinkedList<GroupByResult> dest, int numGroupByExpressions) {
      GroupKeyResultPair groupKeyResultPair;
      if (numGroupByExpressions == 1) {
        while ((groupKeyResultPair = _heap.poll()) != null) {
          GroupByResult groupByResult = new GroupByResult();
          groupByResult.setGroup(Collections.singletonList(groupKeyResultPair._groupKey));
          groupByResult.setValue(AggregationFunctionUtils.getSerializableValue(groupKeyResultPair._result));

          // Add to head to reverse the order
          dest.addFirst(groupByResult);
        }
      } else {
        while ((groupKeyResultPair = _heap.poll()) != null) {
          // Preserve all tokens to support empty strings
          String[] groupKeys =
              StringUtils.splitPreserveAllTokens(groupKeyResultPair._groupKey, GroupKeyGenerator.DELIMITER);

          GroupByResult groupByResult = new GroupByResult();
          groupByResult.setGroup(Arrays.asList(groupKeys));
          groupByResult.setValue(AggregationFunctionUtils.getSerializableValue(groupKeyResultPair._result));

          // Add to head to reverse the order
          dest.addFirst(groupByResult);
        }
      }
    }

    private static class GroupKeyResultPair implements Comparable<GroupKeyResultPair> {
      private final String _groupKey;
      private final Comparable<? super Comparable> _result;

      public GroupKeyResultPair(String groupKey, Comparable<? super Comparable> result) {
        _groupKey = groupKey;
        _result = result;
      }

      @Override
      public int compareTo(GroupKeyResultPair o) {
        return _result.compareTo(o._result);
      }
    }
  }

  /**
   * Helper class based on {@link TreeMap} to sort on non-comparable values:
   * <ul>
   *   <li>
   *     The key of the map is the final result derived from the intermediate result passed in
   *   </li>
   *   <li>
   *     The value of the map is a list of groupKey-result pairs that inserted with the same key
   *   </li>
   *   <li>
   *     If the number of values added is less than the trim size, simply add the groupKey-result pair into the map
   *   </li>
   *   <li>
   *     If the number of values added is greater or equal to the trim size, compare the given key against the max key
   *     from the map. If the given key is smaller, insert the new groupKey-result pair into the map
   *   </li>
   *   <li>
   *     When possible, remove the max key from the map when enough values inserted
   *   </li>
   * </ul>
   */
  private static class NonComparableSorter implements Sorter {
    private final int _trimSize;
    private final Comparator<? super Comparable> _comparator;
    private final AggregationFunction _aggregationFunction;
    private final TreeMap<Comparable, List<ImmutablePair<String, Object>>> _treeMap;
    private int _numValuesAdded = 0;

    public NonComparableSorter(int trimSize, Comparator<? super Comparable> comparator,
        AggregationFunction aggregationFunction) {
      _trimSize = trimSize;
      _comparator = comparator;
      _aggregationFunction = aggregationFunction;
      _treeMap = new TreeMap<>(comparator);
    }

    @Override
    public void add(String groupKey, Object result) {
      Comparable newKey = _aggregationFunction.extractFinalResult(result);
      ImmutablePair<String, Object> groupKeyResultPair = new ImmutablePair<>(groupKey, result);

      List<ImmutablePair<String, Object>> groupKeyResultPairs = _treeMap.get(newKey);
      if (_numValuesAdded >= _trimSize) {
        // Check whether the pair should be added
        Map.Entry<Comparable, List<ImmutablePair<String, Object>>> maxEntry = _treeMap.lastEntry();
        Comparable maxKey = maxEntry.getKey();
        if (_comparator.compare(newKey, maxKey) < 0) {
          // Add the pair into list of pairs
          if (groupKeyResultPairs == null) {
            groupKeyResultPairs = new ArrayList<>();
            _treeMap.put(newKey, groupKeyResultPairs);
          }
          groupKeyResultPairs.add(groupKeyResultPair);
          _numValuesAdded++;

          // Check if the max key can be removed
          if (maxEntry.getValue().size() + _trimSize == _numValuesAdded) {
            _treeMap.remove(maxKey);
          }
        }
      } else {
        // Pair should be added
        if (groupKeyResultPairs == null) {
          groupKeyResultPairs = new ArrayList<>();
          _treeMap.put(newKey, groupKeyResultPairs);
        }
        groupKeyResultPairs.add(groupKeyResultPair);
        _numValuesAdded++;
      }
    }

    @Override
    public void dumpToMap(Map<String, Object> dest) {
      // Track the number of results added because there could be more than trim size values inside the map
      int numResultsAdded = 0;
      for (List<ImmutablePair<String, Object>> groupKeyResultPairs : _treeMap.values()) {
        for (ImmutablePair<String, Object> groupResultPair : groupKeyResultPairs) {
          if (numResultsAdded != _trimSize) {
            dest.put(groupResultPair.left, groupResultPair.right);
            numResultsAdded++;
          } else {
            return;
          }
        }
      }
    }

    @Override
    public void dumpToGroupByResults(LinkedList<GroupByResult> dest, int numGroupByExpressions) {
      throw new UnsupportedOperationException();
    }
  }
}
