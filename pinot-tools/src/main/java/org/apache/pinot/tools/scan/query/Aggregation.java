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
package org.apache.pinot.tools.scan.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.utils.Pair;


public class Aggregation {
  private boolean _addCountStar;
  private ImmutableSegment _immutableSegment;
  private SegmentMetadataImpl _metadata;
  private List<Integer> _filteredDocIds;
  private List<AggregationInfo> _aggregationsInfo;
  private List<String> _groupByColumns;
  private List<Pair> _columnFunctionList;
  private Map<String, Dictionary> _dictionaryMap;
  private List<Pair> _projectionColumns;
  private List<Pair> _allColumns;
  private long _topN = 10;

  private void init(List<String> groupByColumns) {
    _groupByColumns = groupByColumns;
    _columnFunctionList = new ArrayList<>();
    _addCountStar = false;

    for (AggregationInfo aggregationInfo : _aggregationsInfo) {
      for (String column : aggregationInfo.getExpressions()) {
        // Apparently in case of multiple group by's '*' is replaced by empty/null in brokerRequest.
        if (column == null || column.isEmpty() || column.equals("*")) {
          _addCountStar = true;
          continue;
        }
        _columnFunctionList.add(new Pair(column, aggregationInfo.getAggregationType().toLowerCase()));
      }
    }

    // Count star appended at the end in the result table.
    if (_addCountStar) {
      _columnFunctionList.add(new Pair("*", "count"));
    }

    _projectionColumns = new ArrayList<>();
    _allColumns = new ArrayList<>();

    if (_groupByColumns != null) {
      for (String column : _groupByColumns) {
        _projectionColumns.add(new Pair(column, null));
        _allColumns.add(new Pair(column, null));
      }
    }

    for (Pair pair : _columnFunctionList) {
      String column = (String) pair.getFirst();
      if (!column.equals("*")) {
        _projectionColumns.add(pair);
        _allColumns.add(pair);
      }
    }

    // This is always the last columns.
    if (_addCountStar) {
      _allColumns.add(new Pair("*", "count"));
    }
  }

  public Aggregation(List<AggregationInfo> aggregationsInfo, List<String> groupByColumns, long topN) {
    _aggregationsInfo = aggregationsInfo;
    _topN = topN;
    init(groupByColumns);
  }

  public Aggregation(ImmutableSegment immutableSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<AggregationInfo> aggregationsInfo, List<String> groupByColumns, long topN) {
    _immutableSegment = immutableSegment;
    _metadata = metadata;
    _dictionaryMap = new HashMap<>();

    _filteredDocIds = filteredDocIds;
    _aggregationsInfo = aggregationsInfo;
    _topN = topN;
    init(groupByColumns);

    for (Pair pair : _projectionColumns) {
      String column = (String) pair.getFirst();
      _dictionaryMap.put(column, _immutableSegment.getDictionary(column));
    }
  }

  public ResultTable run() {
    Projection projection =
        new Projection(_immutableSegment, _metadata, _filteredDocIds, _projectionColumns, _dictionaryMap,
            _addCountStar);
    return aggregate(projection.run());
  }

  public ResultTable aggregate(ResultTable input) {
    if (_groupByColumns == null) {
      return aggregateOne(input);
    }

    Map<GroupByOperator, ResultTable> resultsMap = new HashMap<>();
    for (ResultTable.Row row : input) {
      // For MV Columns we enumerate each value as a separate group, instead of all values of the column in one group.
      for (List<Object> groupByValues : enumerateGroups(row)) {
        GroupByOperator groupByOperator = new GroupByOperator(groupByValues);
        ResultTable resultTable;

        if (resultsMap.containsKey(groupByOperator)) {
          resultTable = resultsMap.get(groupByOperator);
        } else {
          resultTable = new ResultTable(_allColumns, 0);
          resultsMap.put(groupByOperator, resultTable);
        }

        resultTable.append(row);
      }
    }

    ResultTable results = new ResultTable(_allColumns, 0);

    for (Map.Entry<GroupByOperator, ResultTable> entry : resultsMap.entrySet()) {
      GroupByOperator groupByOperator = entry.getKey();
      ResultTable groupByTable = entry.getValue();
      ResultTable aggregationResult = new ResultTable(_allColumns, 1);

      for (Object groupByColumn : groupByOperator._getGroupBys()) {
        aggregationResult.add(0, groupByColumn);
      }

      ResultTable.Row row = aggregateOne(groupByTable).getRow(0);
      for (Object value : row) {
        aggregationResult.add(0, value);
      }
      results.append(aggregationResult);
    }

    results.setResultType(ResultTable.ResultType.AggregationGroupBy);
    return results;
  }

  private List<List<Object>> enumerateGroups(ResultTable.Row row) {
    List<List<Object>> groups = new ArrayList<>();

    for (String groupByColumn : _groupByColumns) {
      Object value = row.get(groupByColumn, null);
      if (value instanceof Object[]) {
        groups.add(Arrays.asList((Object[]) value));
      } else {
        groups.add(Arrays.asList(value));
      }
    }
    return Utils.cartesianProduct(groups);
  }

  private ResultTable aggregateOne(ResultTable input) {
    ResultTable results = new ResultTable(_allColumns, 1);
    results.setResultType(ResultTable.ResultType.Aggregation);

    if (input.isEmpty()) {
      return new ResultTable(_allColumns, 0);
    }

    for (Pair pair : _columnFunctionList) {
      String column = (String) pair.getFirst();
      String function = (String) pair.getSecond();

      AggregationFunc aggregationFunc = AggregationFuncFactory.getAggregationFunc(input, column, function);
      ResultTable aggregationResult = aggregationFunc.run();
      results.add(0, aggregationResult.get(0, 0));
    }

    return results;
  }
}
