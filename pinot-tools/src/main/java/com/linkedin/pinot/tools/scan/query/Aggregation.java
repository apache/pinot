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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Aggregation {
  private boolean _addCountStar;
  private IndexSegmentImpl _indexSegment;
  private SegmentMetadataImpl _metadata;
  private List<Integer> _filteredDocIds;
  private List<AggregationInfo> _aggregationsInfo;
  private List<String> _groupByColumns;
  private List<Pair> _columnFunctionList;
  private Map<String, Dictionary> _dictionaryMap;
  private List<String> _projectionColumns;
  private List<Pair> _allColumns;

  private void init(List<String> groupByColumns) {
    _groupByColumns = groupByColumns;
    _columnFunctionList = new ArrayList<>();

    for (AggregationInfo aggregationInfo : _aggregationsInfo) {
      Map<String, String> aggregationParams = aggregationInfo.getAggregationParams();
      for (Map.Entry<String, String> entry : aggregationParams.entrySet()) {
        _columnFunctionList.add(new Pair(entry.getValue(), aggregationInfo.getAggregationType()));
      }
    }

    _addCountStar = false;
    _projectionColumns = new ArrayList<>();
    _allColumns = new ArrayList<>();

    if (_groupByColumns != null) {
      for (String column : _groupByColumns) {
        _projectionColumns.add(column);
        _allColumns.add(new Pair(column, null));
      }
    }

    for (Pair pair : _columnFunctionList) {
      String column = (String) pair.getFirst();
      if (column.equals("*")) {
        _addCountStar = true;
      } else {
        _projectionColumns.add(column);
        _allColumns.add(new Pair(column, pair.getSecond()));
      }
    }

    // This is always the last columns.
    if (_addCountStar) {
      _allColumns.add(new Pair("count", "star"));
    }
  }

  public Aggregation(List<AggregationInfo> aggregationsInfo, List<String> groupByColumns) {
    _aggregationsInfo = aggregationsInfo;
    init(groupByColumns);
  }

  public Aggregation(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<AggregationInfo> aggregationsInfo, List<String> groupByColumns) {
    _indexSegment = indexSegment;
    _metadata = metadata;
    _dictionaryMap = new HashMap<>();

    _filteredDocIds = filteredDocIds;
    _aggregationsInfo = aggregationsInfo;
    init(groupByColumns);

    // Aggregation columns have been renamed, group by columns have not.
    for (String column : _projectionColumns) {
      _dictionaryMap.put(column, _indexSegment.getDictionaryFor(column));
    }
  }

  public ResultTable run() {
    Projection projection = new Projection(_indexSegment, _metadata, _filteredDocIds, _projectionColumns,
        _dictionaryMap, _addCountStar);
    return aggregate(projection.run());
  }

  public ResultTable aggregate(ResultTable input) {
    if (_groupByColumns == null) {
      return aggregateOne(input);
    }

    Map<GroupByOperator, ResultTable> resultsMap = new HashMap<>();
    for (ResultTable.Row row : input) {
      List<Object> groupByValues = new ArrayList<>();
      for (String groupByColumn : _groupByColumns) {
        groupByValues.add(row.get(groupByColumn));
      }

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

    ResultTable results = new ResultTable(_allColumns, 0);

    for (Map.Entry<GroupByOperator, ResultTable> entry : resultsMap.entrySet()) {
      GroupByOperator groupByOperator = entry.getKey();
      ResultTable groupByTable = entry.getValue();
      ResultTable aggregationResult = new ResultTable(_allColumns, 1);

      for (Object groupByColumn : groupByOperator._getGroupBys()) {
        aggregationResult.add(0, groupByColumn);
      }
      aggregationResult.add(0, aggregateOne(groupByTable).get(0, 0));

      results.append(aggregationResult);
    }
    return results;
  }

  private ResultTable aggregateOne(ResultTable input) {
    ResultTable results = new ResultTable(_allColumns, 1);

    for (Pair pair : _columnFunctionList) {
      String column = (String) pair.getFirst();
      String function = (String) pair.getSecond();

      AggregationFunc aggregationFunc =
          AggregationFuncFactory.getAggregationFunc(input, column, function);
      ResultTable aggregationResult = aggregationFunc.run();

      results.add(0, aggregationResult.get(0, 0));
    }
    return results;
  }
}
