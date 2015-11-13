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
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class AggregationGroupBy {
  private boolean _addCountStar;
  private IndexSegmentImpl _indexSegment;
  private SegmentMetadataImpl _metadata;
  private List<Integer> _filteredDocIds;
  private List<AggregationInfo> _aggregationsInfo;
  private List<String> _groupByColumns;
  private Map<String, String> _columnAggregationFuncMap;
  private HashMap<String, Dictionary> _dictionaryMap;
  private List<String> _projectionColumns;

  private void init() {
    _columnAggregationFuncMap = new HashMap<>();
    for (AggregationInfo aggregationInfo : _aggregationsInfo) {
      Map<String, String> aggregationParams = aggregationInfo.getAggregationParams();
      for (Map.Entry<String, String> entry : aggregationParams.entrySet()) {
        _columnAggregationFuncMap.put(entry.getValue(), aggregationInfo.getAggregationType());
      }
    }
  }

  public AggregationGroupBy(List<AggregationInfo> aggregationsInfo) {
    _aggregationsInfo = aggregationsInfo;
    init();
  }

  public AggregationGroupBy(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<AggregationInfo> aggregationsInfo, List<String> groupByColumns) {
    _indexSegment = indexSegment;
    _metadata = metadata;
    _dictionaryMap = new HashMap<>();
    _groupByColumns = groupByColumns;

    _filteredDocIds = filteredDocIds;
    _aggregationsInfo = aggregationsInfo;
    init();

    _addCountStar = false;
    _projectionColumns = new ArrayList<>();
    for (String column : _columnAggregationFuncMap.keySet()) {
      if (!column.equals("*")) {
        _projectionColumns.add(column);
      } else {
        _addCountStar = true;
      }
    }
    _projectionColumns.addAll(_groupByColumns);

    for (String column : _projectionColumns) {
      if (!column.equals("*")) {
        _dictionaryMap.put(column, _indexSegment.getDictionaryFor(column));
      }
    }
  }

  public ResultTable run() {
    Projection projection = new Projection(_indexSegment, _metadata, _filteredDocIds, _projectionColumns);
    ResultTable projectionResult = projection.run();

    Map<GroupByOperator, ResultTable>
        projectionGroupByResult = groupBy(projectionResult.values(_dictionaryMap, _addCountStar));
    ResultTable results = new ResultTable(_projectionColumns, 0);

    for (Map.Entry<GroupByOperator, ResultTable> entry : projectionGroupByResult.entrySet()) {
      GroupByOperator groupByOperator = entry.getKey();
      ResultTable groupByTable = entry.getValue();
      ResultTable aggregationResult = aggregate(groupByTable);

      for (Object groupByColumn : groupByOperator._getGroupBys()) {
        aggregationResult.add(0, groupByColumn);
      }
      results.append(aggregationResult);
    }
    return results;
  }

  private Map<GroupByOperator, ResultTable> groupBy(ResultTable input) {
    Map<GroupByOperator, ResultTable> resultsMap = new HashMap<>();
    List<String> aggregationColumns = new ArrayList<>(_columnAggregationFuncMap.keySet());

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
        resultTable = new ResultTable(aggregationColumns, 0);
        resultsMap.put(groupByOperator, resultTable);
      }

      resultTable.append(row);
    }

    return resultsMap;
  }

  public ResultTable aggregate(ResultTable groupByTable) {
    ResultTable results = new ResultTable(_projectionColumns, 1);

    for (Map.Entry<String, String> entry : _columnAggregationFuncMap.entrySet()) {
      String column = entry.getKey();
      AggregationFunc aggregationFunc =
          AggregationFuncFactory.getAggregationFunc(groupByTable, column, entry.getValue());
      ResultTable aggregationResult = aggregationFunc.run();

      results.add(0, aggregationResult.get(0, 0));
    }
    return results;
  }
}
