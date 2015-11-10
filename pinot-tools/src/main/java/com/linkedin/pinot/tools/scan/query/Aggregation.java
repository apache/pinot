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


public class Aggregation {
  private IndexSegmentImpl _indexSegment;
  private SegmentMetadataImpl _metadata;
  private List<Integer> _filteredDocIds;
  private List<AggregationInfo> _aggregationsInfo;
  private List<String> _aggregationColumns;
  private Map<String, String> _columnAggregationFuncMap;

  public Aggregation(List<AggregationInfo> aggregationsInfo) {
    _aggregationsInfo = aggregationsInfo;
    _aggregationColumns = new ArrayList<>();
    _columnAggregationFuncMap = new HashMap<>();

    for (AggregationInfo aggregationInfo : _aggregationsInfo) {
      Map<String, String> aggregationParams = aggregationInfo.getAggregationParams();
      for (Map.Entry<String, String> entry : aggregationParams.entrySet()) {
        _columnAggregationFuncMap.put(entry.getValue(), aggregationInfo.getAggregationType());
      }
      _aggregationColumns.addAll(aggregationParams.values());
    }
  }

  public Aggregation(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, List<Integer> filteredDocIds,
      List<AggregationInfo> aggregationsInfo) {

    _indexSegment = indexSegment;
    _metadata = metadata;
    _filteredDocIds = filteredDocIds;
    _aggregationsInfo = aggregationsInfo;

    _aggregationColumns = new ArrayList<>();
    _columnAggregationFuncMap = new HashMap<>();

    for (AggregationInfo aggregationInfo : _aggregationsInfo) {
      Map<String, String> aggregationParams = aggregationInfo.getAggregationParams();
      for (Map.Entry<String, String> entry : aggregationParams.entrySet()) {
        _columnAggregationFuncMap.put(entry.getValue(), aggregationInfo.getAggregationType());
      }
      _aggregationColumns.addAll(aggregationParams.values());
    }
  }

  public ResultTable run() {
    Projection projection = new Projection(_indexSegment, _metadata, _filteredDocIds, _aggregationColumns);
    ResultTable projectionResult = projection.run();

    Map<String, Dictionary> dictionaryMap = new HashMap<>();
    for (String column : _aggregationColumns) {
      dictionaryMap.put(column, _indexSegment.getDictionaryFor(column));
    }

    ResultTable results = aggregate(projectionResult.values(dictionaryMap));
    return results;
  }

  public ResultTable aggregate(ResultTable projectionResult) {
    ResultTable results = new ResultTable(projectionResult.getColumnMetadatas(), 1);

    for (Map.Entry<String, String> entry : _columnAggregationFuncMap.entrySet()) {
      String column = entry.getKey();
      AggregationFunc aggregationFunc =
          AggregationFuncFactory.getAggregationFunc(projectionResult, column, entry.getValue());
      ResultTable aggregationResult = aggregationFunc.run();

      results.add(0, aggregationResult.get(0, 0));
    }
    return results;
  }
}
