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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.core.query.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;


public class QueryResponse {
  private int _numDocsScanned = 0;
  private int _totalDocs = 0;
  private long _timeUsedMs = 0;

  SelectionResults _selectionResults;
  List<AggregationResult> _aggregationResults;

  QueryResponse(ResultTable resultTable) {
    if (resultTable == null) {
      return;
    }

    _numDocsScanned = resultTable.getNumDocsScanned();
    _totalDocs = resultTable.getTotalDocs();
    _timeUsedMs = resultTable.getProcessingTime();

    switch (resultTable.getResultType()) {
      case Selection:
        buildSelectionResult(resultTable);
        break;

      case Aggregation:
        buildAggregationResult(resultTable);
        break;

      case AggregationGroupBy:
        buildAggregationGroupByResult(resultTable);
        break;

      default:
        // Nothing to do here.
    }
  }

  private void buildSelectionResult(ResultTable resultTable) {
    List<List<String>> results = new ArrayList<>();
    List<String> columns = new ArrayList<>();

    for (Pair<String, String> pair : resultTable.getColumnList()) {
      columns.add(pair.getFirst());
    }


    for (ResultTable.Row row : resultTable) {
      List<String> columnValues = new ArrayList<>();
      for (Object value : row) {
        columnValues.add(value.toString());
      }
      results.add(columnValues);
    }

    _selectionResults = new SelectionResults(columns, results);
  }

  private void buildAggregationResult(ResultTable resultTable) {
    _aggregationResults = new ArrayList<>();
    for (ResultTable.Row row : resultTable) {
      int columnId = 0;
      for (Object value : row) {
        AggregationResult aggregationResult =
            new AggregationResult(resultTable.getFunction(columnId), value.toString());
        _aggregationResults.add(aggregationResult);
        ++columnId;
      }
    }
  }


  private void buildAggregationGroupByResult(ResultTable resultTable) {
    List<Pair> columnList = resultTable.getColumnList();
    List<String> groupByColumns = new ArrayList<>();
    _aggregationResults = new ArrayList<>();

    for (Pair pair : columnList) {
      if (pair.getSecond() == null) {
        groupByColumns.add(pair.getFirst().toString());
      }
    }

    int numGroupByColumns = groupByColumns.size();
    List<List<String>> groupValueStrings = new ArrayList<>();

    for (ResultTable.Row row : resultTable) {
      int colId = 0;
      List<String> group = new ArrayList<>();

      for (Object value : row) {
        // All group by columns come first.
        if (columnList.get(colId).getSecond() != null) {
          break;
        }
        if (value instanceof  Object []) {
          Object[] array = (Object[]) value;
          for (Object obj : array) {
            group.add(obj.toString());
          }
        } else {
          group.add(value.toString());
        }
        ++colId;
      }
      groupValueStrings.add(group);
    }

    for (int colId = numGroupByColumns; colId < columnList.size(); ++colId) {
      String function = resultTable.getFunction(colId);
      if (function.equalsIgnoreCase("count_*")) {
        function = "count_star";
      }

      int rowId = 0;
      List<GroupValue> groupValues = new ArrayList<>();
      for (ResultTable.Row row : resultTable) {
        String value = row.get(colId).toString();
        GroupValue groupValue = new GroupValue(value, groupValueStrings.get(rowId));
        groupValues.add(groupValue);
        ++rowId;
      }

      AggregationResult aggregationResult = new AggregationResult(groupValues, groupByColumns, function);
      _aggregationResults.add(aggregationResult);
    }
  }

  public int getNumDocsScanned() {
    return _numDocsScanned;
  }

  public int getTotalDocs() {
    return _totalDocs;
  }

  public long getTimeUsedMs() {
    return _timeUsedMs;
  }

  @JsonProperty("selectionResults")
  @JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
  SelectionResults getSelectionResults() {
    return _selectionResults;
  }

  @JsonProperty("aggregationResults")
  @JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
  List<AggregationResult> getAggregationResults() {
    return _aggregationResults;
  }

  class SelectionResults {
    private final List<String> _columnList;
    private final List<List<String>> _results;

    SelectionResults(List<String> columnList, List<List<String>> results) {
      _columnList = columnList;
      _results = results;
    }

    @JsonProperty("columns")
    public List<String> getColumnList() {
      return _columnList;
    }

    @JsonProperty("results")
    public List<List<String>> getResults() {
      return _results;
    }
  }

  class AggregationResult {
    private String _value;
    private String _function;
    List<GroupValue> _groupValues;
    List<String> _groupByColumns;

    AggregationResult(String function, String value) {
      _function = function;
      if (_function.equalsIgnoreCase("count_*")) {
        _function = "count_star";
      }
      _value = value;
      _groupValues = null;
    }

    public AggregationResult(List<GroupValue> group, List<String> groupByColumns, String function) {
      _groupValues = group;
      _groupByColumns = groupByColumns;
      _function = function;
      _value = null;
    }

    @JsonProperty("function")
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public String getFunction() {
      return _function;
    }

    @JsonProperty("value")
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public String getValue() {
      return _value;
    }

    @JsonProperty("groupByResult")
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public List<GroupValue> getGroupValues() {
      return _groupValues;
    }

    @JsonProperty("groupByColumns")
    public List<String> getGroupByColumns() {
      return _groupByColumns;
    }
  }

  class GroupValue {
    private final String _value;
    private final List<String> _group;

    GroupValue(String value, List<String> group) {
      _value = value;
      _group = group;
    }

    @JsonProperty("value")
    public String getValue() {
      return _value;
    }

    @JsonProperty("group")
    public List<String> getGroup() {
      return _group;
    }
  }

  @Override
  public String toString() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (IOException e) {
      return null;
    }
  }
}
