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

import com.linkedin.pinot.core.query.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;


public class QueryResponse {
  private int _numDocsScanned;
  private int _totalDocs;
  private long _timeUsedMs;

  SelectionResults _selectionResults;
  List<AggregationResult> _aggregationResults;
  List<AggregationGroupByResult> _aggregationGroupByResults;

  QueryResponse(ResultTable resultTable) {
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
        throw new RuntimeException("Invalid result type");
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

    for (Pair pair : columnList) {
      if (pair.getSecond() == null) {
        groupByColumns.add(pair.getFirst().toString());
      }
    }

    int numGroupByColumns = groupByColumns.size();
    int numAggregationColumns = columnList.size() - numGroupByColumns;

    _aggregationGroupByResults = new ArrayList<>(numAggregationColumns);
    List<List<String>> groupValues = new ArrayList<>();

    for (ResultTable.Row row : resultTable) {
      int colId = 0;
      List<String> group = new ArrayList<>();

      for (Object value : row) {
        // All group by columns come first.
        if (columnList.get(colId).getSecond() != null) {
          break;
        }
        group.add(value.toString());
        ++colId;
      }
      groupValues.add(group);
    }

    for (int colId = numGroupByColumns; colId < columnList.size(); ++colId) {
      AggregationGroupByResult
          groupByResult = new AggregationGroupByResult(resultTable.getFunction(colId), groupByColumns);

      int rowId = 0;
      for (ResultTable.Row row : resultTable) {
        String value = row.get(colId).toString();
        GroupValue groupValue = new GroupValue(value, groupValues.get(rowId));
        groupByResult.addGroupValue(groupValue);
        ++rowId;
      }

      _aggregationGroupByResults.add(groupByResult);
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

  @JsonProperty("aggregationGroupByResults")
  @JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
  List<AggregationGroupByResult> getAggregationGroupByResults() {
    return _aggregationGroupByResults;
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

    AggregationResult(String function, String value) {
      _function = function;
      if (_function.equalsIgnoreCase("count_*")) {
        _function = "count_star";
      }
      _value = value;
    }

    public String getFunction() {
      return _function;
    }

    public String getValue() {
      return _value;
    }
  }

  class GroupValue {
    private final String _value;
    private final List<String> _group;

    GroupValue(String value, List<String> group) {
      _value = value;
      _group = group;
    }

    public String getValue() {
      return _value;
    }

    public List<String> getGroup() {
      return _group;
    }
  }

  class AggregationGroupByResult {
    String _function;
    List<GroupValue> _groupValues;
    List<String> _groupByColumns;

    AggregationGroupByResult(String function, List<String> groupByColumns) {
      _function = function;
      if (_function.equalsIgnoreCase("count_*")) {
        _function = "count_star";
      }
      _groupByColumns = groupByColumns;
      _groupValues = new ArrayList<>();
    }

    public void addGroupValue(GroupValue groupValue) {
      _groupValues.add(groupValue);
    }

    @JsonProperty("groupByResult")
    public List<GroupValue> getGroupValues() {
      return _groupValues;
    }

    @JsonProperty("function")
    public String getFunction() {
      return _function;
    }

    @JsonProperty("groupByColumns")
    public List<String> getGroupByColumns() {
      return _groupByColumns;
    }
  }
}
