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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResultTable implements Iterable<ResultTable.Row> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultTable.class);
  private static final String AVERAGE = "avg";
  private static final String DISTINCT_COUNT = "distinctCount";

  private void convertSumToAvgIfNeeded() {
    if (isEmpty()) {
      return;
    }

    List<Integer> averageColumns = new ArrayList<>();
    int i = 0;
    for (Pair pair : _columnList) {
      String function = (String) pair.getSecond();
      if (function != null && function.equalsIgnoreCase(AVERAGE)) {
        averageColumns.add(i);
      }
      ++i;
    }
    // No average columns found.
    if (averageColumns.isEmpty()) {
      return;
    }

    for (Row row : _rows) {
      for (int colId : averageColumns) {
        double[] value = (double[]) row.get(colId);
        double average = (value[1] != 0) ? (value[0] / value[1]) : 0;
        row.set(colId, average);
      }
    }
  }

  private void computeDistinctCount() {
    if (isEmpty()) {
      return;
    }

    List<Integer> distinctCountColumns = new ArrayList<>();
    int i = 0;
    for (Pair pair : _columnList) {
      String function = (String) pair.getSecond();
      if (function != null && function.equalsIgnoreCase(DISTINCT_COUNT)) {
        distinctCountColumns.add(i);
      }
      ++i;
    }

    // No distinctCount columns found.
    if (distinctCountColumns.isEmpty()) {
      return;
    }

    for (Row row : _rows) {
      for (int colId : distinctCountColumns) {
        Set<String> distinctCountSet = (Set<String>) row.get(colId);
        row.set(colId, distinctCountSet.size());
      }
    }
  }

  public boolean isEmpty() {
    return _rows.isEmpty();
  }

  public void seal() {
    convertSumToAvgIfNeeded();
    computeDistinctCount();
  }

  enum ResultType {
    Selection, Aggregation, AggregationGroupBy, Invalid
  }

  List<Row> _rows;
  private List<Pair> _columnList;
  private Map<String, Integer> _columnMap;
  int _numDocsScanned;
  private long _processingTime;
  private int _totalDocs;
  private ResultType _resultType = ResultType.Invalid;

  ResultTable(List<Pair> columns, int numRows) {
    _columnList = new ArrayList<>(columns);
    _rows = new ArrayList<>(numRows);
    _numDocsScanned = 0;

    int index = 0;
    _columnMap = new HashMap<>();
    for (Pair pair : columns) {
      String key = (String) pair.getFirst() + "_" + (String) pair.getSecond();
      _columnMap.put(key, index);
      ++index;
    }

    for (int i = 0; i < numRows; ++i) {
      _rows.add(i, new Row(_columnMap));
    }
  }

  public void add(int rowId, Object value) {
    Row row = _rows.get(rowId);
    row.add(value);
  }

  @Override
  public Iterator<Row> iterator() {
    return _rows.iterator();
  }

  public List<Row> getRows() {
    return _rows;
  }

  public Object get(int rowId, int colId) {
    return _rows.get(rowId).get(colId);
  }

  public ResultTable append(ResultTable resultsToAppend) {
    if (!resultsToAppend.isEmpty()) {
      _rows.addAll(resultsToAppend._rows);
    }
    return this;
  }

  public Row getRow(int i) {
    return _rows.get(i);
  }

  public String getColumn(int columnId) {
    return (String) _columnList.get(columnId).getFirst();
  }

  public String getFunction(int columnId) {
    Pair pair = _columnList.get(columnId);
    String column = (String) pair.getFirst();
    String function = (String) pair.getSecond();
    return function + "_" + column;
  }

  void addCountStarColumn() {
    _columnMap.put("*_count", _columnList.size());
    _columnList.add(new Pair("*", "count"));
  }

  public int size() {
    return _rows.size();
  }

  public void print() {
    for (Row row : _rows) {
      row.print();
    }
    LOGGER.info("Docs scanned: " + _numDocsScanned);
    LOGGER.info("Total Docs  : " + _totalDocs);
    LOGGER.info("timeUsedMs: " + _processingTime);
  }

  public void setNumDocsScanned(int numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  public int getNumDocsScanned() {
    return _numDocsScanned;
  }

  public void setProcessingTime(long processingTime) {
    _processingTime = processingTime;
  }

  public void setTotalDocs(int totalDocs) {
    _totalDocs = totalDocs;
  }

  public int getTotalDocs() {
    return _totalDocs;
  }

  public long getProcessingTime() {
    return _processingTime;
  }

  public void append(Row row) {
    _rows.add(row);
  }

  public Map<String, Integer> getColumnMap() {
    return _columnMap;
  }

  public List<Pair> getColumnList() {
    return _columnList;
  }

  class Row implements Iterable<Object> {
    List<Object> _cols;
    private Map<String, Integer> _columnMap;
    private ArrayList<String> _columnList;

    public Row(Map<String, Integer> columnMap) {
      _columnMap = columnMap;
      _cols = new ArrayList<>();

      // Make a copy as new column (count_star may be added to the passed in ref
      _columnList = new ArrayList<>(_columnMap.keySet());

      // Need to maintain the order.
      for (Map.Entry<String, Integer> entry : _columnMap.entrySet()) {
        _columnList.set(entry.getValue(), entry.getKey());
      }
    }

    public void add(Object value) {
      _cols.add(value);
    }

    public Object get(int index) {
      return _cols.get(index);
    }

    public Object get(String column, String function) {
      String key = column + "_" + function;
      int index = _columnMap.get(key);
      return get(index);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < _cols.size(); ++i) {
        sb.append(_cols.get(i).toString());
      }
      return sb.toString();
    }

    public int size() {
      return _cols.size();
    }

    public void print() {
      for (int i = 0; i < _cols.size(); ++i) {
        Object object = _cols.get(i);
        String value = (object instanceof Object[]) ? Arrays.toString((Object[]) object) : object.toString();
        LOGGER.info(_columnList.get(i) + " " + value);
      }
    }

    @Override
    public Iterator<Object> iterator() {
      return _cols.iterator();
    }

    public void set(int colId, Object object) {
      _cols.set(colId, object);
    }
  }

  public void setResultType(ResultType resultType) {
    _resultType = resultType;
  }

  public ResultType getResultType() {
    return _resultType;
  }
}
