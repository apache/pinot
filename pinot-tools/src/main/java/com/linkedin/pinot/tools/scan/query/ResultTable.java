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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResultTable implements Iterable<ResultTable.Row> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultTable.class);

  List<Row> _rows;
  private List<Pair> _columnList;
  private Map<String, Integer> _columnMap;
  int _numDocsScanned;
  private long _processingTime;
  private int _totalDocs;

  ResultTable(List<Pair> columns, int numRows) {
    _columnList = new ArrayList<>(columns);
    _rows = new ArrayList<>(numRows);
    _numDocsScanned = 0;

    int index = 0;
    _columnMap = new HashMap<>();
    for (Pair pair : columns) {
      _columnMap.put((String) pair.getFirst(), index);
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
    _rows.addAll(resultsToAppend._rows);
    return this;
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
    // Assumes count(*) column is to be added at the end.
    int index = _rows.get(0).size() - 1;
    _columnMap.put("*", index);
    _columnList.add(new Pair("count", "star"));
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

  public int getColumnIndex(String column) {
    return _columnMap.get(column);
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
      _columnList = new ArrayList<>(_columnMap.keySet());
    }

    public void add(Object value) {
      _cols.add(value);
    }

    public Object get(int index) {
      return _cols.get(index);
    }

    public Object get(String column) {
      int index = _columnMap.get(column);
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
        String value = (object instanceof Object []) ? Arrays.toString((Object []) object) : object.toString();
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
}
