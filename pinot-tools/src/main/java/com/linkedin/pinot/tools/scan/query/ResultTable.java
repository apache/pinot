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

import com.linkedin.pinot.core.segment.index.readers.Dictionary;
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
  private List<String> _columnList;
  private Map<String, Integer> _columnMap;
  int _numDocsScanned;
  private long _processingTime;
  private int _totalDocs;

  ResultTable(List<String> columns, int numRows) {
    _columnList = columns;
    _rows = new ArrayList<>(numRows);
    _numDocsScanned = 0;

    for (int i = 0; i < numRows; ++i) {
      _rows.add(i, new Row());
    }

    int index = 0;
    _columnMap = new HashMap<>();
    for (String column : columns) {
      _columnMap.put(column, index);
      ++index;
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

  public List<String> getColumnList() {
    return _columnList;
  }

  public Object get(int rowId, int colId) {
    return _rows.get(rowId).get(colId);
  }

  public ResultTable append(ResultTable resultsToAppend) {
    _rows.addAll(resultsToAppend._rows);
    return this;
  }

  public ResultTable values(Map<String, Dictionary> dictionaryMap, boolean addCountStar) {
    ResultTable results = new ResultTable(_columnList, 0);

    for (Row row : _rows) {
      Row valuesRow = new Row();

      for (int colId = 0; colId < row.size(); ++colId) {
        String column = _columnList.get(colId);
        Dictionary dictionary = dictionaryMap.get(column);
        Object object = row.get(colId);

        if (object instanceof Object[]) {
          Object[] objArray = (Object[]) object;
          Object[] valArray = new Object[objArray.length];

          for (int i = 0; i < objArray.length; ++i) {
            int dictId = (int) objArray[i];
            valArray[i] = dictionary.get(dictId);
          }
          valuesRow.add(valArray);
        } else {
          int dictId = (int) object;
          valuesRow.add(dictionary.get(dictId));
        }
      }

      if (addCountStar) {
        valuesRow.add(1);
      }
      results._rows.add(valuesRow);
    }

    if (addCountStar) {
      results.addCountStarColumn();
    }
    return results;
  }

  private void addCountStarColumn() {
    // Assumes count(*) column is to be added at the end.
    int index = _rows.get(0).size() - 1;
    _columnMap.put("*", index);
    _columnList.add("*");
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

  public long getProcessingTime() {
    return _processingTime;
  }

  public void setTotalDocs(int totalDocs) {
    _totalDocs = totalDocs;
  }

  public int getTotalDocs() {
    return _totalDocs;
  }

  class Row {
    List<Object> _cols;

    public Row() {
      _cols = new ArrayList<>();
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
  }
}
