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

import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class ResultTable implements Iterable<ResultTable.Row> {
  List<Row> _rows;
  private final List<ColumnMetadata> _columnMetadatas;
  private final Map<String, Integer> _columnMap;
  private final int _numRows;
  private static int numRows;

  ResultTable(List<ColumnMetadata> columns, int numRows) {
    _columnMetadatas = columns;
    _numRows = numRows;
    _rows = new ArrayList<>(_numRows);

    for (int i = 0; i < _numRows; ++i) {
      _rows.add(i, new Row());
    }

    int index = 0;
    _columnMap = new HashMap<>();
    for (ColumnMetadata metadata : columns) {
      if (metadata != null) {
        _columnMap.put(metadata.getColumnName(), index);
      }
      ++index;
    }
  }

  public static int getNumRows() {
    return numRows;
  }

  public void add(int rowId, Object value) {
    Row row = _rows.get(rowId);
    row.add(value);
  }

  public Iterator<Row> iterator() {
    return _rows.iterator();
  }

  public List<Row> getRows() {
    return _rows;
  }

  public List<ColumnMetadata> getColumnMetadatas() {
    return _columnMetadatas;
  }

  public ColumnMetadata getColumnMetadata(String column) {
    int index = _columnMap.get(column);
    return _columnMetadatas.get(index);
  }

  public Object get(int rowId, int colId) {
    return _rows.get(rowId).get(colId);
  }

  public ResultTable append(ResultTable resultsToAppend) {
    _rows.addAll(resultsToAppend._rows);
    return this;
  }

  public ResultTable values(Map<String, Dictionary> dictionaryMap) {
    ResultTable results = new ResultTable(_columnMetadatas, 0);

    for (Row row : _rows) {
      Row valuesRow = new Row();

      for (int colId = 0; colId < row.size(); ++colId) {
        String column = _columnMetadatas.get(colId).getColumnName();
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

      results._rows.add(valuesRow);
    }

    return results;
  }

  public int size() {
    return _numRows;
  }

  public void print() {
    for (Row row : _rows) {
      row.print();
    }
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
        System.out.println(_cols.get(i));
      }
    }
  }
}
