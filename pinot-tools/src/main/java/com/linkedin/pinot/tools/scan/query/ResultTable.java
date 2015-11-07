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
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import java.util.ArrayList;
import java.util.List;

public class ResultTable {
  List<Row> _rows;
  private final List<ColumnMetadata> _columns;
  private final int _numRows;

  ResultTable(List<ColumnMetadata> columns, int numRows) {
    _columns = columns;
    _numRows = numRows;
    _rows = new ArrayList<>(_numRows);

    for (int i = 0; i < _numRows; ++i) {
      _rows.add(i, new Row());
    }
  }

  public void add(int rowId, Object value) {
    Row row = _rows.get(rowId);
    row.add(value);
  }

  public void aggregate(ResultTable projectionResult, List<AggregationInfo> aggregationsInfo) {
  }

  public void groupByAggregation(ResultTable projectionResult, String[] groupByColumns, String[] aggColumns,
      String[] aggFunctions) {
  }

  class Row {
    List<Object> _cols;

    public Row() {
      _cols = new ArrayList<>();
    }

    public void add(Object value) {
      _cols.add(value);
    }
  }
}
