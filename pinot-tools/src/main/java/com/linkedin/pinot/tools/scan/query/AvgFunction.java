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
import java.util.List;


public class AvgFunction extends AggregationFunc {

  AvgFunction(ResultTable rows, String column) {
    super(rows, column);
  }

  public ResultTable run() {
    Double average = 0.0;
    int numRows = 0;

    for (ResultTable.Row row : _rows) {
      average += new Double(row.get(_column).toString());
      ++numRows;
    }

    average = average / numRows;
    List<ColumnMetadata> columnMetadatas = new ArrayList<ColumnMetadata>();
    columnMetadatas.add(_columnMetadata);

    ResultTable resultTable = new ResultTable(columnMetadatas, 1);
    resultTable.add(0, average);

    return resultTable;
  }
}
