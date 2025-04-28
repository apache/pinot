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
package org.apache.pinot.core.query.selection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;


/**
 * The <code>SelectionOperatorService</code> class provides the services for selection queries with
 * <code>ORDER BY</code>.
 * <p>Expected behavior:
 * <ul>
 *   <li>
 *     Return selection results with the same order of columns as user passed in.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For 'SELECT *', return columns with alphabetically order.
 *     <ul>
 *       <li>Eg. SELECT * FROM table -> [valA, valB, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Order by does not change the order of columns in selection results.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class SelectionOperatorService {
  private final QueryContext _queryContext;
  private final DataSchema _dataSchema;
  private final int[] _columnIndices;
  private final int _offset;
  private final int _numRowsToKeep;
  private List<Object[]> _rows;

  public SelectionOperatorService(QueryContext queryContext, DataSchema dataSchema, int[] columnIndices) {
    _queryContext = queryContext;
    _dataSchema = dataSchema;
    _columnIndices = columnIndices;
    // Select rows from offset to offset + limit.
    _offset = queryContext.getOffset();
    _numRowsToKeep = _offset + queryContext.getLimit();
    assert queryContext.getOrderByExpressions() != null;
    _rows = new ArrayList<>();
  }

  /**
   * Reduces a collection of {@link DataTable}s to selection rows for selection queries with <code>ORDER BY</code>.
   */
  public void reduceWithOrdering(Collection<DataTable> dataTables, boolean isSelectStarQuery,
                                 boolean hasSchemaMismatch) {
    Comparator<Object[]> comparator = OrderByComparatorFactory.getComparator(
            _queryContext.getOrderByExpressions(), _queryContext.isNullHandlingEnabled());
    List<String> reduceSelectColumns = null;
    if (isSelectStarQuery && hasSchemaMismatch) {
      reduceSelectColumns = SelectionOperatorUtils.getReducedColumns(dataTables);
    }
    _rows = SelectionOperatorUtils.reduceResults(dataTables, _numRowsToKeep, _queryContext.isNullHandlingEnabled(),
            reduceSelectColumns, comparator);
  }

  /**
   * Renders the selection rows to a {@link ResultTable} object for selection queries with <code>ORDER BY</code>.
   */
  public ResultTable renderResultTableWithOrdering() {
    List<Object[]> resultRows = new ArrayList<>();
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    int numRows = _rows.size();
    for (int index = _offset; index < numRows; index++) {
      Object[] row = _rows.get(index);
      Object[] resultRow = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        Object value = row[_columnIndices[i]];
        if (value != null) {
          resultRow[i] = columnDataTypes[i].convertAndFormat(value);
        }
      }
      resultRows.add(resultRow);
    }
    return new ResultTable(_dataSchema, resultRows);
  }
}
