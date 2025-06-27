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
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.RoaringBitmap;


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
  private int _offset;
  private final int _limit;
  private final int _numRowsToKeep;
  // NOTE: this pq is only useful for server version < 1.3.0
  private final PriorityQueue<Object[]> _heapSortRows;
  private final PriorityQueue<MergeItem> _mergeSortRows;
  private final Comparator<Object[]> _comparator;
  private final Comparator<MergeItem> _mergeComparator;

  /** Util class used for n-way merge */
  private static class MergeItem {
   final Object[] _row;
   final int _dataTableId;

   MergeItem(Object[] row, int dataTableId) {
      _row = row;
      _dataTableId = dataTableId;
    }
  }

  public SelectionOperatorService(QueryContext queryContext, DataSchema dataSchema, int[] columnIndices) {
    _queryContext = queryContext;
    _dataSchema = dataSchema;
    _columnIndices = columnIndices;
    // Select rows from offset to offset + limit.
    _offset = queryContext.getOffset();
    _limit = queryContext.getLimit();
    _numRowsToKeep = _offset + _limit;
    assert queryContext.getOrderByExpressions() != null;
    _comparator = OrderByComparatorFactory.getComparator(queryContext.getOrderByExpressions(),
        _queryContext.isNullHandlingEnabled());
    _heapSortRows =
        new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
            _comparator.reversed());
    _mergeComparator = (MergeItem o1, MergeItem o2) -> _comparator.compare(o1._row, o2._row);
    _mergeSortRows =
        new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
            _mergeComparator);
  }

  /**
   * Reduce multiple sorted (and unsorted) dataTables into a single resultTable, ordered, limited, and offset
   * @param dataTables dataTables to be reduced
   * @return resultTable
   */
  public ResultTable reduceWithOrderingAndRender(Collection<DataTable> dataTables) {
    boolean allSorted = true;
    for (DataTable dataTable : dataTables) {
      String sorted = dataTable.getMetadata().get(DataTable.MetadataKey.SORTED.getName());
      if (!Boolean.parseBoolean(sorted)) {
        allSorted = false;
        break;
      }
    }

    // TODO: backward compatible, to be removed after 1.2.0 no longer supported
    if (!allSorted) {
      List<Object[]> heapSortedRows = heapSortDataTable(dataTables);
      return new ResultTable(_dataSchema, heapSortedRows);
    } // end todo

    // n-way merge sorted dataTable
    List<Object[]> mergedRows = nWayMergeDataTables(dataTables);
    return new ResultTable(_dataSchema, mergedRows);
  }

  // process dataTable rows, perform null-handling
  private List<List<Object[]>> processDataTableRows(Collection<DataTable> dataTables) {
    List<List<Object[]>> dataTablesRowList = new ArrayList<>();
    for (DataTable dataTable : dataTables) {
      List<Object[]> rightRows = new ArrayList<>();
      int numRows = dataTable.getNumberOfRows();
      if (_queryContext.isNullHandlingEnabled()) {
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[dataTable.getDataSchema().size()];
        for (int colId = 0; colId < nullBitmaps.length; colId++) {
          nullBitmaps[colId] = dataTable.getNullRowIds(colId);
        }
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          for (int colId = 0; colId < nullBitmaps.length; colId++) {
            if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
              row[colId] = null;
            }
          }
          rightRows.add(row);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          rightRows.add(row);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      }
      dataTablesRowList.add(rightRows);
    }
    return dataTablesRowList;
  }

  /**
   * Merge sorted dataTables using N-way merge
   * @param dataTables sorted dataTables
   * @return sorted rows
   */
  private List<Object[]> nWayMergeDataTables(Collection<DataTable> dataTables) {
    List<List<Object[]>> dataTablesRowList = processDataTableRows(dataTables);
    // populate pq
    List<Object[]> resultRows = new ArrayList<>();
    int[] nextRowIds = new int[dataTablesRowList.size()];
    int[] numRows = new int[dataTablesRowList.size()];
    for (int i = 0; i < dataTablesRowList.size(); i++) {
      List<Object[]> rowList = dataTablesRowList.get(i);
      numRows[i] = rowList.size();
      if (!rowList.isEmpty()) {
        Object[] row = rowList.get(0);
        MergeItem mergeItem = new MergeItem(row, i);
        _mergeSortRows.add(mergeItem);
        nextRowIds[i] = 1;
      }
    }

    // merge
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    while (resultRows.size() < _limit && !_mergeSortRows.isEmpty()) {
      MergeItem item = _mergeSortRows.poll();
      if (_offset > 0) {
        _offset--;
      } else {
        Object[] row = item._row;
        Object[] resultRow = new Object[numColumns];
        for (int i = 0; i < numColumns; i++) {
          Object value = row[_columnIndices[i]];
          if (value != null) {
            resultRow[i] = columnDataTypes[i].convertAndFormat(value);
          }
        }
        resultRows.add(resultRow);
      }
      int dataTableId = item._dataTableId;
      int nextRowId = nextRowIds[dataTableId]++;
      if (nextRowId >= numRows[dataTableId]) {
        continue;
      }
      List<Object[]> rowList = dataTablesRowList.get(dataTableId);
      Object[] row = rowList.get(nextRowId);
      MergeItem mergeItem = new MergeItem(row, dataTableId);
      _mergeSortRows.add(mergeItem);
    }
    return resultRows;
  }

  /**
   * Heapsort unsorted dataTables, this code is unreachable for
   * server version >= 1.3.0 as server always performs sorting
   * TODO: remove after 1.2.0 no longer supported
   * @param dataTables unsorted dataTables
   * @return sorted rows
   */
  private List<Object[]> heapSortDataTable(Collection<DataTable> dataTables) {
    // reduce
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      if (_queryContext.isNullHandlingEnabled()) {
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[dataTable.getDataSchema().size()];
        for (int colId = 0; colId < nullBitmaps.length; colId++) {
          nullBitmaps[colId] = dataTable.getNullRowIds(colId);
        }
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          for (int colId = 0; colId < nullBitmaps.length; colId++) {
            if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
              row[colId] = null;
            }
          }
          SelectionOperatorUtils.addToPriorityQueue(row, _heapSortRows, _numRowsToKeep);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          SelectionOperatorUtils.addToPriorityQueue(row, _heapSortRows, _numRowsToKeep);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      }
    }
    // render
    LinkedList<Object[]> resultRows = new LinkedList<>();
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    while (_heapSortRows.size() > _offset) {
      Object[] row = _heapSortRows.poll();
      assert row != null;
      Object[] resultRow = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        Object value = row[_columnIndices[i]];
        if (value != null) {
          resultRow[i] = columnDataTypes[i].convertAndFormat(value);
        }
      }
      resultRows.addFirst(resultRow);
    }
    return resultRows;
  }
}
