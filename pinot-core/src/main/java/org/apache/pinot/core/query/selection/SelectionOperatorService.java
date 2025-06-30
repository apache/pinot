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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.RoaringBitmap;


/**
 *
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
  private final int _limit;
  private final int _numRowsToKeep;
  // TODO: consider moving this to a util class

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
  }

  /**
   * Reduce multiple sorted (and unsorted) dataTables into a single resultTable, ordered, limited, and offset
   * @param dataTables dataTables to be reduced
   * @return resultTable
   */
  public ResultTable reduceWithOrdering(Collection<DataTable> dataTables) {
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

    if (dataTables.size() == 1) {
      // short circuit single table case
      DataTable dataTable = dataTables.iterator().next();
      List<Object[]> resultRows = processSingleDataTable(dataTable);
      return new ResultTable(_dataSchema, resultRows);
    }

    // n-way merge sorted dataTable, we need to access dataTable by index
    List<DataTable> dataTableList = new ArrayList<>(dataTables);
    List<Object[]> mergedRows = nWayMergeDataTables(dataTableList);
    return new ResultTable(_dataSchema, mergedRows);
  }

  /**
   * Merge sorted dataTables using N-way merge
   * @param dataTables sorted dataTables
   * @return sorted rows
   */
  private List<Object[]> nWayMergeDataTables(List<DataTable> dataTables) {
    Comparator<Object[]> comparator = OrderByComparatorFactory.getComparator(_queryContext.getOrderByExpressions(),
        _queryContext.isNullHandlingEnabled());
    Comparator<MergeItem> mergeItemComparator = (MergeItem o1, MergeItem o2) -> comparator.compare(o1._row, o2._row);
    PriorityQueue<MergeItem> mergeSortRows =
        new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
            mergeItemComparator);

    // populate pq
    int numDataTables = dataTables.size();
    int[] nextRowIds = new int[numDataTables];
    int[] numRows = new int[numDataTables];
    RoaringBitmap[][] dataTableNullBitmaps = getdataTableNullBitmaps(dataTables);
    for (int i = 0; i < numDataTables; i++) {
      DataTable dataTable = dataTables.get(i);
      numRows[i] = dataTable.getNumberOfRows();
      if (numRows[i] > 0) {
        Object[] row = getDataTableRow(dataTable, 0, dataTableNullBitmaps[i]);
        MergeItem mergeItem = new MergeItem(row, i);
        mergeSortRows.add(mergeItem);
        nextRowIds[i] = 1;
      }
    }

    // merge
    List<Object[]> resultRows = new ArrayList<>();
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    int offsetCounter = _offset;
    while (resultRows.size() < _limit && !mergeSortRows.isEmpty()) {
      MergeItem item = mergeSortRows.poll();
      if (offsetCounter > 0) {
        offsetCounter--;
      } else {
        Object[] row = item._row;
        Object[] resultRow = formatRow(numColumns, row, columnDataTypes);
        resultRows.add(resultRow);
      }
      int dataTableId = item._dataTableId;
      int nextRowId = nextRowIds[dataTableId]++;
      if (nextRowId >= numRows[dataTableId]) {
        continue;
      }
      DataTable dataTable = dataTables.get(dataTableId);
      Object[] row = getDataTableRow(dataTable, nextRowId, dataTableNullBitmaps[dataTableId]);
      MergeItem mergeItem = new MergeItem(row, dataTableId);
      mergeSortRows.add(mergeItem);
    }
    return resultRows;
  }

  private List<Object[]> processSingleDataTable(DataTable dataTable) {
    List<Object[]> resultRows = new ArrayList<>();
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = _dataSchema.size();
    int numRows = dataTable.getNumberOfRows();

    if (numRows <= _offset) {
      return Collections.emptyList();
    }

    int start = _offset;
    int end = Math.min(numRows, _offset + _limit);

    if (_queryContext.isNullHandlingEnabled()) {
      RoaringBitmap[] nullBitmaps = getNullBitmap(dataTable);
      for (int rowId = start; rowId < end; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        setNullsForRow(nullBitmaps, rowId, row);
        Object[] resultRow = formatRow(numColumns, row, columnDataTypes);
        resultRows.add(resultRow);
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
      }
    } else {
      for (int rowId = start; rowId < end; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        Object[] resultRow = formatRow(numColumns, row, columnDataTypes);
        resultRows.add(resultRow);
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
      }
    }
    return resultRows;
  }

  /** get nullBitmaps for dataTables */
  private RoaringBitmap[][] getdataTableNullBitmaps(List<DataTable> dataTables) {
    RoaringBitmap[][] dataTableNullBitmaps = new RoaringBitmap[dataTables.size()][];
    if (!_queryContext.isNullHandlingEnabled()) {
      return dataTableNullBitmaps;
    }
    int idx = 0;
    for (DataTable dataTable : dataTables) {
      dataTableNullBitmaps[idx++] = getNullBitmap(dataTable);
    }
    return dataTableNullBitmaps;
  }

  /** get a single row from dataTable with null handling if nullBitmaps provided */
  private Object[] getDataTableRow(DataTable dataTable, int rowId, @Nullable RoaringBitmap[] nullBitmaps) {
    Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
    if (nullBitmaps != null) {
      setNullsForRow(nullBitmaps, rowId, row);
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
    }
    return row;
  }

  private static void setNullsForRow(RoaringBitmap[] nullBitmaps, int rowId, Object[] row) {
    for (int colId = 0; colId < nullBitmaps.length; colId++) {
      if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
        row[colId] = null;
      }
    }
  }

  private static RoaringBitmap[] getNullBitmap(DataTable dataTable) {
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[dataTable.getDataSchema().size()];
    for (int colId = 0; colId < nullBitmaps.length; colId++) {
      nullBitmaps[colId] = dataTable.getNullRowIds(colId);
    }
    return nullBitmaps;
  }

  private Object[] formatRow(int numColumns, Object[] row, DataSchema.ColumnDataType[] columnDataTypes) {
    Object[] resultRow = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      Object value = row[_columnIndices[i]];
      if (value != null) {
        resultRow[i] = columnDataTypes[i].convertAndFormat(value);
      }
    }
    return resultRow;
  }

  /**
   * Heapsort unsorted dataTables, this code is unreachable for
   * server version >= 1.3.0 as server always performs sorting
   * TODO: remove after 1.2.0 no longer supported
   * @param dataTables unsorted dataTables
   * @return sorted rows
   */
  private List<Object[]> heapSortDataTable(Collection<DataTable> dataTables) {
    Comparator<Object[]> comparator = OrderByComparatorFactory.getComparator(_queryContext.getOrderByExpressions(),
        _queryContext.isNullHandlingEnabled());
    PriorityQueue<Object[]> heapSortRows =
        new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
            comparator.reversed());
    // reduce
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      if (_queryContext.isNullHandlingEnabled()) {
        RoaringBitmap[] nullBitmaps = getNullBitmap(dataTable);
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          setNullsForRow(nullBitmaps, rowId, row);
          SelectionOperatorUtils.addToPriorityQueue(row, heapSortRows, _numRowsToKeep);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          SelectionOperatorUtils.addToPriorityQueue(row, heapSortRows, _numRowsToKeep);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      }
    }
    // render
    LinkedList<Object[]> resultRows = new LinkedList<>();
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    while (heapSortRows.size() > _offset) {
      Object[] row = heapSortRows.poll();
      assert row != null;
      Object[] resultRow = formatRow(numColumns, row, columnDataTypes);
      resultRows.addFirst(resultRow);
    }
    return resultRows;
  }
}
