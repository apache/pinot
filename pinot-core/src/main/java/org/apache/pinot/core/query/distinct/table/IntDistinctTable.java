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
package org.apache.pinot.core.query.distinct.table;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.RoaringBitmap;


public class IntDistinctTable extends DistinctTable {
  protected final IntOpenHashSet _valueSet;
  protected final OrderByExpressionContext _orderByExpression;

  protected IntHeapPriorityQueue _priorityQueue;

  /**
   * Constructor for distinct table without data table (on the server side).
   */
  public IntDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression) {
    super(dataSchema, limit, nullHandlingEnabled);

    _valueSet = new IntOpenHashSet(Math.min(limit, MAX_INITIAL_CAPACITY));
    _orderByExpression = orderByExpression;
  }

  /**
   * Constructor for distinct table with data table (on the broker side).
   */
  public IntDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression, DataTable dataTable) {
    super(dataSchema, limit, nullHandlingEnabled);

    int numRows = dataTable.getNumberOfRows();
    _valueSet = new IntOpenHashSet(numRows);
    _orderByExpression = orderByExpression;

    RoaringBitmap nullRowIds = nullHandlingEnabled ? dataTable.getNullRowIds(0) : null;
    if (nullRowIds == null) {
      for (int i = 0; i < numRows; i++) {
        _valueSet.add(dataTable.getInt(i, 0));
      }
    } else {
      assert nullRowIds.getCardinality() == 1;
      addNull();
      int nullRowId = nullRowIds.first();
      if (nullRowId == 0) {
        for (int i = 1; i < numRows; i++) {
          _valueSet.add(dataTable.getInt(i, 0));
        }
      } else {
        // For backward compatibility where null value is not stored as the first row
        for (int i = 0; i < nullRowId; i++) {
          _valueSet.add(dataTable.getInt(i, 0));
        }
        for (int i = nullRowId + 1; i < numRows; i++) {
          _valueSet.add(dataTable.getInt(i, 0));
        }
      }
    }
    assert _valueSet.size() <= limit;
  }

  @Override
  public boolean hasOrderBy() {
    return _orderByExpression != null;
  }

  public boolean addWithoutOrderBy(int value) {
    assert _valueSet.size() < _limit;
    _valueSet.add(value);
    return _valueSet.size() >= _limitWithoutNull;
  }

  public void addWithOrderBy(int value) {
    assert _valueSet.size() <= _limit;
    if (_valueSet.size() < _limit) {
      _valueSet.add(value);
      return;
    }
    if (_valueSet.contains(value)) {
      return;
    }
    if (_priorityQueue == null) {
      _priorityQueue = new IntHeapPriorityQueue(_valueSet, getComparator(_orderByExpression));
    }
    int firstValue = _priorityQueue.firstInt();
    if (_priorityQueue.comparator().compare(value, firstValue) > 0) {
      _valueSet.remove(firstValue);
      _valueSet.add(value);
      _priorityQueue.dequeueInt();
      _priorityQueue.enqueue(value);
    }
  }

  protected IntComparator getComparator(OrderByExpressionContext orderByExpression) {
    return orderByExpression.isAsc() ? (v1, v2) -> Integer.compare(v2, v1) : Integer::compare;
  }

  public void addUnbounded(int value) {
    _valueSet.add(value);
  }

  @Override
  public void mergeDistinctTable(DistinctTable distinctTable) {
    IntDistinctTable intDistinctTable = (IntDistinctTable) distinctTable;
    if (intDistinctTable._hasNull) {
      addNull();
    }
    IntIterator intIterator = intDistinctTable._valueSet.iterator();
    if (hasLimit()) {
      if (hasOrderBy()) {
        while (intIterator.hasNext()) {
          addWithOrderBy(intIterator.nextInt());
        }
      } else {
        while (intIterator.hasNext()) {
          if (addWithoutOrderBy(intIterator.nextInt())) {
            return;
          }
        }
      }
    } else {
      // NOTE: Do not use _valueSet.addAll() to avoid unnecessary resize when most values are common.
      while (intIterator.hasNext()) {
        addUnbounded(intIterator.nextInt());
      }
    }
  }

  @Override
  public boolean mergeDataTable(DataTable dataTable) {
    int numRows = dataTable.getNumberOfRows();
    RoaringBitmap nullRowIds = _nullHandlingEnabled ? dataTable.getNullRowIds(0) : null;
    if (nullRowIds == null) {
      return addValues(dataTable, 0, numRows);
    } else {
      assert nullRowIds.getCardinality() == 1;
      addNull();
      int nullRowId = nullRowIds.first();
      if (nullRowId == 0) {
        return addValues(dataTable, 1, numRows);
      } else {
        // For backward compatibility where null value is not stored as the first row
        return addValues(dataTable, 0, nullRowId) || addValues(dataTable, nullRowId + 1, numRows);
      }
    }
  }

  private boolean addValues(DataTable dataTable, int from, int to) {
    if (hasLimit()) {
      if (hasOrderBy()) {
        for (int i = from; i < to; i++) {
          addWithOrderBy(dataTable.getInt(i, 0));
        }
      } else {
        for (int i = from; i < to; i++) {
          if (addWithoutOrderBy(dataTable.getInt(i, 0))) {
            return true;
          }
        }
      }
    } else {
      for (int i = from; i < to; i++) {
        addUnbounded(dataTable.getInt(i, 0));
      }
    }
    return false;
  }

  @Override
  public int size() {
    int numValues = _valueSet.size();
    return _hasNull ? numValues + 1 : numValues;
  }

  @Override
  public boolean isSatisfied() {
    return _orderByExpression == null && _valueSet.size() >= _limitWithoutNull;
  }

  @Override
  public List<Object[]> getRows() {
    List<Object[]> rows = new ArrayList<>(size());
    if (_hasNull) {
      rows.add(new Object[]{null});
    }
    IntIterator intIterator = _valueSet.iterator();
    while (intIterator.hasNext()) {
      rows.add(new Object[]{intIterator.nextInt()});
    }
    return rows;
  }

  @Override
  public DataTable toDataTable()
      throws IOException {
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(_dataSchema);
    if (_hasNull) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, CommonConstants.NullValuePlaceHolder.INT);
      dataTableBuilder.finishRow();
    }
    int numRowsAdded = 0;
    IntIterator intIterator = _valueSet.iterator();
    while (intIterator.hasNext()) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(numRowsAdded);
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, intIterator.nextInt());
      dataTableBuilder.finishRow();
      numRowsAdded++;
    }
    if (_hasNull) {
      RoaringBitmap nullBitmap = new RoaringBitmap();
      nullBitmap.add(0);
      dataTableBuilder.setNullRowIds(nullBitmap);
    }
    return dataTableBuilder.build();
  }

  @Override
  public ResultTable toResultTable() {
    return hasOrderBy() ? toResultTableWithOrderBy() : toResultTableWithoutOrderBy();
  }

  private ResultTable toResultTableWithOrderBy() {
    int[] sortedValues;
    if (_priorityQueue != null) {
      int numValues = _priorityQueue.size();
      sortedValues = new int[numValues];
      for (int i = numValues - 1; i >= 0; i--) {
        sortedValues[i] = _priorityQueue.dequeueInt();
      }
    } else {
      sortedValues = _valueSet.toIntArray();
      Arrays.sort(sortedValues);
      if (!_orderByExpression.isAsc()) {
        ArrayUtils.reverse(sortedValues);
      }
    }
    int numValues = sortedValues.length;
    assert numValues <= _limit;
    List<Object[]> rows;
    ColumnDataType columnDataType = _dataSchema.getColumnDataType(0);
    if (_hasNull) {
      if (numValues == _limit) {
        rows = new ArrayList<>(_limit);
        if (_orderByExpression.isNullsLast()) {
          addRows(columnDataType, sortedValues, numValues, rows);
        } else {
          rows.add(new Object[]{null});
          addRows(columnDataType, sortedValues, numValues - 1, rows);
        }
      } else {
        rows = new ArrayList<>(numValues + 1);
        if (_orderByExpression.isNullsLast()) {
          addRows(columnDataType, sortedValues, numValues, rows);
          rows.add(new Object[]{null});
        } else {
          rows.add(new Object[]{null});
          addRows(columnDataType, sortedValues, numValues, rows);
        }
      }
    } else {
      rows = new ArrayList<>(numValues);
      addRows(columnDataType, sortedValues, numValues, rows);
    }
    return new ResultTable(_dataSchema, rows);
  }

  private static void addRows(ColumnDataType columnDataType, int[] values, int length, List<Object[]> rows) {
    if (columnDataType == ColumnDataType.BOOLEAN) {
      for (int i = 0; i < length; i++) {
        rows.add(new Object[]{values[i] == 1});
      }
    } else {
      for (int i = 0; i < length; i++) {
        rows.add(new Object[]{values[i]});
      }
    }
  }

  private ResultTable toResultTableWithoutOrderBy() {
    int numValues = _valueSet.size();
    assert numValues <= _limit;
    List<Object[]> rows;
    ColumnDataType columnDataType = _dataSchema.getColumnDataType(0);
    if (_hasNull && numValues < _limit) {
      rows = new ArrayList<>(numValues + 1);
      addRows(columnDataType, _valueSet, rows);
      rows.add(new Object[]{null});
    } else {
      rows = new ArrayList<>(numValues);
      addRows(columnDataType, _valueSet, rows);
    }
    return new ResultTable(_dataSchema, rows);
  }

  private static void addRows(ColumnDataType columnDataType, IntOpenHashSet values, List<Object[]> rows) {
    IntIterator intIterator = values.iterator();
    if (columnDataType == ColumnDataType.BOOLEAN) {
      while (intIterator.hasNext()) {
        rows.add(new Object[]{intIterator.nextInt() == 1});
      }
    } else {
      while (intIterator.hasNext()) {
        rows.add(new Object[]{intIterator.nextInt()});
      }
    }
  }
}
