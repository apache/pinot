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

import it.unimi.dsi.fastutil.floats.FloatComparator;
import it.unimi.dsi.fastutil.floats.FloatHeapPriorityQueue;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.RoaringBitmap;


public class FloatDistinctTable extends DistinctTable {
  private final FloatOpenHashSet _valueSet;
  private final OrderByExpressionContext _orderByExpression;

  private FloatHeapPriorityQueue _priorityQueue;

  /**
   * Constructor for distinct table without data table (on the server side).
   */
  public FloatDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression) {
    super(dataSchema, limit, nullHandlingEnabled);

    _valueSet = new FloatOpenHashSet(Math.min(limit, MAX_INITIAL_CAPACITY));
    _orderByExpression = orderByExpression;
  }

  /**
   * Constructor for distinct table with data table (on the broker side).
   */
  public FloatDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression, DataTable dataTable) {
    super(dataSchema, limit, nullHandlingEnabled);

    int numRows = dataTable.getNumberOfRows();
    _valueSet = new FloatOpenHashSet(numRows);
    _orderByExpression = orderByExpression;

    RoaringBitmap nullRowIds = nullHandlingEnabled ? dataTable.getNullRowIds(0) : null;
    if (nullRowIds == null) {
      for (int i = 0; i < numRows; i++) {
        _valueSet.add(dataTable.getFloat(i, 0));
      }
    } else {
      assert nullRowIds.getCardinality() == 1;
      addNull();
      int nullRowId = nullRowIds.first();
      if (nullRowId == 0) {
        for (int i = 1; i < numRows; i++) {
          _valueSet.add(dataTable.getFloat(i, 0));
        }
      } else {
        // For backward compatibility where null value is not stored as the first row
        for (int i = 0; i < nullRowId; i++) {
          _valueSet.add(dataTable.getFloat(i, 0));
        }
        for (int i = nullRowId + 1; i < numRows; i++) {
          _valueSet.add(dataTable.getFloat(i, 0));
        }
      }
    }
    assert _valueSet.size() <= limit;
  }

  @Override
  public boolean hasOrderBy() {
    return _orderByExpression != null;
  }

  public boolean addWithoutOrderBy(float value) {
    assert _valueSet.size() < _limit;
    _valueSet.add(value);
    return _valueSet.size() >= _limitWithoutNull;
  }

  public void addWithOrderBy(float value) {
    assert _valueSet.size() <= _limit;
    if (_valueSet.size() < _limit) {
      _valueSet.add(value);
      return;
    }
    if (_valueSet.contains(value)) {
      return;
    }
    if (_priorityQueue == null) {
      FloatComparator comparator = _orderByExpression.isAsc() ? (v1, v2) -> Float.compare(v2, v1) : Float::compare;
      _priorityQueue = new FloatHeapPriorityQueue(_valueSet, comparator);
    }
    float firstValue = _priorityQueue.firstFloat();
    if (_priorityQueue.comparator().compare(value, firstValue) > 0) {
      _valueSet.remove(firstValue);
      _valueSet.add(value);
      _priorityQueue.dequeueFloat();
      _priorityQueue.enqueue(value);
    }
  }

  public void addUnbounded(float value) {
    _valueSet.add(value);
  }

  @Override
  public void mergeDistinctTable(DistinctTable distinctTable) {
    FloatDistinctTable floatDistinctTable = (FloatDistinctTable) distinctTable;
    if (floatDistinctTable._hasNull) {
      addNull();
    }
    FloatIterator floatIterator = floatDistinctTable._valueSet.iterator();
    if (hasLimit()) {
      if (hasOrderBy()) {
        while (floatIterator.hasNext()) {
          addWithOrderBy(floatIterator.nextFloat());
        }
      } else {
        while (floatIterator.hasNext()) {
          if (addWithoutOrderBy(floatIterator.nextFloat())) {
            return;
          }
        }
      }
    } else {
      // NOTE: Do not use _valueSet.addAll() to avoid unnecessary resize when most values are common.
      while (floatIterator.hasNext()) {
        addUnbounded(floatIterator.nextFloat());
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
          addWithOrderBy(dataTable.getFloat(i, 0));
        }
      } else {
        for (int i = from; i < to; i++) {
          if (addWithoutOrderBy(dataTable.getFloat(i, 0))) {
            return true;
          }
        }
      }
    } else {
      for (int i = from; i < to; i++) {
        addUnbounded(dataTable.getFloat(i, 0));
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
    FloatIterator floatIterator = _valueSet.iterator();
    while (floatIterator.hasNext()) {
      rows.add(new Object[]{floatIterator.nextFloat()});
    }
    return rows;
  }

  @Override
  public DataTable toDataTable()
      throws IOException {
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(_dataSchema);
    if (_hasNull) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, CommonConstants.NullValuePlaceHolder.FLOAT);
      dataTableBuilder.finishRow();
    }
    int numRowsAdded = 0;
    FloatIterator floatIterator = _valueSet.iterator();
    while (floatIterator.hasNext()) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(numRowsAdded);
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, floatIterator.nextFloat());
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
  public ResultTableRows toResultTable() {
    return hasOrderBy() ? toResultTableWithOrderBy() : toResultTableWithoutOrderBy();
  }

  private ResultTableRows toResultTableWithOrderBy() {
    float[] sortedValues;
    if (_priorityQueue != null) {
      int numValues = _priorityQueue.size();
      sortedValues = new float[numValues];
      for (int i = numValues - 1; i >= 0; i--) {
        sortedValues[i] = _priorityQueue.dequeueFloat();
      }
    } else {
      sortedValues = _valueSet.toFloatArray();
      Arrays.sort(sortedValues);
      if (!_orderByExpression.isAsc()) {
        ArrayUtils.reverse(sortedValues);
      }
    }
    int numValues = sortedValues.length;
    assert numValues <= _limit;
    List<Object[]> rows;
    if (_hasNull) {
      if (numValues == _limit) {
        rows = new ArrayList<>(_limit);
        if (_orderByExpression.isNullsLast()) {
          addRows(sortedValues, numValues, rows);
        } else {
          rows.add(new Object[]{null});
          addRows(sortedValues, numValues - 1, rows);
        }
      } else {
        rows = new ArrayList<>(numValues + 1);
        if (_orderByExpression.isNullsLast()) {
          addRows(sortedValues, numValues, rows);
          rows.add(new Object[]{null});
        } else {
          rows.add(new Object[]{null});
          addRows(sortedValues, numValues, rows);
        }
      }
    } else {
      rows = new ArrayList<>(numValues);
      addRows(sortedValues, numValues, rows);
    }
    return new ResultTableRows(_dataSchema, rows);
  }

  private static void addRows(float[] values, int length, List<Object[]> rows) {
    for (int i = 0; i < length; i++) {
      rows.add(new Object[]{values[i]});
    }
  }

  private ResultTableRows toResultTableWithoutOrderBy() {
    int numValues = _valueSet.size();
    assert numValues <= _limit;
    List<Object[]> rows;
    if (_hasNull && numValues < _limit) {
      rows = new ArrayList<>(numValues + 1);
      addRows(_valueSet, rows);
      rows.add(new Object[]{null});
    } else {
      rows = new ArrayList<>(numValues);
      addRows(_valueSet, rows);
    }
    return new ResultTableRows(_dataSchema, rows);
  }

  private static void addRows(FloatOpenHashSet values, List<Object[]> rows) {
    FloatIterator floatIterator = values.iterator();
    while (floatIterator.hasNext()) {
      rows.add(new Object[]{floatIterator.nextFloat()});
    }
  }
}
