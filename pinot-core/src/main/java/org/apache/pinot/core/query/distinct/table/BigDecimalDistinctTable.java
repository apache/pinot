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

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.RoaringBitmap;


public class BigDecimalDistinctTable extends DistinctTable {
  private final HashSet<BigDecimal> _valueSet;
  private final OrderByExpressionContext _orderByExpression;

  private ObjectHeapPriorityQueue<BigDecimal> _priorityQueue;

  /**
   * Constructor for distinct table without data table (on the server side).
   */
  public BigDecimalDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression) {
    super(dataSchema, limit, nullHandlingEnabled);

    _valueSet = Sets.newHashSetWithExpectedSize(Math.min(limit, MAX_INITIAL_CAPACITY));
    _orderByExpression = orderByExpression;
  }

  /**
   * Constructor for distinct table with data table (on the broker side).
   */
  public BigDecimalDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable OrderByExpressionContext orderByExpression, DataTable dataTable) {
    super(dataSchema, limit, nullHandlingEnabled);

    int numRows = dataTable.getNumberOfRows();
    _valueSet = Sets.newHashSetWithExpectedSize(numRows);
    _orderByExpression = orderByExpression;

    RoaringBitmap nullRowIds = nullHandlingEnabled ? dataTable.getNullRowIds(0) : null;
    if (nullRowIds == null) {
      for (int i = 0; i < numRows; i++) {
        _valueSet.add(dataTable.getBigDecimal(i, 0));
      }
    } else {
      assert nullRowIds.getCardinality() == 1;
      addNull();
      int nullRowId = nullRowIds.first();
      if (nullRowId == 0) {
        for (int i = 1; i < numRows; i++) {
          _valueSet.add(dataTable.getBigDecimal(i, 0));
        }
      } else {
        // For backward compatibility where null value is not stored as the first row
        for (int i = 0; i < nullRowId; i++) {
          _valueSet.add(dataTable.getBigDecimal(i, 0));
        }
        for (int i = nullRowId + 1; i < numRows; i++) {
          _valueSet.add(dataTable.getBigDecimal(i, 0));
        }
      }
    }
    assert _valueSet.size() <= limit;
  }

  @Override
  public boolean hasOrderBy() {
    return _orderByExpression != null;
  }

  public boolean addWithoutOrderBy(BigDecimal value) {
    assert _valueSet.size() < _limit;
    _valueSet.add(value);
    return _valueSet.size() >= _limitWithoutNull;
  }

  public void addWithOrderBy(BigDecimal value) {
    assert _valueSet.size() <= _limit;
    if (_valueSet.size() < _limit) {
      _valueSet.add(value);
      return;
    }
    if (_valueSet.contains(value)) {
      return;
    }
    if (_priorityQueue == null) {
      Comparator<BigDecimal> comparator =
          _orderByExpression.isAsc() ? Comparator.reverseOrder() : Comparator.naturalOrder();
      _priorityQueue = new ObjectHeapPriorityQueue<>(_valueSet, comparator);
    }
    BigDecimal firstValue = _priorityQueue.first();
    if (_priorityQueue.comparator().compare(value, firstValue) > 0) {
      _valueSet.remove(firstValue);
      _valueSet.add(value);
      _priorityQueue.dequeue();
      _priorityQueue.enqueue(value);
    }
  }

  public void addUnbounded(BigDecimal value) {
    _valueSet.add(value);
  }

  @Override
  public void mergeDistinctTable(DistinctTable distinctTable) {
    BigDecimalDistinctTable bigDecimalDistinctTable = (BigDecimalDistinctTable) distinctTable;
    if (bigDecimalDistinctTable._hasNull) {
      addNull();
    }
    if (hasLimit()) {
      if (hasOrderBy()) {
        for (BigDecimal value : bigDecimalDistinctTable._valueSet) {
          addWithOrderBy(value);
        }
      } else {
        for (BigDecimal value : bigDecimalDistinctTable._valueSet) {
          if (addWithoutOrderBy(value)) {
            return;
          }
        }
      }
    } else {
      // NOTE: Do not use _valueSet.addAll() to avoid unnecessary resize when most values are common.
      for (BigDecimal value : bigDecimalDistinctTable._valueSet) {
        addUnbounded(value);
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
          addWithOrderBy(dataTable.getBigDecimal(i, 0));
        }
      } else {
        for (int i = from; i < to; i++) {
          if (addWithoutOrderBy(dataTable.getBigDecimal(i, 0))) {
            return true;
          }
        }
      }
    } else {
      for (int i = from; i < to; i++) {
        addUnbounded(dataTable.getBigDecimal(i, 0));
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
    for (BigDecimal value : _valueSet) {
      rows.add(new Object[]{value});
    }
    return rows;
  }

  @Override
  public DataTable toDataTable()
      throws IOException {
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(_dataSchema);
    if (_hasNull) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, CommonConstants.NullValuePlaceHolder.BIG_DECIMAL);
      dataTableBuilder.finishRow();
    }
    int numRowsAdded = 0;
    for (BigDecimal value : _valueSet) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(numRowsAdded);
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, value);
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
    BigDecimal[] sortedValues;
    if (_priorityQueue != null) {
      int numValues = _priorityQueue.size();
      sortedValues = new BigDecimal[numValues];
      for (int i = numValues - 1; i >= 0; i--) {
        sortedValues[i] = _priorityQueue.dequeue();
      }
    } else {
      sortedValues = _valueSet.toArray(new BigDecimal[0]);
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
    return new ResultTable(_dataSchema, rows);
  }

  private static void addRows(BigDecimal[] values, int length, List<Object[]> rows) {
    for (int i = 0; i < length; i++) {
      rows.add(new Object[]{values[i].toPlainString()});
    }
  }

  private ResultTable toResultTableWithoutOrderBy() {
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
    return new ResultTable(_dataSchema, rows);
  }

  private static void addRows(HashSet<BigDecimal> values, List<Object[]> rows) {
    for (BigDecimal value : values) {
      rows.add(new Object[]{value.toPlainString()});
    }
  }
}
