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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.roaringbitmap.RoaringBitmap;


public class MultiColumnDistinctTable extends DistinctTable {
  private final HashSet<Record> _recordSet;
  private final List<OrderByExpressionContext> _orderByExpressions;

  private ObjectHeapPriorityQueue<Record> _priorityQueue;

  /**
   * Constructor for distinct table without data table (on the server side).
   */
  public MultiColumnDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable List<OrderByExpressionContext> orderByExpressions) {
    this(dataSchema, limit, nullHandlingEnabled, orderByExpressions, Math.min(limit, MAX_INITIAL_CAPACITY));
  }

  /**
   * Constructor for distinct table with initial set size (on the server side).
   */
  public MultiColumnDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable List<OrderByExpressionContext> orderByExpressions, int initialSetSize) {
    super(dataSchema, limit, nullHandlingEnabled);

    _recordSet = Sets.newHashSetWithExpectedSize(initialSetSize);
    _orderByExpressions = orderByExpressions;
  }

  /**
   * Constructor for distinct table with data table (on the broker side).
   */
  public MultiColumnDistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable List<OrderByExpressionContext> orderByExpressions, DataTable dataTable) {
    super(dataSchema, limit, nullHandlingEnabled);

    int numRows = dataTable.getNumberOfRows();
    _recordSet = Sets.newHashSetWithExpectedSize(numRows);
    _orderByExpressions = orderByExpressions;

    int numColumns = dataSchema.size();
    if (nullHandlingEnabled) {
      RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
      for (int coldId = 0; coldId < numColumns; coldId++) {
        nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
      }
      for (int i = 0; i < numRows; i++) {
        _recordSet.add(
            new Record(SelectionOperatorUtils.extractRowFromDataTableWithNullHandling(dataTable, i, nullBitmaps)));
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        _recordSet.add(new Record(SelectionOperatorUtils.extractRowFromDataTable(dataTable, i)));
      }
    }
    assert _recordSet.size() <= limit;
  }

  @Override
  public void addNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasOrderBy() {
    return _orderByExpressions != null;
  }

  public boolean addWithoutOrderBy(Record record) {
    assert _recordSet.size() < _limit;
    _recordSet.add(record);
    return _recordSet.size() == _limit;
  }

  public void addWithOrderBy(Record record) {
    assert _recordSet.size() <= _limit;
    if (_recordSet.size() < _limit) {
      _recordSet.add(record);
      return;
    }
    if (_recordSet.contains(record)) {
      return;
    }
    if (_priorityQueue == null) {
      _priorityQueue = new ObjectHeapPriorityQueue<>(_recordSet, getComparator());
    }
    Record firstRecord = _priorityQueue.first();
    if (_priorityQueue.comparator().compare(record, firstRecord) > 0) {
      _recordSet.remove(firstRecord);
      _recordSet.add(record);
      _priorityQueue.dequeue();
      _priorityQueue.enqueue(record);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Comparator<Record> getComparator() {
    List<String> columnNames = Arrays.asList(_dataSchema.getColumnNames());
    int numOrderByExpressions = _orderByExpressions.size();
    int[] orderByExpressionIndices = new int[numOrderByExpressions];
    int[] comparisonFactors = new int[numOrderByExpressions];
    int[] nullComparisonFactors = new int[numOrderByExpressions];
    for (int i = 0; i < numOrderByExpressions; i++) {
      OrderByExpressionContext orderByExpression = _orderByExpressions.get(i);
      orderByExpressionIndices[i] = columnNames.indexOf(orderByExpression.getExpression().toString());
      comparisonFactors[i] = orderByExpression.isAsc() ? -1 : 1;
      nullComparisonFactors[i] = orderByExpression.isNullsLast() ? -1 : 1;
    }
    if (_nullHandlingEnabled) {
      return (r1, r2) -> {
        Object[] values1 = r1.getValues();
        Object[] values2 = r2.getValues();
        for (int i = 0; i < numOrderByExpressions; i++) {
          int index = orderByExpressionIndices[i];
          Comparable value1 = (Comparable) values1[index];
          Comparable value2 = (Comparable) values2[index];
          if (value1 == null) {
            if (value2 == null) {
              continue;
            }
            return nullComparisonFactors[i];
          } else if (value2 == null) {
            return -nullComparisonFactors[i];
          }
          int result = value1.compareTo(value2) * comparisonFactors[i];
          if (result != 0) {
            return result;
          }
        }
        return 0;
      };
    } else {
      return (r1, r2) -> {
        Object[] values1 = r1.getValues();
        Object[] values2 = r2.getValues();
        for (int i = 0; i < numOrderByExpressions; i++) {
          int index = orderByExpressionIndices[i];
          Comparable value1 = (Comparable) values1[index];
          Comparable value2 = (Comparable) values2[index];
          int result = value1.compareTo(value2) * comparisonFactors[i];
          if (result != 0) {
            return result;
          }
        }
        return 0;
      };
    }
  }

  public void addUnbounded(Record record) {
    _recordSet.add(record);
  }

  @Override
  public void mergeDistinctTable(DistinctTable distinctTable) {
    MultiColumnDistinctTable multiColumnDistinctTable = (MultiColumnDistinctTable) distinctTable;
    if (hasLimit()) {
      if (hasOrderBy()) {
        for (Record record : multiColumnDistinctTable._recordSet) {
          addWithOrderBy(record);
        }
      } else {
        for (Record record : multiColumnDistinctTable._recordSet) {
          if (addWithoutOrderBy(record)) {
            return;
          }
        }
      }
    } else {
      // NOTE: Do not use _valueSet.addAll() to avoid unnecessary resize when most values are common.
      for (Record record : multiColumnDistinctTable._recordSet) {
        addUnbounded(record);
      }
    }
  }

  @Override
  public boolean mergeDataTable(DataTable dataTable) {
    int numRows = dataTable.getNumberOfRows();
    int numColumns = _dataSchema.size();
    if (_nullHandlingEnabled) {
      RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
      for (int coldId = 0; coldId < numColumns; coldId++) {
        nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
      }
      return addRecords(numRows,
          i -> new Record(SelectionOperatorUtils.extractRowFromDataTableWithNullHandling(dataTable, i, nullBitmaps)));
    } else {
      return addRecords(numRows, i -> new Record(SelectionOperatorUtils.extractRowFromDataTable(dataTable, i)));
    }
  }

  private boolean addRecords(int numRows, IntFunction<Record> recordSupplier) {
    if (hasLimit()) {
      if (hasOrderBy()) {
        for (int i = 0; i < numRows; i++) {
          addWithOrderBy(recordSupplier.apply(i));
        }
      } else {
        for (int i = 0; i < numRows; i++) {
          if (addWithoutOrderBy(recordSupplier.apply(i))) {
            return true;
          }
        }
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        addUnbounded(recordSupplier.apply(i));
      }
    }
    return false;
  }

  @Override
  public int size() {
    return _recordSet.size();
  }

  @Override
  public boolean isSatisfied() {
    return _orderByExpressions == null && _recordSet.size() == _limit;
  }

  @Override
  public List<Object[]> getRows() {
    List<Object[]> rows = new ArrayList<>(_recordSet.size());
    for (Record record : _recordSet) {
      rows.add(record.getValues());
    }
    return rows;
  }

  @Override
  public DataTable toDataTable()
      throws IOException {
    return SelectionOperatorUtils.getDataTableFromRows(getRows(), _dataSchema, _nullHandlingEnabled);
  }

  @Override
  public ResultTableRows toResultTable() {
    return hasOrderBy() ? toResultTableWithOrderBy() : toResultTableWithoutOrderBy();
  }

  private ResultTableRows toResultTableWithOrderBy() {
    Record[] sortedRecords;
    if (_priorityQueue != null) {
      int numRecords = _priorityQueue.size();
      sortedRecords = new Record[numRecords];
      for (int i = numRecords - 1; i >= 0; i--) {
        sortedRecords[i] = _priorityQueue.dequeue();
      }
    } else {
      sortedRecords = _recordSet.toArray(new Record[0]);
      Arrays.sort(sortedRecords, getComparator().reversed());
    }
    return createResultTable(Arrays.asList(sortedRecords));
  }

  private ResultTableRows toResultTableWithoutOrderBy() {
    return createResultTable(_recordSet);
  }

  private ResultTableRows createResultTable(Collection<Record> records) {
    int numRecords = records.size();
    assert numRecords <= _limit;
    List<Object[]> rows = new ArrayList<>(numRecords);
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    for (Record record : records) {
      Object[] values = record.getValues();
      for (int i = 0; i < numColumns; i++) {
        Object value = values[i];
        if (value != null) {
          values[i] = columnDataTypes[i].convertAndFormat(value);
        }
      }
      rows.add(values);
    }
    return new ResultTableRows(_dataSchema, rows);
  }
}
