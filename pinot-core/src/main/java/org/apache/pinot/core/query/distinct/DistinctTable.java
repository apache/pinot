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
package org.apache.pinot.core.query.distinct;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code DistinctTable} stores the distinct records for the distinct queries.
 * <p>There are 2 types of DistinctTables:
 * <ul>
 *   <li>
 *     Main DistinctTable: Constructed with DataSchema, order-by information and limit, which can be used to add records
 *     or merge other DistinctTables.
 *   </li>
 *   <li>
 *     Wrapper DistinctTable: Constructed with DataSchema and a collection of records, and has no data structure to
 *     handle the addition of new records. It cannot be used to add more records or merge other DistinctTables, but can
 *     only be used to be merged into the main DistinctTable.
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctTable {
  // Available in both main and wrapper DistinctTable
  private final DataSchema _dataSchema;
  private final Collection<Record> _records;
  private final boolean _isMainTable;

  // Available in main DistinctTable only
  private final int _limit;
  private final boolean _nullHandlingEnabled;
  private final ObjectSet<Record> _recordSet;
  private final PriorityQueue<Record> _priorityQueue;

  /**
   * Constructor of the main DistinctTable which can be used to add records and merge other DistinctTables.
   */
  public DistinctTable(DataSchema dataSchema, @Nullable List<OrderByExpressionContext> orderByExpressions, int limit,
      NullMode nullMode) {
    _dataSchema = dataSchema;
    _isMainTable = true;
    _limit = limit;
    _nullHandlingEnabled = nullMode.nullAtQueryTime();

    // NOTE: When LIMIT is smaller than or equal to the MAX_INITIAL_CAPACITY, no resize is required.
    int initialCapacity = Math.min(limit, DistinctExecutor.MAX_INITIAL_CAPACITY);
    _recordSet = new ObjectOpenHashSet<>(initialCapacity);
    _records = _recordSet;

    if (orderByExpressions != null) {
      List<String> columnNames = Arrays.asList(dataSchema.getColumnNames());
      int numOrderByExpressions = orderByExpressions.size();
      int[] orderByExpressionIndices = new int[numOrderByExpressions];
      int[] comparisonFactors = new int[numOrderByExpressions];
      int[] nullComparisonFactors = new int[numOrderByExpressions];
      for (int i = 0; i < numOrderByExpressions; i++) {
        OrderByExpressionContext orderByExpression = orderByExpressions.get(i);
        orderByExpressionIndices[i] = columnNames.indexOf(orderByExpression.getExpression().toString());
        comparisonFactors[i] = orderByExpression.isAsc() ? -1 : 1;
        nullComparisonFactors[i] = orderByExpression.isNullsLast() ? -1 : 1;
      }
      if (_nullHandlingEnabled) {
        _priorityQueue = new ObjectHeapPriorityQueue<>(initialCapacity, (r1, r2) -> {
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
        });
      } else {
        _priorityQueue = new ObjectHeapPriorityQueue<>(initialCapacity, (r1, r2) -> {
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
        });
      }
    } else {
      _priorityQueue = null;
    }
  }

  /**
   * Constructor of the wrapper DistinctTable which can only be merged into the main DistinctTable.
   */
  public DistinctTable(DataSchema dataSchema, Collection<Record> records, NullMode nullMode) {
    _dataSchema = dataSchema;
    _records = records;
    _nullHandlingEnabled = nullMode.nullAtQueryTime();
    _isMainTable = false;
    _limit = Integer.MIN_VALUE;
    _recordSet = null;
    _priorityQueue = null;
  }

  /**
   * Constructor of the wrapper DistinctTable which can only be merged into the main DistinctTable.
   */
  public DistinctTable(DataSchema dataSchema, Collection<Record> records) {
    this(dataSchema, records, NullMode.NONE_NULLABLE);
  }

  /**
   * Returns the {@link DataSchema} of the DistinctTable.
   */
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /**
   * Returns {@code true} for main DistinctTable, {@code false} for wrapper DistinctTable.
   */
  public boolean isMainTable() {
    return _isMainTable;
  }

  /**
   * Returns the number of unique records within the DistinctTable.
   */
  public int size() {
    return _records.size();
  }

  /**
   * Returns true if the DistinctTable is empty.
   */
  public boolean isEmpty() {
    return _records.isEmpty();
  }

  @VisibleForTesting
  public Collection<Record> getRecords() {
    return _records;
  }

  /**
   * Returns {@code true} if the main DistinctTable has order-by, {@code false} otherwise.
   */
  public boolean hasOrderBy() {
    assert _isMainTable;
    return _priorityQueue != null;
  }

  /**
   * Adds a record to the main DistinctTable without order-by and returns {@code true} if the DistinctTable is already
   * satisfied, {@code false} otherwise.
   * <p>NOTE: There should be no more calls to this method after it returns {@code true}.
   */
  public boolean addWithoutOrderBy(Record record) {
    assert _isMainTable && _priorityQueue == null;
    _recordSet.add(record);
    return _recordSet.size() >= _limit;
  }

  /**
   * Adds a record to the main DistinctTable with order-by.
   */
  public void addWithOrderBy(Record record) {
    assert _isMainTable && _priorityQueue != null;
    if (!_recordSet.contains(record)) {
      if (_priorityQueue.size() < _limit) {
        _recordSet.add(record);
        _priorityQueue.enqueue(record);
      } else {
        Record firstRecord = _priorityQueue.first();
        if (_priorityQueue.comparator().compare(record, firstRecord) > 0) {
          _recordSet.remove(firstRecord);
          _recordSet.add(record);
          _priorityQueue.dequeue();
          _priorityQueue.enqueue(record);
        }
      }
    }
  }

  /**
   * Merges another DistinctTable into the main DistinctTable.
   */
  public void mergeTable(DistinctTable distinctTable) {
    assert _isMainTable;
    int mergedRecords = 0;
    if (hasOrderBy()) {
      for (Record record : distinctTable._records) {
        addWithOrderBy(record);
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedRecords);
        mergedRecords++;
      }
    } else {
      if (_recordSet.size() < _limit) {
        for (Record record : distinctTable._records) {
          if (addWithoutOrderBy(record)) {
            return;
          }
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedRecords);
          mergedRecords++;
        }
      }
    }
  }

  /**
   * Returns the final result (all unique records, sorted if ordering is required) from the main DistinctTable.
   */
  public Iterator<Record> getFinalResult() {
    assert _isMainTable;
    if (_priorityQueue != null) {
      int numRecords = _priorityQueue.size();
      Record[] sortedRecords = new Record[numRecords];
      for (int i = numRecords - 1; i >= 0; i--) {
        sortedRecords[i] = _priorityQueue.dequeue();
      }
      return Arrays.asList(sortedRecords).iterator();
    } else {
      return _recordSet.iterator();
    }
  }

  /**
   * Serializes the DistinctTable into a byte array.
   */
  public byte[] toBytes()
      throws IOException {
    // NOTE: Serialize the DistinctTable as a DataTable
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(_dataSchema);
    ColumnDataType[] storedColumnDataTypes = _dataSchema.getStoredColumnDataTypes();
    int numColumns = storedColumnDataTypes.length;
    RoaringBitmap[] nullBitmaps = null;
    if (_nullHandlingEnabled) {
      nullBitmaps = new RoaringBitmap[numColumns];
      Object[] nullPlaceholders = new Object[numColumns];
      for (int colId = 0; colId < numColumns; colId++) {
        nullPlaceholders[colId] = storedColumnDataTypes[colId].getNullPlaceholder();
        nullBitmaps[colId] = new RoaringBitmap();
      }

      int rowId = 0;
      for (Record record : _records) {
        Object[] values = record.getValues();
        for (int colId = 0; colId < numColumns; colId++) {
          if (values[colId] == null) {
            values[colId] = nullPlaceholders[colId];
            nullBitmaps[colId].add(rowId);
          }
        }
        rowId++;
      }
    }

    for (Record record : _records) {
      dataTableBuilder.startRow();
      Object[] values = record.getValues();
      for (int i = 0; i < numColumns; i++) {
        switch (storedColumnDataTypes[i]) {
          case INT:
            dataTableBuilder.setColumn(i, (int) values[i]);
            break;
          case LONG:
            dataTableBuilder.setColumn(i, (long) values[i]);
            break;
          case FLOAT:
            dataTableBuilder.setColumn(i, (float) values[i]);
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(i, (double) values[i]);
            break;
          case BIG_DECIMAL:
            dataTableBuilder.setColumn(i, (BigDecimal) values[i]);
            break;
          case STRING:
            dataTableBuilder.setColumn(i, (String) values[i]);
            break;
          case BYTES:
            dataTableBuilder.setColumn(i, (ByteArray) values[i]);
            break;
          // Add other distinct column type supports here
          default:
            throw new IllegalStateException();
        }
      }
      dataTableBuilder.finishRow();
    }
    if (_nullHandlingEnabled) {
      for (int colId = 0; colId < numColumns; colId++) {
        dataTableBuilder.setNullRowIds(nullBitmaps[colId]);
      }
    }
    return dataTableBuilder.build().toBytes();
  }

  /**
   * Deserializes the DistinctTable from a {@link ByteBuffer}. The DistinctTable constructed this way is a wrapper
   * DistinctTable and cannot be used to add more records or merge other DistinctTables.
   */
  public static DistinctTable fromByteBuffer(ByteBuffer byteBuffer)
      throws IOException {
    DataTable dataTable = DataTableFactory.getDataTable(byteBuffer);
    DataSchema dataSchema = dataTable.getDataSchema();
    int numRecords = dataTable.getNumberOfRows();
    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int numColumns = storedColumnDataTypes.length;
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    boolean nullHandlingEnabled = false;
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = dataTable.getNullRowIds(colId);
      nullHandlingEnabled |= nullBitmaps[colId] != null;
    }
    List<Record> records = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      Object[] values = new Object[numColumns];
      for (int j = 0; j < numColumns; j++) {
        switch (storedColumnDataTypes[j]) {
          case INT:
            values[j] = dataTable.getInt(i, j);
            break;
          case LONG:
            values[j] = dataTable.getLong(i, j);
            break;
          case FLOAT:
            values[j] = dataTable.getFloat(i, j);
            break;
          case DOUBLE:
            values[j] = dataTable.getDouble(i, j);
            break;
          case BIG_DECIMAL:
            values[j] = dataTable.getBigDecimal(i, j);
            break;
          case STRING:
            values[j] = dataTable.getString(i, j);
            break;
          case BYTES:
            values[j] = dataTable.getBytes(i, j);
            break;
          // Add other distinct column type supports here
          default:
            throw new IllegalStateException();
        }
      }
      records.add(new Record(values));
    }

    if (nullHandlingEnabled) {
      for (int i = 0; i < records.size(); i++) {
        Object[] values = records.get(i).getValues();
        for (int j = 0; j < numColumns; j++) {
          if (nullBitmaps[j] != null && nullBitmaps[j].contains(i)) {
            values[j] = null;
          }
        }
      }
    }
    return new DistinctTable(dataSchema, records);
  }
}
