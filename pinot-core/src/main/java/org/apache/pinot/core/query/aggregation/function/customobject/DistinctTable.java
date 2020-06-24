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
package org.apache.pinot.core.query.aggregation.function.customobject;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * The {@code DistinctTable} class serves as the intermediate result of
 * {@link org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction}.
 * <p>There are 2 types of DistinctTables:
 * <ul>
 *   <li>
 *     Main DistinctTable: Constructed with DataSchema, order-by information and limit, which can be used to add records
 *     or merge other DistinctTables.
 *   </li>
 *   <li>
 *     Deserialized DistinctTable (Broker-side only): Constructed with ByteBuffer, which only contains the DataSchema
 *     and records from the original main DistinctTable, but no data structure to handle the addition of new records. It
 *     cannot be used to add more records or merge other DistinctTables, but can only be used to be merged into the main
 *     DistinctTable.
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctTable {
  private static final int MAX_INITIAL_CAPACITY = 10000;

  // Available in both main and Broker-side de-serialized DistinctTable
  private final DataSchema _dataSchema;

  // Available in main DistinctTable only
  private final int _limit;
  private final Set<Record> _uniqueRecords;
  private final PriorityQueue<Record> _sortedRecords;

  // Available in Broker-side deserialized DistinctTable only
  private final List<Record> _records;

  /**
   * Constructor of the main DistinctTable which can be used to add records and merge other DistinctTables.
   */
  public DistinctTable(DataSchema dataSchema, @Nullable List<OrderByExpressionContext> orderByExpressions, int limit) {
    _dataSchema = dataSchema;
    _limit = limit;

    // TODO: see if 10k is the right max initial capacity to use
    // NOTE: When LIMIT is smaller than or equal to the MAX_INITIAL_CAPACITY, no resize is required.
    int initialCapacity = Math.min(limit, MAX_INITIAL_CAPACITY);
    _uniqueRecords = new ObjectOpenHashSet<>(initialCapacity);
    if (orderByExpressions != null) {
      String[] columns = dataSchema.getColumnNames();
      int numColumns = columns.length;
      Object2IntOpenHashMap<String> columnIndexMap = new Object2IntOpenHashMap<>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        columnIndexMap.put(columns[i], i);
      }
      int numOrderByColumns = orderByExpressions.size();
      int[] orderByColumnIndexes = new int[numOrderByColumns];
      boolean[] orderByAsc = new boolean[numOrderByColumns];
      for (int i = 0; i < numOrderByColumns; i++) {
        OrderByExpressionContext orderByExpression = orderByExpressions.get(i);
        orderByColumnIndexes[i] = columnIndexMap.getInt(orderByExpression.getExpression().toString());
        orderByAsc[i] = orderByExpression.isAsc();
      }
      _sortedRecords = new PriorityQueue<>(initialCapacity, (record1, record2) -> {
        Object[] values1 = record1.getValues();
        Object[] values2 = record2.getValues();
        for (int i = 0; i < numOrderByColumns; i++) {
          Comparable valueToCompare1 = (Comparable) values1[orderByColumnIndexes[i]];
          Comparable valueToCompare2 = (Comparable) values2[orderByColumnIndexes[i]];
          int result =
              orderByAsc[i] ? valueToCompare2.compareTo(valueToCompare1) : valueToCompare1.compareTo(valueToCompare2);
          if (result != 0) {
            return result;
          }
        }
        return 0;
      });
    } else {
      _sortedRecords = null;
    }
    _records = null;
  }

  /**
   * Returns the {@link DataSchema} of the DistinctTable.
   */
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /**
   * Returns the number of unique records within the DistinctTable.
   */
  public int size() {
    if (_uniqueRecords != null) {
      // Main DistinctTable
      return _uniqueRecords.size();
    } else {
      // Deserialized DistinctTable
      return _records.size();
    }
  }

  /**
   * Returns {@code true} if the main DistinctTable has order-by, {@code false} otherwise.
   */
  public boolean hasOrderBy() {
    return _sortedRecords != null;
  }

  /**
   * Adds a record to the main DistinctTable without order-by and returns whether the DistinctTable is already
   * satisfied.
   * <p>NOTE: There should be no more calls to this method after it or {@link #isSatisfied()} returns {@code true}.
   */
  public boolean addWithoutOrderBy(Record record) {
    if (_uniqueRecords.add(record)) {
      return isSatisfied();
    } else {
      return false;
    }
  }

  /**
   * Returns {@code true} if the main DistinctTable without order-by is already satisfied, {@code false} otherwise.
   * <p>The DistinctTable is satisfied when enough unique records have been collected.
   */
  public boolean isSatisfied() {
    return _uniqueRecords.size() == _limit;
  }

  /**
   * Adds a record to the main DistinctTable with order-by.
   */
  public void addWithOrderBy(Record record) {
    if (!_uniqueRecords.contains(record)) {
      if (_sortedRecords.size() < _limit) {
        _uniqueRecords.add(record);
        _sortedRecords.offer(record);
      } else {
        Record leastRecord = _sortedRecords.peek();
        if (_sortedRecords.comparator().compare(record, leastRecord) > 0) {
          _uniqueRecords.remove(leastRecord);
          _uniqueRecords.add(record);
          _sortedRecords.poll();
          _sortedRecords.offer(record);
        }
      }
    }
  }

  /*
   * SERVER ONLY METHODS
   */

  /**
   * Merges another main DistinctTable into the main DistinctTable.
   */
  public void mergeMainDistinctTable(DistinctTable distinctTable) {
    mergeRecords(distinctTable._uniqueRecords);
  }

  /**
   * Helper method to merge a collection of records into the main DistinctTable.
   */
  private void mergeRecords(Collection<Record> records) {
    if (hasOrderBy()) {
      for (Record record : records) {
        addWithOrderBy(record);
      }
    } else {
      if (isSatisfied()) {
        return;
      }
      for (Record record : records) {
        if (addWithoutOrderBy(record)) {
          return;
        }
      }
    }
  }

  /**
   * Serializes the main DistinctTable into a byte array.
   */
  public byte[] toBytes()
      throws IOException {
    // NOTE: Serialize the DistinctTable as a DataTable
    DataTableBuilder dataTableBuilder = new DataTableBuilder(_dataSchema);
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    for (Record record : _uniqueRecords) {
      dataTableBuilder.startRow();
      Object[] values = record.getValues();
      for (int i = 0; i < numColumns; i++) {
        switch (columnDataTypes[i]) {
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
    return dataTableBuilder.build().toBytes();
  }

  /*
   * BROKER ONLY METHODS
   */

  /**
   * Broker-side constructor to deserialize the DistinctTable from a {@link ByteBuffer}. The DistinctTable constructed
   * this way cannot be used to add more records or merge other DistinctTables, but can only be used to be merged into
   * the main DistinctTable because it does not contain the order-by information and limit.
   */
  public DistinctTable(ByteBuffer byteBuffer)
      throws IOException {
    DataTable dataTable = DataTableFactory.getDataTable(byteBuffer);
    _dataSchema = dataTable.getDataSchema();
    _limit = Integer.MIN_VALUE;
    _uniqueRecords = null;
    _sortedRecords = null;
    int numRecords = dataTable.getNumberOfRows();
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    _records = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      Object[] values = new Object[numColumns];
      for (int j = 0; j < numColumns; j++) {
        switch (columnDataTypes[j]) {
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
      _records.add(new Record(values));
    }
  }

  /**
   * Merges a deserialized DistinctTable into the main DistinctTable.
   */
  public void mergeDeserializedDistinctTable(DistinctTable distinctTable) {
    mergeRecords(distinctTable._records);
  }

  /**
   * Returns the final result (all unique records, sorted if ordering is required) from the main DistinctTable.
   */
  public Iterator<Record> getFinalResult() {
    if (_sortedRecords != null) {
      int numRecords = _sortedRecords.size();
      LinkedList<Record> sortedRecords = new LinkedList<>();
      for (int i = 0; i < numRecords; i++) {
        sortedRecords.addFirst(_sortedRecords.poll());
      }
      return sortedRecords.iterator();
    } else {
      return _uniqueRecords.iterator();
    }
  }
}
