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
package org.apache.pinot.core.query.aggregation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.data.table.BaseTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * This serves the following purposes:
 *
 * (1) Intermediate result object for Distinct aggregation function
 * (2) The same object is serialized by the server inside the data table
 * for sending the results to broker. Broker deserializes it.
 * (3) This is also another concrete implementation of {@link BaseTable} and
 * uses {@link Set} to store unique records.
 */
public class DistinctTable extends BaseTable {
  private static final int MAX_INITIAL_CAPACITY = 64 * 1024;
  private Set<Record> _uniqueRecordsSet;
  private boolean _noMoreNewRecords;
  private Iterator<Record> _sortedIterator;

  public DistinctTable(DataSchema dataSchema, List<SelectionSort> orderBy, int capacity) {
    // TODO: see if 64k is the right max initial capacity to use
    // NOTE: The passed in capacity is calculated based on the LIMIT in the query as Math.max(limit * 5, 5000). When
    //       LIMIT is smaller than (64 * 1024 * 0.75 (load factor) / 5 = 9830), then it is guaranteed that no resize is
    //       required.
    super(dataSchema, Collections.emptyList(), orderBy, capacity);
    int initialCapacity = Math.min(MAX_INITIAL_CAPACITY, HashUtil.getHashMapCapacity(capacity));
    _uniqueRecordsSet = new HashSet<>(initialCapacity);
    _noMoreNewRecords = false;
  }

  @Override
  public boolean upsert(Key key, Record record) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public boolean upsert(Record newRecord) {
    if (_noMoreNewRecords) {
      // for no ORDER BY queries, if we have reached the N as specified
      // in LIMIT N (or default 10 if user didn't specify anything)
      // then this function is NOOP
      return false;
    }

    _uniqueRecordsSet.add(newRecord);

    if (_uniqueRecordsSet.size() >= _maxCapacity) {
      if (_isOrderBy) {
        // ORDER BY; capacity < maxCapacity so trim to capacity
        resize(_capacity);
      } else {
        // No ORDER BY; capacity == maxCapacity == user specified limit
        // we can simply stop accepting anymore records from now on
        _noMoreNewRecords = true;
      }
    }

    return true;
  }

  /**
   * SERIALIZE: Server side
   * @return serialized bytes
   * @throws IOException
   */
  public byte[] toBytes()
      throws IOException {
    // build rows for data table
    // Only after the server level merge is done by CombineOperator to merge all the indexed tables
    // of segments 1 .. N - 1 into the indexed table of 0th segment, we do finish(false) as that
    // time we need the iterator as well and we send a trimmed set of records to the broker.
    // finish is NOOP for non ORDER BY queries.
    finish(false);
    DataTableBuilder dataTableBuilder = new DataTableBuilder(_dataSchema);
    Iterator<Record> iterator = iterator();
    while (iterator.hasNext()) {
      dataTableBuilder.startRow();
      final Record record = iterator.next();
      serializeColumns(record.getValues(), _dataSchema.getColumnDataTypes(), dataTableBuilder);
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();
    return dataTable.toBytes();
  }

  private void serializeColumns(Object[] columns, DataSchema.ColumnDataType[] columnDataTypes,
      DataTableBuilder dataTableBuilder)
      throws IOException {
    for (int colIndex = 0; colIndex < columns.length; colIndex++) {
      switch (columnDataTypes[colIndex]) {
        case INT:
          dataTableBuilder.setColumn(colIndex, ((Number) columns[colIndex]).intValue());
          break;
        case LONG:
          dataTableBuilder.setColumn(colIndex, ((Number) columns[colIndex]).longValue());
          break;
        case FLOAT:
          dataTableBuilder.setColumn(colIndex, ((Number) columns[colIndex]).floatValue());
          break;
        case DOUBLE:
          dataTableBuilder.setColumn(colIndex, ((Number) columns[colIndex]).doubleValue());
          break;
        case STRING:
          dataTableBuilder.setColumn(colIndex, ((String) columns[colIndex]));
          break;
        case BYTES:
          dataTableBuilder.setColumn(colIndex, (ByteArray) columns[colIndex]);
          break;
        // Add other distinct column type supports here
        default:
          throw new IllegalStateException();
      }
    }
  }

  /**
   * DESERIALIZE: Broker side
   * @param byteBuffer data to deserialize
   * @throws IOException
   */
  public DistinctTable(ByteBuffer byteBuffer)
      throws IOException {
    // This is called by the BrokerReduceService when it de-serializes the
    // DISTINCT result from the DataTable. As of now we don't have all the
    // information to pass to super class so just pass null, empty lists
    // and the broker will set the correct information before merging the
    // data tables.
    super(new DataSchema(new String[0], new DataSchema.ColumnDataType[0]), Collections.emptyList(), new ArrayList<>(),
        0);
    DataTable dataTable = DataTableFactory.getDataTable(byteBuffer);
    _dataSchema = dataTable.getDataSchema();
    _uniqueRecordsSet = new HashSet<>();

    int numRows = dataTable.getNumberOfRows();
    int numColumns = _dataSchema.size();

    // extract rows from the datatable
    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
      Object[] columnValues = new Object[numColumns];
      for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        DataSchema.ColumnDataType columnDataType = _dataSchema.getColumnDataType(colIndex);
        switch (columnDataType) {
          case INT:
            columnValues[colIndex] = dataTable.getInt(rowIndex, colIndex);
            break;
          case LONG:
            columnValues[colIndex] = dataTable.getLong(rowIndex, colIndex);
            break;
          case FLOAT:
            columnValues[colIndex] = dataTable.getFloat(rowIndex, colIndex);
            break;
          case DOUBLE:
            columnValues[colIndex] = dataTable.getDouble(rowIndex, colIndex);
            break;
          case STRING:
            columnValues[colIndex] = dataTable.getString(rowIndex, colIndex);
            break;
          case BYTES:
            columnValues[colIndex] = dataTable.getBytes(rowIndex, colIndex);
            break;
          // Add other distinct column type supports here
          default:
            throw new IllegalStateException(
                "Unexpected column data type " + columnDataType + " while deserializing data table for DISTINCT query");
        }
      }

      Record record = new Record(columnValues);
      _uniqueRecordsSet.add(record);
    }
  }

  /**
   * Called by {@link org.apache.pinot.core.query.reduce.BrokerReduceService}
   * just before it attempts to invoke merge() on
   * {@link org.apache.pinot.core.query.aggregation.function.AggregationFunction}
   * for DISTINCT. Since the DISTINCT uses IndexedTable underneath as the main
   * data structure to hold unique tuples/rows and also for resizing/trimming/sorting,
   * we need to pass info on limit, order by to super class
   * {@link org.apache.pinot.core.data.table.IndexedTable}.
   * @param brokerRequest broker request
   */
  public void addLimitAndOrderByInfo(BrokerRequest brokerRequest) {
    addCapacityAndOrderByInfo(brokerRequest.getOrderBy(), brokerRequest.getLimit());
  }

  private void resize(int trimToSize) {
    _tableResizer.resizeRecordsSet(_uniqueRecordsSet, trimToSize);
  }

  @Override
  public int size() {
    return _uniqueRecordsSet.size();
  }

  @Override
  public Iterator<Record> iterator() {
    return _sortedIterator != null ? _sortedIterator : _uniqueRecordsSet.iterator();
  }

  @Override
  public void finish(boolean sort) {
    if (_isOrderBy) {
      if (sort) {
        List<Record> sortedRecords = _tableResizer.resizeAndSortRecordSet(_uniqueRecordsSet, _capacity);
        _sortedIterator = sortedRecords.iterator();
      } else {
        resize(_capacity);
      }
    }
  }
}
