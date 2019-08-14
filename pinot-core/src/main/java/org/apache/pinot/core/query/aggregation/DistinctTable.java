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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.data.table.Key;


/**
 * This serves the following purposes:
 *
 * (1) Intermediate result object for Distinct aggregation function
 * (2) The same object is serialized by the server inside the data table
 * for sending the results to broker. Broker deserializes it.
 */
public class DistinctTable {
  private static final double LOAD_FACTOR = 0.75;
  private static final int MAX_INITIAL_CAPACITY = 64 * 1024;
  private FieldSpec.DataType[] _columnTypes;
  private String[] _columnNames;
  private Set<Key> _table;

  /**
   * Add a row to hash table
   * @param key multi-column key to add
   */
  public void addKey(final Key key) {
    _table.add(key);
  }

  public DistinctTable(int limit) {
    // TODO: see if 64k is the right max initial capacity to use
    // if it turns out that users always use LIMIT N > 0.75 * 64k and
    // there are indeed that many records, then there will be resizes.
    // The current method of setting the initial capacity as
    // min(64k, limit/loadFactor) will not require resizes for LIMIT N
    // where N <= 48000
    int initialCapacity = Math.min(MAX_INITIAL_CAPACITY, Math.abs(nextPowerOfTwo((int) (limit / LOAD_FACTOR))));
    _table = new HashSet<>(initialCapacity);
  }

  /**
   * DESERIALIZE: Broker side
   * @param byteBuffer data to deserialize
   * @throws IOException
   */
  public DistinctTable(ByteBuffer byteBuffer)
      throws IOException {
    DataTable dataTable = DataTableFactory.getDataTable(byteBuffer);
    DataSchema dataSchema = dataTable.getDataSchema();
    int numRows = dataTable.getNumberOfRows();
    int numColumns = dataSchema.size();

    _table = new HashSet<>();

    // extract rows from the datatable
    for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
      Object[] columnValues = new Object[numColumns];
      for (int colIndex = 0; colIndex < numColumns; colIndex++) {
        DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(colIndex);
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
            columnValues[colIndex] = dataTable.getString(rowIndex, colIndex);
          default:
            throw new IllegalStateException(
                "Unexpected column data type " + columnDataType + " while deserializing data table for DISTINCT query");
        }
      }

      _table.add(new Key(columnValues));
    }

    _columnNames = dataSchema.getColumnNames();

    // note: when deserializing at broker, we don't need to build
    // FieldSpec.DataType since the work is already done.
    // we have the column names and unique rows in set and that
    // is all what we need to set the broker response. the deserialized
    // column data types from schema are enough to work with each cell
  }

  /**
   * SERIALIZE: Server side
   * @return serialized bytes
   * @throws IOException
   */
  public byte[] toBytes()
      throws IOException {
    final String[] columnNames = new String[_columnNames.length];
    final DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_columnNames.length];

    // set actual column names and column data types in data schema
    for (int i = 0; i < _columnNames.length; i++) {
      columnNames[i] = _columnNames[i];
      switch (_columnTypes[i]) {
        case INT:
          columnDataTypes[i] = DataSchema.ColumnDataType.INT;
          break;
        case LONG:
          columnDataTypes[i] = DataSchema.ColumnDataType.LONG;
          break;
        case FLOAT:
          columnDataTypes[i] = DataSchema.ColumnDataType.FLOAT;
          break;
        case DOUBLE:
          columnDataTypes[i] = DataSchema.ColumnDataType.DOUBLE;
          break;
        case STRING:
          columnDataTypes[i] = DataSchema.ColumnDataType.STRING;
          break;
        case BYTES:
          columnDataTypes[i] = DataSchema.ColumnDataType.BYTES;
        default:
          throw new IllegalStateException(
              "Unexpected column data type " + _columnTypes[i] + " while serializing data table for DISTINCT query");
      }
    }

    // build rows for data table
    DataTableBuilder dataTableBuilder = new DataTableBuilder(new DataSchema(columnNames, columnDataTypes));

    final Iterator<Key> iterator = _table.iterator();
    while (iterator.hasNext()) {
      dataTableBuilder.startRow();
      final Key key = iterator.next();
      serializeColumns(key.getColumns(), columnDataTypes, dataTableBuilder);
      dataTableBuilder.finishRow();
    }

    final DataTable dataTable = dataTableBuilder.build();
    return dataTable.toBytes();
  }

  private void serializeColumns(final Object[] columns, final DataSchema.ColumnDataType[] columnDataTypes,
      final DataTableBuilder dataTableBuilder) {
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
          dataTableBuilder.setColumn(colIndex, BytesUtils.toHexString((byte[]) columns[colIndex]));
      }
    }
  }

  public int size() {
    return _table.size();
  }

  public Iterator<Key> getIterator() {
    return _table.iterator();
  }

  public void setColumnNames(String[] columnNames) {
    _columnNames = columnNames;
  }

  public void setColumnTypes(FieldSpec.DataType[] columnTypes) {
    _columnTypes = columnTypes;
  }

  public String[] getColumnNames() {
    return _columnNames;
  }

  public FieldSpec.DataType[] getColumnTypes() {
    return _columnTypes;
  }

  private static int nextPowerOfTwo(int val) {
    if (val == 0 || val == 1) {
      return val + 1;
    }
    int highestBit = Integer.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }
}
