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
package org.apache.pinot.core.data.manager.offline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class InMemoryTable {
  public InMemoryTable(DataTable dataTable) {
    _dataTable = dataTable;
    _dataSchema = dataTable.getDataSchema();
    String[] columnNames = _dataSchema.getColumnNames();
    _idxMap = new HashMap<>();
    for (int i = 0; i < columnNames.length; i++) {
      _idxMap.put(columnNames[i], i);
    }
  }

  public HashMap<PrimaryKey, Object[]> getHashMap(List<String> primaryKeyColumns) {
    List<Integer> primaryKeyIdx = new ArrayList<>();
    for (String key : primaryKeyColumns) {
      primaryKeyIdx.add(_idxMap.get(key));
    }
    int numRows = _dataTable.getNumberOfRows();
    int numCols = _dataSchema.getColumnNames().length;
    DataSchema.ColumnDataType[] storedColumnDataTypes = _dataSchema.getStoredColumnDataTypes();

    HashMap<PrimaryKey, Object[]> hashMap = new HashMap<>();
    for (int rowId = 0; rowId < numRows; rowId++) {
      Object[] keyValues = new Object[primaryKeyColumns.size()];
      for (int j = 0; j < primaryKeyIdx.size(); j++) {
        int colId = primaryKeyIdx.get(j);
        DataSchema.ColumnDataType storedType = storedColumnDataTypes[primaryKeyIdx.get(j)];
        switch (storedType) {
          case INT:
            keyValues[j] = _dataTable.getInt(rowId, colId);
            break;
          case LONG:
            keyValues[j] = _dataTable.getLong(rowId, colId);
            break;
          case FLOAT:
            keyValues[j] = _dataTable.getFloat(rowId, colId);
            break;
          case DOUBLE:
            keyValues[j] = _dataTable.getDouble(rowId, colId);
            break;
          case BIG_DECIMAL:
            keyValues[j] = _dataTable.getBigDecimal(rowId, colId);
            break;
          case STRING:
            keyValues[j] = _dataTable.getString(rowId, colId);
            break;
          case BYTES:
            keyValues[j] = _dataTable.getBytes(rowId, colId);
            break;
          case OBJECT:
            // TODO: Move ser/de into AggregationFunction interface
            DataTable.CustomObject customObject = _dataTable.getCustomObject(rowId, colId);
            if (customObject != null) {
              keyValues[j] = ObjectSerDeUtils.deserialize(customObject);
            }
            break;
          // Add other aggregation intermediate result / group-by column type supports here
          default:
            throw new IllegalStateException();
        }
      }
      PrimaryKey key = new PrimaryKey(keyValues);
      Object[] values = new Object[numCols];
      for (int colId = 0; colId < numCols; colId++) {
        DataSchema.ColumnDataType storedType = storedColumnDataTypes[colId];
        switch (storedType) {
          case INT:
            values[colId] = _dataTable.getInt(rowId, colId);
            break;
          case LONG:
            values[colId] = _dataTable.getLong(rowId, colId);
            break;
          case FLOAT:
            values[colId] = _dataTable.getFloat(rowId, colId);
            break;
          case DOUBLE:
            values[colId] = _dataTable.getDouble(rowId, colId);
            break;
          case BIG_DECIMAL:
            values[colId] = _dataTable.getBigDecimal(rowId, colId);
            break;
          case STRING:
            values[colId] = _dataTable.getString(rowId, colId);
            break;
          case BYTES:
            values[colId] = _dataTable.getBytes(rowId, colId);
            break;
          case OBJECT:
            // TODO: Move ser/de into AggregationFunction interface
            DataTable.CustomObject customObject = _dataTable.getCustomObject(rowId, colId);
            if (customObject != null) {
              values[colId] = ObjectSerDeUtils.deserialize(customObject);
            }
            break;
          // Add other aggregation intermediate result / group-by column type supports here
          default:
            throw new IllegalStateException();
        }
      }
      hashMap.put(key, values);
    }
    return hashMap;
  }

  public HashMap<String, Integer> getColumnIndex() {
    return _idxMap;
  }

  public DataSchema.ColumnDataType getDataType(int colIdx) {
    return _dataSchema.getColumnDataType(colIdx);
  }

  private final DataTable _dataTable;
  private final DataSchema _dataSchema;

  private final HashMap<String, Integer> _idxMap;
}
