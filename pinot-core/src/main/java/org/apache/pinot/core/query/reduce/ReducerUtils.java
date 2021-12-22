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
package org.apache.pinot.core.query.reduce;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.Record;


public class ReducerUtils {
  private ReducerUtils() {
  }

  /**
   * Extracts a record from the data table.
   */
  public static Record getRecord(DataTable dataTable, int rowId, DataSchema.ColumnDataType[] storedColumnDataTypes) {
    int numColumns = storedColumnDataTypes.length;
    Object[] values = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      switch (storedColumnDataTypes[colId]) {
        case INT:
          values[colId] = dataTable.getInt(rowId, colId);
          break;
        case LONG:
          values[colId] = dataTable.getLong(rowId, colId);
          break;
        case FLOAT:
          values[colId] = dataTable.getFloat(rowId, colId);
          break;
        case DOUBLE:
          values[colId] = dataTable.getDouble(rowId, colId);
          break;
        case STRING:
          values[colId] = dataTable.getString(rowId, colId);
          break;
        case BYTES:
          values[colId] = dataTable.getBytes(rowId, colId);
          break;
        case OBJECT:
          values[colId] = dataTable.getObject(rowId, colId);
          break;
        // Add other aggregation intermediate result / group-by column type supports here
        default:
          throw new IllegalStateException();
      }
    }
    return new Record(values);
  }
}
