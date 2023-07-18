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
package org.apache.pinot.query.runtime.operator.utils;

import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.BooleanUtils;


public class TypeUtils {
  private TypeUtils() {
  }

  /**
   * Convert result to the appropriate column data type according to the desired {@link DataSchema.ColumnDataType}
   * of the {@link org.apache.pinot.core.common.Operator}.
   *
   * @param inputObj input entry
   * @param columnDataType desired column data type
   * @return converted entry
   */
  @Nullable
  public static Object convert(@Nullable Object inputObj, DataSchema.ColumnDataType columnDataType) {
    if (columnDataType.isNumber() && columnDataType != DataSchema.ColumnDataType.BIG_DECIMAL) {
      return inputObj == null ? null : columnDataType.convert(inputObj);
    } else {
      return inputObj;
    }
  }

  /**
   * This util is used to canonicalize row generated from V1 engine, which is stored using
   * {@link DataSchema#getStoredColumnDataTypes()} format. However, the transferable block ser/de stores data in the
   * {@link DataSchema#getColumnDataTypes()} format.
   *
   * @param row un-canonicalize row.
   * @param dataSchema data schema desired for the row.
   * @return canonicalize row.
   */
  public static Object[] canonicalizeRow(Object[] row, DataSchema dataSchema) {
    Object[] resultRow = new Object[row.length];
    for (int colId = 0; colId < row.length; colId++) {
      Object value = row[colId];
      if (value != null) {
        if (dataSchema.getColumnDataType(colId) == DataSchema.ColumnDataType.OBJECT) {
          resultRow[colId] = value;
        } else if (dataSchema.getColumnDataType(colId) == DataSchema.ColumnDataType.BOOLEAN) {
          resultRow[colId] = BooleanUtils.toBoolean(value);
        } else {
          resultRow[colId] = dataSchema.getColumnDataType(colId).convert(value);
        }
      }
    }
    return resultRow;
  }

  /**
   * Canonicalize rows with column indices not matching calcite order.
   *
   * see: {@link TypeUtils#canonicalizeRow(Object[], DataSchema)}
   */
  public static Object[] canonicalizeRow(Object[] row, DataSchema dataSchema, int[] columnIndices) {
    Object[] resultRow = new Object[columnIndices.length];
    for (int colId = 0; colId < columnIndices.length; colId++) {
      Object value = row[columnIndices[colId]];
      if (value != null) {
        resultRow[colId] = dataSchema.getColumnDataType(colId).convert(value);
      }
    }
    return resultRow;
  }
}
