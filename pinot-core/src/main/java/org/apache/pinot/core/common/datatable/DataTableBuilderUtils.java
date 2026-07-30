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
package org.apache.pinot.core.common.datatable;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.pinot.common.utils.ArrayListUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Helpers for writing values into a {@link DataTableBuilder}.
 */
public class DataTableBuilderUtils {
  private DataTableBuilderUtils() {
  }

  /**
   * Writes a non-null value of the given stored column data type into the {@link DataTableBuilder} at
   * the given column. Supports all scalar and array stored types. OBJECT columns are NOT handled here
   * (callers serialize them via the owning aggregation function). Used by the group-by result
   * serialization on both the server ({@code GroupByResultsBlock}) and the merge-only reduce path.
   */
  public static void setColumn(DataTableBuilder dataTableBuilder, ColumnDataType storedColumnDataType,
      int columnIndex, Object value)
      throws IOException {
    switch (storedColumnDataType) {
      case INT:
        dataTableBuilder.setColumn(columnIndex, (int) value);
        break;
      case LONG:
        dataTableBuilder.setColumn(columnIndex, (long) value);
        break;
      case FLOAT:
        dataTableBuilder.setColumn(columnIndex, (float) value);
        break;
      case DOUBLE:
        dataTableBuilder.setColumn(columnIndex, (double) value);
        break;
      case BIG_DECIMAL:
        dataTableBuilder.setColumn(columnIndex, (BigDecimal) value);
        break;
      case STRING:
        dataTableBuilder.setColumn(columnIndex, value.toString());
        break;
      case BYTES:
        dataTableBuilder.setColumn(columnIndex, (ByteArray) value);
        break;
      case INT_ARRAY:
        if (value instanceof IntArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toIntArray((IntArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (int[]) value);
        }
        break;
      case LONG_ARRAY:
        if (value instanceof LongArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toLongArray((LongArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (long[]) value);
        }
        break;
      case FLOAT_ARRAY:
        if (value instanceof FloatArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toFloatArray((FloatArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (float[]) value);
        }
        break;
      case DOUBLE_ARRAY:
        if (value instanceof DoubleArrayList) {
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toDoubleArray((DoubleArrayList) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (double[]) value);
        }
        break;
      case BIG_DECIMAL_ARRAY:
        if (value instanceof ObjectArrayList) {
          //noinspection unchecked
          dataTableBuilder.setColumn(columnIndex,
              ArrayListUtils.toBigDecimalArray((ObjectArrayList<BigDecimal>) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (BigDecimal[]) value);
        }
        break;
      case STRING_ARRAY:
        if (value instanceof ObjectArrayList) {
          //noinspection unchecked
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toStringArray((ObjectArrayList<String>) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (String[]) value);
        }
        break;
      case BYTES_ARRAY:
        if (value instanceof ObjectArrayList) {
          //noinspection unchecked
          dataTableBuilder.setColumn(columnIndex, ArrayListUtils.toBytesArray((ObjectArrayList<ByteArray>) value));
        } else {
          dataTableBuilder.setColumn(columnIndex, (ByteArray[]) value);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported stored type: " + storedColumnDataType);
    }
  }
}
