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
package org.apache.pinot.core.common.datablock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;


public final class DataBlockUtils {
  protected static final int VERSION_TYPE_SHIFT = 5;
  private DataBlockUtils() {
    // do not instantiate.
  }

  public static MetadataBlock getErrorDataBlock(Exception e) {
    if (e instanceof ProcessingException) {
      return getErrorDataBlock(Collections.singletonMap(((ProcessingException) e).getErrorCode(), e.getMessage()));
    } else {
      return getErrorDataBlock(Collections.singletonMap(QueryException.UNKNOWN_ERROR_CODE, e.getMessage()));
    }
  }

  public static MetadataBlock getErrorDataBlock(Map<Integer, String> exceptions) {
    MetadataBlock errorBlock = new MetadataBlock();
    for (Map.Entry<Integer, String> exception : exceptions.entrySet()) {
      errorBlock.addException(exception.getKey(), exception.getValue());
    }
    return errorBlock;
  }

  public static MetadataBlock getEndOfStreamDataBlock(@Nonnull DataSchema dataSchema) {
    // TODO: add query statistics metadata for the block.
    return new MetadataBlock(dataSchema);
  }

  public static BaseDataBlock getDataBlock(ByteBuffer byteBuffer)
      throws IOException {
    int versionType = byteBuffer.getInt();
    int version = versionType & ((1 << VERSION_TYPE_SHIFT) - 1);
    BaseDataBlock.Type type = BaseDataBlock.Type.fromOrdinal(versionType >> VERSION_TYPE_SHIFT);
    switch (type) {
      case COLUMNAR:
        return new ColumnarDataBlock(byteBuffer);
      case ROW:
        return new RowDataBlock(byteBuffer);
      case METADATA:
        return new MetadataBlock(byteBuffer);
      default:
        throw new UnsupportedOperationException("Unsupported data table version: " + version + " with type: " + type);
    }
  }

  public static List<Object[]> extraRows(BaseDataBlock dataBlock) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int numRows = dataBlock.getNumberOfRows();
    int numColumns = storedColumnDataTypes.length;

    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      Object[] row = new Object[numColumns];
      for (int j = 0; j < numColumns; j++) {
        switch (storedColumnDataTypes[j]) {
          // Single-value column
          case INT:
            row[j] = dataBlock.getInt(i, j);
            break;
          case LONG:
            row[j] = dataBlock.getLong(i, j);
            break;
          case FLOAT:
            row[j] = dataBlock.getFloat(i, j);
            break;
          case DOUBLE:
            row[j] = dataBlock.getDouble(i, j);
            break;
          case BIG_DECIMAL:
            row[j] = dataBlock.getBigDecimal(i, j);
            break;
          case STRING:
            row[j] = dataBlock.getString(i, j);
            break;
          case BYTES:
            row[j] = dataBlock.getBytes(i, j);
            break;

          // Multi-value column
          case INT_ARRAY:
            row[j] = dataBlock.getIntArray(i, j);
            break;
          case LONG_ARRAY:
            row[j] = dataBlock.getLongArray(i, j);
            break;
          case FLOAT_ARRAY:
            row[j] = dataBlock.getFloatArray(i, j);
            break;
          case DOUBLE_ARRAY:
            row[j] = dataBlock.getDoubleArray(i, j);
            break;
          case STRING_ARRAY:
            row[j] = dataBlock.getStringArray(i, j);
            break;

          default:
            throw new IllegalStateException(
                String.format("Unsupported data type: %s for column: %s", storedColumnDataTypes[j],
                    dataSchema.getColumnName(j)));
        }
      }
      rows.add(row);
    }
    return rows;
  }

  /**
   * Given a {@link DataSchema}, compute each column's offset and fill them into the passed in array, then return the
   * row size in bytes.
   *
   * @param dataSchema data schema.
   * @param columnOffsets array of column offsets.
   * @return row size in bytes.
   */
  public static int computeColumnOffsets(DataSchema dataSchema, int[] columnOffsets) {
    int numColumns = columnOffsets.length;
    assert numColumns == dataSchema.size();

    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int rowSizeInBytes = 0;
    for (int i = 0; i < numColumns; i++) {
      columnOffsets[i] = rowSizeInBytes;
      switch (storedColumnDataTypes[i]) {
        case INT:
          rowSizeInBytes += 4;
          break;
        case LONG:
          rowSizeInBytes += 8;
          break;
        case FLOAT:
          rowSizeInBytes += 4;
          break;
        case DOUBLE:
          rowSizeInBytes += 8;
          break;
        case STRING:
          rowSizeInBytes += 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          rowSizeInBytes += 8;
          break;
      }
    }

    return rowSizeInBytes;
  }

  /**
   * Given a {@link DataSchema}, compute each column's size and fill them into the passed in array.
   *
   * @param dataSchema data schema.
   * @param columnSizes array of column size.
   * @return row size in bytes.
   */
  public static void computeColumnSizeInBytes(DataSchema dataSchema, int[] columnSizes) {
    int numColumns = columnSizes.length;
    assert numColumns == dataSchema.size();

    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    for (int i = 0; i < numColumns; i++) {
      switch (storedColumnDataTypes[i]) {
        case INT:
          columnSizes[i] = 4;
          break;
        case LONG:
          columnSizes[i] = 8;
          break;
        case FLOAT:
          columnSizes[i] = 4;
          break;
        case DOUBLE:
          columnSizes[i] = 8;
          break;
        case STRING:
          columnSizes[i] = 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          columnSizes[i] = 8;
          break;
      }
    }
  }
}
