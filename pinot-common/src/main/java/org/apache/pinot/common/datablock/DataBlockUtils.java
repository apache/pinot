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
package org.apache.pinot.common.datablock;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.roaringbitmap.RoaringBitmap;


public final class DataBlockUtils {
  private DataBlockUtils() {
  }

  static final int VERSION_TYPE_SHIFT = 5;

  public static MetadataBlock getErrorDataBlock(Exception e) {
    if (e instanceof ProcessingException) {
      return getErrorDataBlock(Collections.singletonMap(((ProcessingException) e).getErrorCode(), extractErrorMsg(e)));
    } else {
      // TODO: Pass in meaningful error code.
      return getErrorDataBlock(Collections.singletonMap(QueryException.UNKNOWN_ERROR_CODE, extractErrorMsg(e)));
    }
  }

  private static String extractErrorMsg(Throwable t) {
    while (t.getCause() != null && t.getMessage() == null) {
      t = t.getCause();
    }
    return t.getMessage() + "\n" + QueryException.getTruncatedStackTrace(t);
  }

  public static MetadataBlock getErrorDataBlock(Map<Integer, String> exceptions) {
    MetadataBlock errorBlock = new MetadataBlock(MetadataBlock.MetadataBlockType.ERROR);
    for (Map.Entry<Integer, String> exception : exceptions.entrySet()) {
      errorBlock.addException(exception.getKey(), exception.getValue());
    }
    return errorBlock;
  }

  public static MetadataBlock getEndOfStreamDataBlock() {
    // TODO: add query statistics metadata for the block.
    return new MetadataBlock(MetadataBlock.MetadataBlockType.EOS);
  }

  public static MetadataBlock getEndOfStreamDataBlock(Map<String, String> stats) {
    // TODO: add query statistics metadata for the block.
    return new MetadataBlock(MetadataBlock.MetadataBlockType.EOS, stats);
  }

  public static DataBlock getDataBlock(ByteBuffer byteBuffer)
      throws IOException {
    int versionType = byteBuffer.getInt();
    int version = versionType & ((1 << VERSION_TYPE_SHIFT) - 1);
    DataBlock.Type type = DataBlock.Type.fromOrdinal(versionType >> VERSION_TYPE_SHIFT);
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

  public static List<Object[]> extractRows(DataBlock dataBlock, Function<CustomObject, Object> customObjectSerde) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType[] storedTypes = dataSchema.getStoredColumnDataTypes();
    RoaringBitmap[] nullBitmaps = extractNullBitmaps(dataBlock);
    int numRows = dataBlock.getNumberOfRows();
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int rowId = 0; rowId < numRows; rowId++) {
      rows.add(extractRowFromDataBlock(dataBlock, rowId, storedTypes, nullBitmaps, customObjectSerde));
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

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
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

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
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

  public static RoaringBitmap[] extractNullBitmaps(DataBlock dataBlock) {
    int numColumns = dataBlock.getDataSchema().size();
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = dataBlock.getNullRowIds(colId);
    }
    return nullBitmaps;
  }

  private static Object[] extractRowFromDataBlock(DataBlock dataBlock, int rowId, ColumnDataType[] storedTypes,
      RoaringBitmap[] nullBitmaps, Function<CustomObject, Object> customObjectSerde) {
    int numColumns = nullBitmaps.length;
    Object[] row = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      RoaringBitmap nullBitmap = nullBitmaps[colId];
      if (nullBitmap == null || !nullBitmap.contains(rowId)) {
        switch (storedTypes[colId]) {
          // Single-value column
          case INT:
            row[colId] = dataBlock.getInt(rowId, colId);
            break;
          case LONG:
            row[colId] = dataBlock.getLong(rowId, colId);
            break;
          case FLOAT:
            row[colId] = dataBlock.getFloat(rowId, colId);
            break;
          case DOUBLE:
            row[colId] = dataBlock.getDouble(rowId, colId);
            break;
          case BIG_DECIMAL:
            row[colId] = dataBlock.getBigDecimal(rowId, colId);
            break;
          case STRING:
            row[colId] = dataBlock.getString(rowId, colId);
            break;
          case BYTES:
            row[colId] = dataBlock.getBytes(rowId, colId);
            break;

          // Multi-value column
          case INT_ARRAY:
            row[colId] = dataBlock.getIntArray(rowId, colId);
            break;
          case LONG_ARRAY:
            row[colId] = dataBlock.getLongArray(rowId, colId);
            break;
          case FLOAT_ARRAY:
            row[colId] = dataBlock.getFloatArray(rowId, colId);
            break;
          case DOUBLE_ARRAY:
            row[colId] = dataBlock.getDoubleArray(rowId, colId);
            break;
          case STRING_ARRAY:
            row[colId] = dataBlock.getStringArray(rowId, colId);
            break;

          // Special intermediate result for aggregation function
          case OBJECT:
            row[colId] = customObjectSerde.apply(dataBlock.getCustomObject(rowId, colId));
            break;

          default:
            throw new IllegalStateException(
                String.format("Unsupported stored type: %s for column: %s", storedTypes[colId],
                    dataBlock.getDataSchema().getColumnName(colId)));
        }
      }
    }
    return row;
  }

  /**
   * Given a datablock and the column index, extracts the integer values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return int array of values in the column
   */
  public static int[] extractIntValuesForColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    int[] values = new int[numRows];
    if (nullBitmap == null) {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = (int) dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = (int) dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = (int) dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).intValue();
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot extract int values for column: %s with stored type: %s",
              dataSchema.getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = (int) dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = (int) dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = (int) dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).intValue();
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot extract int values for column: %s with stored type: %s",
              dataSchema.getColumnName(colId), storedType));
      }
    }
    return values;
  }

  /**
   * Given a datablock and the column index, extracts the long values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return long array of values in the column
   */
  public static long[] extractLongValuesForColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    long[] values = new long[numRows];
    if (nullBitmap == null) {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = (long) dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = (long) dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).longValue();
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract long values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = (long) dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = (long) dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).longValue();
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract long values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    }
    return values;
  }

  /**
   * Given a datablock and the column index, extracts the float values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return float array of values in the column
   */
  public static float[] extractFloatValuesForColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    float[] values = new float[numRows];
    if (nullBitmap == null) {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = (float) dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).floatValue();
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract float values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = (float) dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).floatValue();
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract float values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    }
    return values;
  }

  /**
   * Given a datablock and the column index, extracts the double values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return double array of values in the column
   */
  public static double[] extractDoubleValuesForColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    double[] values = new double[numRows];
    if (nullBitmap == null) {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).doubleValue();
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract double values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getInt(rowId, colId);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getLong(rowId, colId);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getFloat(rowId, colId);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getDouble(rowId, colId);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getBigDecimal(rowId, colId).doubleValue();
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract double values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    }
    return values;
  }

  /**
   * Given a datablock and the column index, extracts the BigDecimal values for the column. Prefer using this function
   * over extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to
   * the desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return BigDecimal array of values in the column
   */
  public static BigDecimal[] extractBigDecimalValuesForColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    BigDecimal[] values = new BigDecimal[numRows];
    if (nullBitmap == null) {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = BigDecimal.valueOf(dataBlock.getInt(rowId, colId));
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = BigDecimal.valueOf(dataBlock.getLong(rowId, colId));
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = BigDecimal.valueOf(dataBlock.getFloat(rowId, colId));
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = BigDecimal.valueOf(dataBlock.getDouble(rowId, colId));
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = dataBlock.getBigDecimal(rowId, colId);
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract BigDecimal values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = BigDecimal.valueOf(dataBlock.getInt(rowId, colId));
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = BigDecimal.valueOf(dataBlock.getLong(rowId, colId));
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = BigDecimal.valueOf(dataBlock.getFloat(rowId, colId));
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = BigDecimal.valueOf(dataBlock.getDouble(rowId, colId));
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (nullBitmap.contains(rowId)) {
              continue;
            }
            values[rowId] = dataBlock.getBigDecimal(rowId, colId);
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract BigDecimal values for column: %s with stored type: %s",
                  dataSchema.getColumnName(colId), storedType));
      }
    }
    return values;
  }

  /**
   * Given a datablock and the column index, extracts the String values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return String array of values in the column
   */
  public static String[] extractStringValuesForColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    Preconditions.checkState(storedType == ColumnDataType.STRING,
        "Cannot extract String values for column: %s with stored type: %s", dataSchema.getColumnName(colId),
        storedType);
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    String[] values = new String[numRows];
    if (nullBitmap == null) {
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = dataBlock.getString(rowId, colId);
      }
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (nullBitmap.contains(rowId)) {
          continue;
        }
        values[rowId] = dataBlock.getString(rowId, colId);
      }
    }
    return values;
  }

  /**
   * Given a datablock and the column index, extracts the byte values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return byte array of values in the column
   */
  public static byte[][] extractBytesValuesForColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    Preconditions.checkState(storedType == ColumnDataType.BYTES,
        "Cannot extract byte[] values for column: %s with stored type: %s", dataSchema.getColumnName(colId),
        storedType);
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    byte[][] values = new byte[numRows][];
    if (nullBitmap == null) {
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = dataBlock.getBytes(rowId, colId).getBytes();
      }
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (nullBitmap.contains(rowId)) {
          continue;
        }
        values[rowId] = dataBlock.getBytes(rowId, colId).getBytes();
      }
    }
    return values;
  }

  /**
   * Given a datablock and the column index, extracts the integer values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return int array of values in the column
   */
  public static int[] extractIntValuesForColumn(DataBlock dataBlock, int colId, int filterArgId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    int[] values = new int[numRows];
    int outRowId = 0;
    for (int inRowId = 0; inRowId < numRows; inRowId++) {
      if (dataBlock.getInt(inRowId, filterArgId) == 1) {
        if (nullBitmap != null && nullBitmap.contains(inRowId)) {
          outRowId++;
          continue;
        }
        switch (storedType) {
          case INT:
            values[outRowId++] = dataBlock.getInt(inRowId, colId);
            break;
          case LONG:
            values[outRowId++] = (int) dataBlock.getLong(inRowId, colId);
            break;
          case FLOAT:
            values[outRowId++] = (int) dataBlock.getFloat(inRowId, colId);
            break;
          case DOUBLE:
            values[outRowId++] = (int) dataBlock.getDouble(inRowId, colId);
            break;
          case BIG_DECIMAL:
            values[outRowId++] = dataBlock.getBigDecimal(inRowId, colId).intValue();
            break;
          default:
            throw new IllegalStateException(
                String.format("Cannot extract int values for column: %s with stored type: %s",
                    dataSchema.getColumnName(colId), storedType));
        }
      }
    }
    return Arrays.copyOfRange(values, 0, outRowId);
  }

  /**
   * Given a datablock and the column index, extracts the long values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return long array of values in the column
   */
  public static long[] extractLongValuesForColumn(DataBlock dataBlock, int colId, int filterArgId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    long[] rows = new long[numRows];
    int outRowId = 0;
    for (int inRowId = 0; inRowId < numRows; inRowId++) {
      if (dataBlock.getInt(inRowId, filterArgId) == 1) {
        if (nullBitmap != null && nullBitmap.contains(inRowId)) {
          outRowId++;
          continue;
        }
        switch (storedType) {
          case INT:
            rows[outRowId++] = dataBlock.getInt(inRowId, colId);
            break;
          case LONG:
            rows[outRowId++] = dataBlock.getLong(inRowId, colId);
            break;
          case FLOAT:
            rows[outRowId++] = (long) dataBlock.getFloat(inRowId, colId);
            break;
          case DOUBLE:
            rows[outRowId++] = (long) dataBlock.getDouble(inRowId, colId);
            break;
          case BIG_DECIMAL:
            rows[outRowId++] = dataBlock.getBigDecimal(inRowId, colId).longValue();
            break;
          default:
            throw new IllegalStateException(
                String.format("Cannot extract long values for column: %s with stored type: %s",
                    dataSchema.getColumnName(colId), storedType));
        }
      }
    }
    return Arrays.copyOfRange(rows, 0, outRowId);
  }

  /**
   * Given a datablock and the column index, extracts the float values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return float array of values in the column
   */
  public static float[] extractFloatValuesForColumn(DataBlock dataBlock, int colId, int filterArgId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    float[] rows = new float[numRows];
    int outRowId = 0;
    for (int inRowId = 0; inRowId < numRows; inRowId++) {
      if (dataBlock.getInt(inRowId, filterArgId) == 1) {
        if (nullBitmap != null && nullBitmap.contains(inRowId)) {
          outRowId++;
          continue;
        }
        switch (storedType) {
          case INT:
            rows[outRowId++] = dataBlock.getInt(inRowId, colId);
            break;
          case LONG:
            rows[outRowId++] = dataBlock.getLong(inRowId, colId);
            break;
          case FLOAT:
            rows[outRowId++] = dataBlock.getFloat(inRowId, colId);
            break;
          case DOUBLE:
            rows[outRowId++] = (float) dataBlock.getDouble(inRowId, colId);
            break;
          case BIG_DECIMAL:
            rows[outRowId++] = dataBlock.getBigDecimal(inRowId, colId).floatValue();
            break;
          default:
            throw new IllegalStateException(
                String.format("Cannot extract float values for column: %s with stored type: %s",
                    dataSchema.getColumnName(colId), storedType));
        }
      }
    }
    return Arrays.copyOfRange(rows, 0, outRowId);
  }

  /**
   * Given a datablock and the column index, extracts the double values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return double array of values in the column
   */
  public static double[] extractDoubleValuesForColumn(DataBlock dataBlock, int colId, int filterArgId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    double[] rows = new double[numRows];
    int outRowId = 0;
    for (int inRowId = 0; inRowId < numRows; inRowId++) {
      if (dataBlock.getInt(inRowId, filterArgId) == 1) {
        if (nullBitmap != null && nullBitmap.contains(inRowId)) {
          outRowId++;
          continue;
        }
        switch (storedType) {
          case INT:
            rows[outRowId++] = dataBlock.getInt(inRowId, colId);
            break;
          case LONG:
            rows[outRowId++] = dataBlock.getLong(inRowId, colId);
            break;
          case FLOAT:
            rows[outRowId++] = dataBlock.getFloat(inRowId, colId);
            break;
          case DOUBLE:
            rows[outRowId++] = dataBlock.getDouble(inRowId, colId);
            break;
          case BIG_DECIMAL:
            rows[outRowId++] = dataBlock.getBigDecimal(inRowId, colId).doubleValue();
            break;
          default:
            throw new IllegalStateException(
                String.format("Cannot extract double values for column: %s with stored type: %s",
                    dataSchema.getColumnName(colId), storedType));
        }
      }
    }
    return Arrays.copyOfRange(rows, 0, outRowId);
  }

  /**
   * Given a datablock and the column index, extracts the BigDecimal values for the column. Prefer using this function
   * over extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to
   * the desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return BigDecimal array of values in the column
   */
  public static BigDecimal[] extractBigDecimalValuesForColumn(DataBlock dataBlock, int colId, int filterArgId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    BigDecimal[] rows = new BigDecimal[numRows];
    int outRowId = 0;
    for (int inRowId = 0; inRowId < numRows; inRowId++) {
      if (dataBlock.getInt(inRowId, filterArgId) == 1) {
        if (nullBitmap != null && nullBitmap.contains(inRowId)) {
          outRowId++;
          continue;
        }
        switch (storedType) {
          case INT:
            rows[outRowId++] = BigDecimal.valueOf(dataBlock.getInt(inRowId, colId));
            break;
          case LONG:
            rows[outRowId++] = BigDecimal.valueOf(dataBlock.getLong(inRowId, colId));
            break;
          case FLOAT:
            rows[outRowId++] = BigDecimal.valueOf(dataBlock.getFloat(inRowId, colId));
            break;
          case DOUBLE:
            rows[outRowId++] = BigDecimal.valueOf(dataBlock.getDouble(inRowId, colId));
            break;
          case BIG_DECIMAL:
            rows[outRowId++] = dataBlock.getBigDecimal(inRowId, colId);
            break;
          default:
            throw new IllegalStateException(
                String.format("Cannot extract BigDecimal values for column: %s with stored type: %s",
                    dataSchema.getColumnName(colId), storedType));
        }
      }
    }
    return Arrays.copyOfRange(rows, 0, outRowId);
  }

  /**
   * Given a datablock and the column index, extracts the String values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return String array of values in the column
   */
  public static String[] extractStringValuesForColumn(DataBlock dataBlock, int colId, int filterArgId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    Preconditions.checkState(storedType == ColumnDataType.STRING,
        "Cannot extract String values for column: %s with stored type: %s", dataSchema.getColumnName(colId),
        storedType);
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    String[] rows = new String[numRows];
    int outRowId = 0;
    for (int inRowId = 0; inRowId < numRows; inRowId++) {
      if (dataBlock.getInt(inRowId, filterArgId) == 1) {
        if (nullBitmap != null && nullBitmap.contains(inRowId)) {
          outRowId++;
          continue;
        }
        rows[outRowId++] = dataBlock.getString(inRowId, colId);
      }
    }
    return Arrays.copyOfRange(rows, 0, outRowId);
  }

  /**
   * Given a datablock and the column index, extracts the byte values for the column. Prefer using this function over
   * extractRowFromDatablock if the desired datatype is known to prevent autoboxing to Object and later unboxing to the
   * desired type.
   * This only works on ROW format.
   * TODO: Add support for COLUMNAR format.
   * @return byte array of values in the column
   */
  public static byte[][] extractBytesValuesForColumn(DataBlock dataBlock, int colId, int filterArgId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    Preconditions.checkState(storedType == ColumnDataType.BYTES,
        "Cannot extract byte[] values for column: %s with stored type: %s", dataSchema.getColumnName(colId),
        storedType);
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    byte[][] rows = new byte[numRows][];
    int outRowId = 0;
    for (int inRowId = 0; inRowId < numRows; inRowId++) {
      if (dataBlock.getInt(inRowId, filterArgId) == 1) {
        if (nullBitmap != null && nullBitmap.contains(inRowId)) {
          outRowId++;
          continue;
        }
        rows[outRowId++] = dataBlock.getBytes(inRowId, colId).getBytes();
      }
    }
    return Arrays.copyOfRange(rows, 0, outRowId);
  }
}
