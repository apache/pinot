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
package org.apache.pinot.core.util;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Utils to extract values from {@link DataBlock}.
 */
public final class DataBlockExtractUtils {
  private DataBlockExtractUtils() {
  }

  public static List<Object[]> extractRows(DataBlock dataBlock) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType[] storedTypes = dataSchema.getStoredColumnDataTypes();
    int numColumns = storedTypes.length;
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = dataBlock.getNullRowIds(colId);
    }
    int numRows = dataBlock.getNumberOfRows();
    List<Object[]> rows = new ArrayList<>(numRows);
    for (int rowId = 0; rowId < numRows; rowId++) {
      Object[] row = new Object[numColumns];
      for (int colId = 0; colId < numColumns; colId++) {
        RoaringBitmap nullBitmap = nullBitmaps[colId];
        if (nullBitmap == null || !nullBitmap.contains(rowId)) {
          row[colId] = extractValue(dataBlock, storedTypes[colId], rowId, colId);
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private static Object extractValue(DataBlock dataBlock, ColumnDataType storedType, int rowId, int colId) {
    switch (storedType) {
      // Single-value column
      case INT:
        return dataBlock.getInt(rowId, colId);
      case LONG:
        return dataBlock.getLong(rowId, colId);
      case FLOAT:
        return dataBlock.getFloat(rowId, colId);
      case DOUBLE:
        return dataBlock.getDouble(rowId, colId);
      case BIG_DECIMAL:
        return dataBlock.getBigDecimal(rowId, colId);
      case STRING:
        return dataBlock.getString(rowId, colId);
      case BYTES:
        return dataBlock.getBytes(rowId, colId);
      case VECTOR:
        return dataBlock.getVector(rowId, colId);

      // Multi-value column
      case INT_ARRAY:
        return dataBlock.getIntArray(rowId, colId);
      case LONG_ARRAY:
        return dataBlock.getLongArray(rowId, colId);
      case FLOAT_ARRAY:
        return dataBlock.getFloatArray(rowId, colId);
      case DOUBLE_ARRAY:
        return dataBlock.getDoubleArray(rowId, colId);
      case STRING_ARRAY:
        return dataBlock.getStringArray(rowId, colId);

      // Special intermediate result for aggregation function
      case OBJECT:
        return ObjectSerDeUtils.deserialize(dataBlock.getCustomObject(rowId, colId));

      default:
        throw new IllegalStateException(String.format("Unsupported stored type: %s for column: %s", storedType,
            dataBlock.getDataSchema().getColumnName(colId)));
    }
  }

  public static Key[] extractKeys(DataBlock dataBlock, int[] keyIds) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    int numKeys = keyIds.length;
    ColumnDataType[] storedTypes = new ColumnDataType[numKeys];
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numKeys];
    for (int colId = 0; colId < numKeys; colId++) {
      storedTypes[colId] = dataSchema.getColumnDataType(keyIds[colId]).getStoredType();
      nullBitmaps[colId] = dataBlock.getNullRowIds(keyIds[colId]);
    }
    int numRows = dataBlock.getNumberOfRows();
    Key[] keys = new Key[numRows];
    for (int rowId = 0; rowId < numRows; rowId++) {
      Object[] values = new Object[numKeys];
      for (int colId = 0; colId < numKeys; colId++) {
        RoaringBitmap nullBitmap = nullBitmaps[colId];
        if (nullBitmap == null || !nullBitmap.contains(rowId)) {
          values[colId] = extractValue(dataBlock, storedTypes[colId], rowId, keyIds[colId]);
        }
      }
      keys[rowId] = new Key(values);
    }
    return keys;
  }

  public static Key[] extractKeys(DataBlock dataBlock, int[] keyIds, int numMatchedRows, RoaringBitmap matchedBitmap) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    int numKeys = keyIds.length;
    ColumnDataType[] storedTypes = new ColumnDataType[numKeys];
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numKeys];
    for (int colId = 0; colId < numKeys; colId++) {
      storedTypes[colId] = dataSchema.getColumnDataType(keyIds[colId]).getStoredType();
      nullBitmaps[colId] = dataBlock.getNullRowIds(keyIds[colId]);
    }
    Key[] keys = new Key[numMatchedRows];
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      Object[] values = new Object[numKeys];
      for (int colId = 0; colId < numKeys; colId++) {
        RoaringBitmap nullBitmap = nullBitmaps[colId];
        if (nullBitmap == null || !nullBitmap.contains(rowId)) {
          values[colId] = extractValue(dataBlock, storedTypes[colId], rowId, keyIds[colId]);
        }
      }
      keys[matchedRowId] = new Key(values);
    }
    return keys;
  }

  public static Object[] extractColumn(DataBlock dataBlock, int colId) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    int numRows = dataBlock.getNumberOfRows();
    Object[] values = new Object[numRows];
    if (nullBitmap == null) {
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = extractValue(dataBlock, storedType, rowId, colId);
      }
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (!nullBitmap.contains(rowId)) {
          values[rowId] = extractValue(dataBlock, storedType, rowId, colId);
        }
      }
    }
    return values;
  }

  public static Object[] extractColumn(DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap) {
    DataSchema dataSchema = dataBlock.getDataSchema();
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
    Object[] values = new Object[numMatchedRows];
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    if (nullBitmap == null) {
      for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
        int rowId = iterator.next();
        values[matchedRowId] = extractValue(dataBlock, storedType, rowId, colId);
      }
    } else {
      for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
        int rowId = iterator.next();
        if (!nullBitmap.contains(rowId)) {
          values[matchedRowId] = extractValue(dataBlock, storedType, rowId, colId);
        }
      }
    }
    return values;
  }

  public static int[] extractIntColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    int numRows = dataBlock.getNumberOfRows();
    int[] values = new int[numRows];
    if (numRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
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
              dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getInt(rowId, colId);
            }
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = (int) dataBlock.getLong(rowId, colId);
            }
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = (int) dataBlock.getFloat(rowId, colId);
            }
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = (int) dataBlock.getDouble(rowId, colId);
            }
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getBigDecimal(rowId, colId).intValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot extract int values for column: %s with stored type: %s",
              dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static long[] extractLongColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    int numRows = dataBlock.getNumberOfRows();
    long[] values = new long[numRows];
    if (numRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
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
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getInt(rowId, colId);
            }
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getLong(rowId, colId);
            }
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = (long) dataBlock.getFloat(rowId, colId);
            }
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = (long) dataBlock.getDouble(rowId, colId);
            }
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getBigDecimal(rowId, colId).longValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract long values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static float[] extractFloatColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    int numRows = dataBlock.getNumberOfRows();
    float[] values = new float[numRows];
    if (numRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
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
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getInt(rowId, colId);
            }
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getLong(rowId, colId);
            }
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getFloat(rowId, colId);
            }
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = (float) dataBlock.getDouble(rowId, colId);
            }
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getBigDecimal(rowId, colId).floatValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract float values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static double[] extractDoubleColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    int numRows = dataBlock.getNumberOfRows();
    double[] values = new double[numRows];
    if (numRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
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
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getInt(rowId, colId);
            }
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getLong(rowId, colId);
            }
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getFloat(rowId, colId);
            }
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getDouble(rowId, colId);
            }
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            if (!nullBitmap.contains(rowId)) {
              values[rowId] = dataBlock.getBigDecimal(rowId, colId).doubleValue();
            }
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract double values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static BigDecimal[] extractBigDecimalColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    int numRows = dataBlock.getNumberOfRows();
    BigDecimal[] values = new BigDecimal[numRows];
    if (numRows == 0) {
      return values;
    }
    if (storedType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BIG_DECIMAL);
      return values;
    }
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
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    } else {
      switch (storedType) {
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = !nullBitmap.contains(rowId) ? BigDecimal.valueOf(dataBlock.getInt(rowId, colId))
                : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] = !nullBitmap.contains(rowId) ? BigDecimal.valueOf(dataBlock.getLong(rowId, colId))
                : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] =
                !nullBitmap.contains(rowId) ? new BigDecimal(Float.toString(dataBlock.getFloat(rowId, colId)))
                    : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] =
                !nullBitmap.contains(rowId) ? new BigDecimal(Double.toString(dataBlock.getDouble(rowId, colId)))
                    : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            values[rowId] =
                !nullBitmap.contains(rowId) ? dataBlock.getBigDecimal(rowId, colId) : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract BigDecimal values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static String[] extractStringColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    int numRows = dataBlock.getNumberOfRows();
    String[] values = new String[numRows];
    if (numRows == 0) {
      return values;
    }
    if (storedType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.STRING);
      return values;
    }
    Preconditions.checkState(storedType == DataType.STRING,
        "Cannot extract String values for column: %s with stored type: %s",
        dataBlock.getDataSchema().getColumnName(colId), storedType);
    if (nullBitmap == null) {
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = dataBlock.getString(rowId, colId);
      }
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = !nullBitmap.contains(rowId) ? dataBlock.getString(rowId, colId) : NullValuePlaceHolder.STRING;
      }
    }
    return values;
  }

  public static byte[][] extractBytesColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    int numRows = dataBlock.getNumberOfRows();
    byte[][] values = new byte[numRows][];
    if (numRows == 0) {
      return values;
    }
    if (storedType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BYTES);
      return values;
    }
    Preconditions.checkState(storedType == DataType.BYTES,
        "Cannot extract byte[] values for column: %s with stored type: %s",
        dataBlock.getDataSchema().getColumnName(colId), storedType);
    if (nullBitmap == null) {
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = dataBlock.getBytes(rowId, colId).getBytes();
      }
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] =
            !nullBitmap.contains(rowId) ? dataBlock.getBytes(rowId, colId).getBytes() : NullValuePlaceHolder.BYTES;
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
  public static Vector[] extractVectorValuesForColumn(DataType storedType, DataBlock dataBlock, int colId,
      @Nullable RoaringBitmap nullBitmap) {
    // Get null bitmap for the column.
    int numRows = dataBlock.getNumberOfRows();
    Vector[] values = new Vector[numRows];

    if (storedType == DataType.UNKNOWN) {
      //TODO: Fill proper null value
      Arrays.fill(values, null);
      return values;
    }
    Preconditions.checkState(storedType == DataType.VECTOR,
        "Cannot extract Vector values for column: %s with stored type: %s",
        dataBlock.getDataSchema().getColumnName(colId), storedType);
    for (int rowId = 0; rowId < numRows; rowId++) {
      if (nullBitmap != null && nullBitmap.contains(rowId)) {
        continue;
      }
      values[rowId] = dataBlock.getVector(rowId, colId);
    }

    return values;
  }

  public static int[] extractIntColumn(DataType storedType, DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, @Nullable RoaringBitmap matchedNullBitmap) {
    int[] values = new int[numMatchedRows];
    if (numMatchedRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      switch (storedType) {
        case INT:
          values[matchedRowId] = dataBlock.getInt(rowId, colId);
          break;
        case LONG:
          values[matchedRowId] = (int) dataBlock.getLong(rowId, colId);
          break;
        case FLOAT:
          values[matchedRowId] = (int) dataBlock.getFloat(rowId, colId);
          break;
        case DOUBLE:
          values[matchedRowId] = (int) dataBlock.getDouble(rowId, colId);
          break;
        case BIG_DECIMAL:
          values[matchedRowId] = dataBlock.getBigDecimal(rowId, colId).intValue();
          break;
        default:
          throw new IllegalStateException(String.format("Cannot extract int values for column: %s with stored type: %s",
              dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static long[] extractLongColumn(DataType storedType, DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, @Nullable RoaringBitmap matchedNullBitmap) {
    long[] values = new long[numMatchedRows];
    if (numMatchedRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      switch (storedType) {
        case INT:
          values[matchedRowId] = dataBlock.getInt(rowId, colId);
          break;
        case LONG:
          values[matchedRowId] = dataBlock.getLong(rowId, colId);
          break;
        case FLOAT:
          values[matchedRowId] = (long) dataBlock.getFloat(rowId, colId);
          break;
        case DOUBLE:
          values[matchedRowId] = (long) dataBlock.getDouble(rowId, colId);
          break;
        case BIG_DECIMAL:
          values[matchedRowId] = dataBlock.getBigDecimal(rowId, colId).longValue();
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract long values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static float[] extractFloatColumn(DataType storedType, DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, @Nullable RoaringBitmap matchedNullBitmap) {
    float[] values = new float[numMatchedRows];
    if (numMatchedRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      switch (storedType) {
        case INT:
          values[matchedRowId] = dataBlock.getInt(rowId, colId);
          break;
        case LONG:
          values[matchedRowId] = dataBlock.getLong(rowId, colId);
          break;
        case FLOAT:
          values[matchedRowId] = dataBlock.getFloat(rowId, colId);
          break;
        case DOUBLE:
          values[matchedRowId] = (float) dataBlock.getDouble(rowId, colId);
          break;
        case BIG_DECIMAL:
          values[matchedRowId] = dataBlock.getBigDecimal(rowId, colId).floatValue();
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract float values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static double[] extractDoubleColumn(DataType storedType, DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, @Nullable RoaringBitmap matchedNullBitmap) {
    double[] values = new double[numMatchedRows];
    if (numMatchedRows == 0 || storedType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      switch (storedType) {
        case INT:
          values[matchedRowId] = dataBlock.getInt(rowId, colId);
          break;
        case LONG:
          values[matchedRowId] = dataBlock.getLong(rowId, colId);
          break;
        case FLOAT:
          values[matchedRowId] = dataBlock.getFloat(rowId, colId);
          break;
        case DOUBLE:
          values[matchedRowId] = dataBlock.getDouble(rowId, colId);
          break;
        case BIG_DECIMAL:
          values[matchedRowId] = dataBlock.getBigDecimal(rowId, colId).doubleValue();
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract double values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static BigDecimal[] extractBigDecimalColumn(DataType storedType, DataBlock dataBlock, int colId,
      int numMatchedRows, RoaringBitmap matchedBitmap, @Nullable RoaringBitmap matchedNullBitmap) {
    BigDecimal[] values = new BigDecimal[numMatchedRows];
    if (numMatchedRows == 0) {
      return values;
    }
    if (storedType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BIG_DECIMAL);
      return values;
    }
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId)) {
        values[matchedRowId] = NullValuePlaceHolder.BIG_DECIMAL;
        continue;
      }
      switch (storedType) {
        case INT:
          values[matchedRowId] = BigDecimal.valueOf(dataBlock.getInt(rowId, colId));
          break;
        case LONG:
          values[matchedRowId] = BigDecimal.valueOf(dataBlock.getLong(rowId, colId));
          break;
        case FLOAT:
          values[matchedRowId] = BigDecimal.valueOf(dataBlock.getFloat(rowId, colId));
          break;
        case DOUBLE:
          values[matchedRowId] = BigDecimal.valueOf(dataBlock.getDouble(rowId, colId));
          break;
        case BIG_DECIMAL:
          values[matchedRowId] = dataBlock.getBigDecimal(rowId, colId);
          break;
        default:
          throw new IllegalStateException(
              String.format("Cannot extract BigDecimal values for column: %s with stored type: %s",
                  dataBlock.getDataSchema().getColumnName(colId), storedType));
      }
    }
    return values;
  }

  public static String[] extractStringColumn(DataType storedType, DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, @Nullable RoaringBitmap matchedNullBitmap) {
    String[] values = new String[numMatchedRows];
    if (numMatchedRows == 0) {
      return values;
    }
    if (storedType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.STRING);
      return values;
    }
    Preconditions.checkState(storedType == DataType.STRING,
        "Cannot extract String values for column: %s with stored type: %s",
        dataBlock.getDataSchema().getColumnName(colId), storedType);
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      boolean isNull = matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId);
      values[matchedRowId] = !isNull ? dataBlock.getString(rowId, colId) : NullValuePlaceHolder.STRING;
    }
    return values;
  }

  public static byte[][] extractBytesColumn(DataType storedType, DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, @Nullable RoaringBitmap matchedNullBitmap) {
    byte[][] values = new byte[numMatchedRows][];
    if (numMatchedRows == 0) {
      return values;
    }
    if (storedType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BYTES);
      return values;
    }
    Preconditions.checkState(storedType == DataType.BYTES,
        "Cannot extract byte[] values for column: %s with stored type: %s",
        dataBlock.getDataSchema().getColumnName(colId), storedType);
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      boolean isNull = matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId);
      values[matchedRowId] = !isNull ? dataBlock.getBytes(rowId, colId).getBytes() : NullValuePlaceHolder.BYTES;
    }
    return values;
  }

  public static Vector[] extractVectorColumn(DataType storedType, DataBlock dataBlock, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, RoaringBitmap matchedNullBitmap) {
    Vector[] values = new Vector[numMatchedRows];
    if (numMatchedRows == 0) {
      return values;
    }
    if (storedType == DataType.UNKNOWN) {
      Arrays.fill(values, null);
      return values;
    }
    Preconditions.checkState(storedType == DataType.VECTOR,
        "Cannot extract Vector values for column: %s with stored type: %s",
        dataBlock.getDataSchema().getColumnName(colId), storedType);
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      boolean isNull = matchedNullBitmap != null && matchedNullBitmap.contains(matchedRowId);
      values[matchedRowId] = !isNull ? dataBlock.getVector(rowId, colId) : null;
    }
    return values;
  }
}
