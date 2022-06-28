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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


public class DataBlockBuilder {
  private final DataSchema _dataSchema;
  private final BaseDataBlock.Type _blockType;
  private final DataSchema.ColumnDataType[] _columnDataType;

  private int[] _columnOffsets;
  private int _rowSizeInBytes;
  private int[] _cumulativeColumnOffsetSizeInBytes;
  private int[] _columnSizeInBytes;

  private int _numRows;
  private int _numColumns;

  private final Object2IntOpenHashMap<String> _dictionary = new Object2IntOpenHashMap<>();
  private final ByteArrayOutputStream _fixedSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  private final DataOutputStream _fixedSizeDataOutputStream = new DataOutputStream(_fixedSizeDataByteArrayOutputStream);
  private final ByteArrayOutputStream _variableSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  private final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  private DataBlockBuilder(DataSchema dataSchema, BaseDataBlock.Type blockType) {
    _dataSchema = dataSchema;
    _columnDataType = dataSchema.getStoredColumnDataTypes();
    _blockType = blockType;
    _numColumns = dataSchema.size();
    if (_blockType == BaseDataBlock.Type.COLUMNAR) {
      _cumulativeColumnOffsetSizeInBytes = new int[_numColumns];
      _columnSizeInBytes = new int[_numColumns];
      DataBlockUtils.computeColumnSizeInBytes(_dataSchema, _columnSizeInBytes);
      int cumulativeColumnOffset = 0;
      for (int i = 0; i < _numColumns; i++) {
        _cumulativeColumnOffsetSizeInBytes[i] = cumulativeColumnOffset;
        cumulativeColumnOffset += _columnSizeInBytes[i] * _numRows;
      }
    } else if (_blockType == BaseDataBlock.Type.ROW) {
      _columnOffsets = new int[_numColumns];
      _rowSizeInBytes = DataBlockUtils.computeColumnOffsets(dataSchema, _columnOffsets);
    }
  }

  public void setNullRowIds(@Nullable RoaringBitmap nullRowIds)
      throws IOException {
    _fixedSizeDataOutputStream.writeInt(_variableSizeDataByteArrayOutputStream.size());
    if (nullRowIds == null || nullRowIds.isEmpty()) {
      _fixedSizeDataOutputStream.writeInt(0);
    } else {
      byte[] bitmapBytes = ObjectSerDeUtils.ROARING_BITMAP_SER_DE.serialize(nullRowIds);
      _fixedSizeDataOutputStream.writeInt(bitmapBytes.length);
      _variableSizeDataByteArrayOutputStream.write(bitmapBytes);
    }
  }

  public static RowDataBlock buildFromRows(List<Object[]> rows, @Nullable RoaringBitmap[] colNullBitmaps,
      DataSchema dataSchema)
      throws IOException {
    DataBlockBuilder rowBuilder = new DataBlockBuilder(dataSchema, BaseDataBlock.Type.ROW);
    rowBuilder._numRows = rows.size();
    for (Object[] row : rows) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(rowBuilder._rowSizeInBytes);
      for (int i = 0; i < rowBuilder._numColumns; i++) {
        Object value = row[i];
        switch (rowBuilder._columnDataType[i]) {
          // Single-value column
          case INT:
            byteBuffer.putInt(((Number) value).intValue());
            break;
          case LONG:
            byteBuffer.putLong(((Number) value).longValue());
            break;
          case FLOAT:
            byteBuffer.putFloat(((Number) value).floatValue());
            break;
          case DOUBLE:
            byteBuffer.putDouble(((Number) value).doubleValue());
            break;
          case BIG_DECIMAL:
            setColumn(rowBuilder, byteBuffer, (BigDecimal) value);
            break;
          case STRING:
            setColumn(rowBuilder, byteBuffer, (String) value);
            break;
          case BYTES:
            setColumn(rowBuilder, byteBuffer, (ByteArray) value);
            break;
          case OBJECT:
            setColumn(rowBuilder, byteBuffer, value);
            break;
          // Multi-value column
          case BOOLEAN_ARRAY:
          case INT_ARRAY:
            setColumn(rowBuilder, byteBuffer, (int[]) value);
            break;
          case TIMESTAMP_ARRAY:
          case LONG_ARRAY:
            // LONG_ARRAY type covers INT_ARRAY and LONG_ARRAY
            if (value instanceof int[]) {
              int[] ints = (int[]) value;
              int length = ints.length;
              long[] longs = new long[length];
              ArrayCopyUtils.copy(ints, longs, length);
              setColumn(rowBuilder, byteBuffer, longs);
            } else {
              setColumn(rowBuilder, byteBuffer, (long[]) value);
            }
            break;
          case FLOAT_ARRAY:
            setColumn(rowBuilder, byteBuffer, (float[]) value);
            break;
          case DOUBLE_ARRAY:
            // DOUBLE_ARRAY type covers INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY and DOUBLE_ARRAY
            if (value instanceof int[]) {
              int[] ints = (int[]) value;
              int length = ints.length;
              double[] doubles = new double[length];
              ArrayCopyUtils.copy(ints, doubles, length);
              setColumn(rowBuilder, byteBuffer, doubles);
            } else if (value instanceof long[]) {
              long[] longs = (long[]) value;
              int length = longs.length;
              double[] doubles = new double[length];
              ArrayCopyUtils.copy(longs, doubles, length);
              setColumn(rowBuilder, byteBuffer, doubles);
            } else if (value instanceof float[]) {
              float[] floats = (float[]) value;
              int length = floats.length;
              double[] doubles = new double[length];
              ArrayCopyUtils.copy(floats, doubles, length);
              setColumn(rowBuilder, byteBuffer, doubles);
            } else {
              setColumn(rowBuilder, byteBuffer, (double[]) value);
            }
            break;
          case BYTES_ARRAY:
          case STRING_ARRAY:
            setColumn(rowBuilder, byteBuffer, (String[]) value);
            break;
          default:
            throw new IllegalStateException(
                String.format("Unsupported data type: %s for column: %s", rowBuilder._columnDataType[i],
                    rowBuilder._dataSchema.getColumnName(i)));
        }
      }
      rowBuilder._fixedSizeDataByteArrayOutputStream.write(byteBuffer.array(), 0, byteBuffer.position());
    }
    // Write null bitmaps after writing data.
    if (colNullBitmaps != null) {
      for (RoaringBitmap nullBitmap : colNullBitmaps) {
        rowBuilder.setNullRowIds(nullBitmap);
      }
    }
    return buildRowBlock(rowBuilder);
  }

  public static ColumnarDataBlock buildFromColumns(List<Object[]> columns, @Nullable RoaringBitmap[] colNullBitmaps,
      DataSchema dataSchema)
      throws IOException {
    DataBlockBuilder columnarBuilder = new DataBlockBuilder(dataSchema, BaseDataBlock.Type.COLUMNAR);
    for (int i = 0; i < columns.size(); i++) {
      Object[] column = columns.get(i);
      columnarBuilder._numRows = column.length;
      ByteBuffer byteBuffer = ByteBuffer.allocate(columnarBuilder._numRows * columnarBuilder._columnSizeInBytes[i]);
      switch (columnarBuilder._columnDataType[i]) {
        // Single-value column
        case INT:
          for (Object value : column) {
            byteBuffer.putInt(((Number) value).intValue());
          }
          break;
        case LONG:
          for (Object value : column) {
            byteBuffer.putLong(((Number) value).longValue());
          }
          break;
        case FLOAT:
          for (Object value : column) {
            byteBuffer.putFloat(((Number) value).floatValue());
          }
          break;
        case DOUBLE:
          for (Object value : column) {
            byteBuffer.putDouble(((Number) value).doubleValue());
          }
          break;
        case BIG_DECIMAL:
          for (Object value : column) {
            setColumn(columnarBuilder, byteBuffer, (BigDecimal) value);
          }
          break;
        case STRING:
          for (Object value : column) {
            setColumn(columnarBuilder, byteBuffer, (String) value);
          }
          break;
        case BYTES:
          for (Object value : column) {
            setColumn(columnarBuilder, byteBuffer, (ByteArray) value);
          }
          break;
        case OBJECT:
          for (Object value : column) {
            setColumn(columnarBuilder, byteBuffer, value);
          }
          break;
        // Multi-value column
        case BOOLEAN_ARRAY:
        case INT_ARRAY:
          for (Object value : column) {
            setColumn(columnarBuilder, byteBuffer, (int[]) value);
          }
          break;
        case TIMESTAMP_ARRAY:
        case LONG_ARRAY:
          for (Object value : column) {
            if (value instanceof int[]) {
              // LONG_ARRAY type covers INT_ARRAY and LONG_ARRAY
              int[] ints = (int[]) value;
              int length = ints.length;
              long[] longs = new long[length];
              ArrayCopyUtils.copy(ints, longs, length);
              setColumn(columnarBuilder, byteBuffer, longs);
            } else {
              setColumn(columnarBuilder, byteBuffer, (long[]) value);
            }
          }
          break;
        case FLOAT_ARRAY:
          for (Object value : column) {
            setColumn(columnarBuilder, byteBuffer, (float[]) value);
          }
          break;
        case DOUBLE_ARRAY:
          for (Object value : column) {
            // DOUBLE_ARRAY type covers INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY and DOUBLE_ARRAY
            if (value instanceof int[]) {
              int[] ints = (int[]) value;
              int length = ints.length;
              double[] doubles = new double[length];
              ArrayCopyUtils.copy(ints, doubles, length);
              setColumn(columnarBuilder, byteBuffer, doubles);
            } else if (value instanceof long[]) {
              long[] longs = (long[]) value;
              int length = longs.length;
              double[] doubles = new double[length];
              ArrayCopyUtils.copy(longs, doubles, length);
              setColumn(columnarBuilder, byteBuffer, doubles);
            } else if (value instanceof float[]) {
              float[] floats = (float[]) value;
              int length = floats.length;
              double[] doubles = new double[length];
              ArrayCopyUtils.copy(floats, doubles, length);
              setColumn(columnarBuilder, byteBuffer, doubles);
            } else {
              setColumn(columnarBuilder, byteBuffer, (double[]) value);
            }
          }
          break;
        case BYTES_ARRAY:
        case STRING_ARRAY:
          for (Object value : column) {
            setColumn(columnarBuilder, byteBuffer, (String[]) value);
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Unsupported data type: %s for column: %s", columnarBuilder._columnDataType[i],
                  columnarBuilder._dataSchema.getColumnName(i)));
      }
      columnarBuilder._fixedSizeDataByteArrayOutputStream.write(byteBuffer.array(), 0, byteBuffer.position());
    }
    // Write null bitmaps after writing data.
    if (colNullBitmaps != null) {
      for (RoaringBitmap nullBitmap : colNullBitmaps) {
        columnarBuilder.setNullRowIds(nullBitmap);
      }
    }
    return buildColumnarBlock(columnarBuilder);
  }

  private static RowDataBlock buildRowBlock(DataBlockBuilder builder) {
    return new RowDataBlock(builder._numRows, builder._dataSchema, getReverseDictionary(builder._dictionary),
        builder._fixedSizeDataByteArrayOutputStream.toByteArray(),
        builder._variableSizeDataByteArrayOutputStream.toByteArray());
  }

  private static ColumnarDataBlock buildColumnarBlock(DataBlockBuilder builder) {
    return new ColumnarDataBlock(builder._numRows, builder._dataSchema, getReverseDictionary(builder._dictionary),
        builder._fixedSizeDataByteArrayOutputStream.toByteArray(),
        builder._variableSizeDataByteArrayOutputStream.toByteArray());
  }

  private static String[] getReverseDictionary(Object2IntOpenHashMap<String> dictionary) {
    String[] reverseDictionary = new String[dictionary.size()];
    for (Object2IntMap.Entry<String> entry : dictionary.object2IntEntrySet()) {
      reverseDictionary[entry.getIntValue()] = entry.getKey();
    }
    return reverseDictionary;
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, BigDecimal value)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = BigDecimalUtils.serialize(value);
    byteBuffer.putInt(bytes.length);
    builder._variableSizeDataByteArrayOutputStream.write(bytes);
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, String value) {
    Object2IntOpenHashMap<String> dictionary = builder._dictionary;
    int dictId = dictionary.computeIntIfAbsent(value, k -> dictionary.size());
    byteBuffer.putInt(dictId);
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, ByteArray value)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = value.getBytes();
    byteBuffer.putInt(bytes.length);
    builder._variableSizeDataByteArrayOutputStream.write(bytes);
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, Object value)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    int objectTypeValue = ObjectSerDeUtils.ObjectType.getObjectType(value).getValue();
    if (objectTypeValue == ObjectSerDeUtils.ObjectType.Null.getValue()) {
      byteBuffer.putInt(0);
      builder._variableSizeDataOutputStream.writeInt(objectTypeValue);
    } else {
      byte[] bytes = ObjectSerDeUtils.serialize(value, objectTypeValue);
      byteBuffer.putInt(bytes.length);
      builder._variableSizeDataOutputStream.writeInt(objectTypeValue);
      builder._variableSizeDataByteArrayOutputStream.write(bytes);
    }
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, int[] values)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byteBuffer.putInt(values.length);
    for (int value : values) {
      builder._variableSizeDataOutputStream.writeInt(value);
    }
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, long[] values)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byteBuffer.putInt(values.length);
    for (long value : values) {
      builder._variableSizeDataOutputStream.writeLong(value);
    }
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, float[] values)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byteBuffer.putInt(values.length);
    for (float value : values) {
      builder._variableSizeDataOutputStream.writeFloat(value);
    }
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, double[] values)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byteBuffer.putInt(values.length);
    for (double value : values) {
      builder._variableSizeDataOutputStream.writeDouble(value);
    }
  }

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, String[] values)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byteBuffer.putInt(values.length);
    Object2IntOpenHashMap<String> dictionary = builder._dictionary;
    for (String value : values) {
      int dictId = dictionary.computeIntIfAbsent(value, k -> dictionary.size());
      builder._variableSizeDataOutputStream.writeInt(dictId);
    }
  }
}
