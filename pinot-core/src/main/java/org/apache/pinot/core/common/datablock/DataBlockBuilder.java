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
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


public class DataBlockBuilder {
  private final DataSchema _dataSchema;
  private final DataBlock.Type _blockType;
  private final DataSchema.ColumnDataType[] _columnDataTypes;

  private int[] _columnOffsets;
  private int _rowSizeInBytes;
  private int[] _cumulativeColumnOffsetSizeInBytes;
  private int[] _columnSizeInBytes;

  private int _numRows;
  private int _numColumns;

  private final Object2IntOpenHashMap<String> _dictionary = new Object2IntOpenHashMap<>();
  private final UnsynchronizedByteArrayOutputStream _fixedSizeDataByteArrayOutputStream;
  private final DataOutputStream _fixedSizeDataOutputStream;
  private final UnsynchronizedByteArrayOutputStream _variableSizeDataByteArrayOutputStream
      = new UnsynchronizedByteArrayOutputStream(8192);
  private final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  private DataBlockBuilder(DataSchema dataSchema, DataBlock.Type blockType, int numRows) {
    _dataSchema = dataSchema;
    _columnDataTypes = dataSchema.getColumnDataTypes();
    _blockType = blockType;
    _numColumns = dataSchema.size();
    if (_blockType == DataBlock.Type.ROW) {
      _columnOffsets = new int[_numColumns];
      _rowSizeInBytes = DataBlockUtils.computeColumnOffsets(dataSchema, _columnOffsets);

      int nullBytes = _numColumns * 8; // we need 2 ints per column to store the roaring bitmaps offsets
      int expectedFixedSizeStreamSize = (_rowSizeInBytes + nullBytes) * numRows;
      _fixedSizeDataByteArrayOutputStream = new UnsynchronizedByteArrayOutputStream(expectedFixedSizeStreamSize);
      _fixedSizeDataOutputStream = new DataOutputStream(_fixedSizeDataByteArrayOutputStream);
    } else {
      _fixedSizeDataByteArrayOutputStream = new UnsynchronizedByteArrayOutputStream(8192);
      _fixedSizeDataOutputStream = new DataOutputStream(_fixedSizeDataByteArrayOutputStream);

      if (_blockType == DataBlock.Type.COLUMNAR) {
        _cumulativeColumnOffsetSizeInBytes = new int[_numColumns];
        _columnSizeInBytes = new int[_numColumns];
        DataBlockUtils.computeColumnSizeInBytes(_dataSchema, _columnSizeInBytes);
        int cumulativeColumnOffset = 0;
        for (int i = 0; i < _numColumns; i++) {
          _cumulativeColumnOffsetSizeInBytes[i] = cumulativeColumnOffset;
          cumulativeColumnOffset += _columnSizeInBytes[i] * _numRows;
        }
      }
    }
  }

  public void setNullRowIds(@Nullable RoaringBitmap nullRowIds)
      throws IOException {
    _fixedSizeDataOutputStream.writeInt(_variableSizeDataByteArrayOutputStream.size());
    if (nullRowIds == null || nullRowIds.isEmpty()) {
      _fixedSizeDataOutputStream.writeInt(0);
    } else {
      byte[] bitmapBytes = RoaringBitmapUtils.serialize(nullRowIds);
      _fixedSizeDataOutputStream.writeInt(bitmapBytes.length);
      _variableSizeDataByteArrayOutputStream.write(bitmapBytes);
    }
  }

  public static RowDataBlock buildFromRows(List<Object[]> rows, DataSchema dataSchema)
      throws IOException {
    DataBlockBuilder rowBuilder = new DataBlockBuilder(dataSchema, DataBlock.Type.ROW, rows.size());
    // TODO: consolidate these null utils into data table utils.
    // Selection / Agg / Distinct all have similar code.
    int numColumns = rowBuilder._numColumns;
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    Object[] nullPlaceholders = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = new RoaringBitmap();
      nullPlaceholders[colId] = columnDataTypes[colId].convert(storedColumnDataTypes[colId].getNullPlaceholder());
    }
    rowBuilder._numRows = rows.size();
    ByteBuffer byteBuffer = ByteBuffer.allocate(rowBuilder._rowSizeInBytes);
    for (int rowId = 0; rowId < rows.size(); rowId++) {
      byteBuffer.clear();
      Object[] row = rows.get(rowId);
      for (int colId = 0; colId < rowBuilder._numColumns; colId++) {
        Object value = row[colId];
        if (value == null) {
          nullBitmaps[colId].add(rowId);
          value = nullPlaceholders[colId];
        }
        switch (rowBuilder._columnDataTypes[colId]) {
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
          case BOOLEAN:
            byteBuffer.putInt(((Boolean) value) ? 1 : 0);
            break;
          case TIMESTAMP:
            if (value instanceof Long) {
              byteBuffer.putLong((long) value);
            } else {
              byteBuffer.putLong(((Timestamp) value).getTime());
            }
            break;
          case STRING:
            setColumn(rowBuilder, byteBuffer, (String) value);
            break;
          case BYTES:
            if (value instanceof byte[]) {
              setColumn(rowBuilder, byteBuffer, new ByteArray((byte[]) value));
            } else {
              setColumn(rowBuilder, byteBuffer, (ByteArray) value);
            }
            break;
          case OBJECT:
            setColumn(rowBuilder, byteBuffer, value);
            break;
          // Multi-value column
          case INT_ARRAY:
            setColumn(rowBuilder, byteBuffer, (int[]) value);
            break;
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
            setColumn(rowBuilder, byteBuffer, (byte[][]) value);
            break;
          case STRING_ARRAY:
            setColumn(rowBuilder, byteBuffer, (String[]) value);
            break;
          case BOOLEAN_ARRAY:
            boolean[] booleans = (boolean[]) value;
            int length = booleans.length;
            int[] ints = new int[length];
            ArrayCopyUtils.copy(booleans, ints, length);
            setColumn(rowBuilder, byteBuffer, ints);
            break;
          case TIMESTAMP_ARRAY:
            Timestamp[] timestamps = (Timestamp[]) value;
            length = timestamps.length;
            long[] longs = new long[length];
            ArrayCopyUtils.copy(timestamps, longs, length);
            setColumn(rowBuilder, byteBuffer, longs);
            break;
          case UNKNOWN:
            setColumn(rowBuilder, byteBuffer, (Object) null);
            break;
          default:
            throw new IllegalStateException(
                String.format("Unsupported data type: %s for column: %s", rowBuilder._columnDataTypes[colId],
                    rowBuilder._dataSchema.getColumnName(colId)));
        }
      }
      rowBuilder._fixedSizeDataByteArrayOutputStream.write(byteBuffer.array(), 0, byteBuffer.position());
    }
    // Write null bitmaps after writing data.
    for (RoaringBitmap nullBitmap : nullBitmaps) {
      rowBuilder.setNullRowIds(nullBitmap);
    }
    return buildRowBlock(rowBuilder);
  }

  public static ColumnarDataBlock buildFromColumns(List<Object[]> columns, DataSchema dataSchema)
      throws IOException {
    int numRows = columns.isEmpty() ? 0 : columns.get(0).length;
    DataBlockBuilder columnarBuilder = new DataBlockBuilder(dataSchema, DataBlock.Type.COLUMNAR, numRows);

    // TODO: consolidate these null utils into data table utils.
    // Selection / Agg / Distinct all have similar code.
    int numColumns = columnarBuilder._numColumns;
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    DataSchema.ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    Object[] nullPlaceholders = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = new RoaringBitmap();
      nullPlaceholders[colId] = columnDataTypes[colId].convert(storedColumnDataTypes[colId].getNullPlaceholder());
    }
    for (int colId = 0; colId < columns.size(); colId++) {
      Object[] column = columns.get(colId);
      columnarBuilder._numRows = column.length;
      ByteBuffer byteBuffer = ByteBuffer.allocate(columnarBuilder._numRows * columnarBuilder._columnSizeInBytes[colId]);
      Object value;
      switch (columnarBuilder._columnDataTypes[colId]) {
        // Single-value column
        case INT:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putInt(((Number) value).intValue());
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putLong(((Number) value).longValue());
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putFloat(((Number) value).floatValue());
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putDouble(((Number) value).doubleValue());
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (BigDecimal) value);
          }
          break;
        case BOOLEAN:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putInt(((Boolean) value) ? 1 : 0);
          }
          break;
        case TIMESTAMP:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putLong(((Timestamp) value).getTime());
          }
          break;
        case STRING:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (String) value);
          }
          break;
        case BYTES:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (ByteArray) value);
          }
          break;
        case OBJECT:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, value);
          }
          break;
        // Multi-value column
        case INT_ARRAY:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (int[]) value);
          }
          break;
        case LONG_ARRAY:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
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
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (float[]) value);
          }
          break;
        case DOUBLE_ARRAY:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
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
        case BOOLEAN_ARRAY:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            int length = ((boolean[]) value).length;
            int[] ints = new int[length];
            ArrayCopyUtils.copy((boolean[]) value, ints, length);
            setColumn(columnarBuilder, byteBuffer, ints);
          }
          break;
        case TIMESTAMP_ARRAY:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            int length = ((Timestamp[]) value).length;
            long[] longs = new long[length];
            ArrayCopyUtils.copy((Timestamp[]) value, longs, length);
            setColumn(columnarBuilder, byteBuffer, longs);
          }
          break;
        case BYTES_ARRAY:
        case STRING_ARRAY:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (String[]) value);
          }
          break;
        case UNKNOWN:
          for (int rowId = 0; rowId < columnarBuilder._numRows; rowId++) {
            setColumn(columnarBuilder, byteBuffer, (Object) null);
          }
          break;
        default:
          throw new IllegalStateException(
              String.format("Unsupported data type: %s for column: %s", columnarBuilder._columnDataTypes[colId],
                  columnarBuilder._dataSchema.getColumnName(colId)));
      }
      columnarBuilder._fixedSizeDataByteArrayOutputStream.write(byteBuffer.array(), 0, byteBuffer.position());
    }
    // Write null bitmaps after writing data.
    for (RoaringBitmap nullBitmap : nullBitmaps) {
      columnarBuilder.setNullRowIds(nullBitmap);
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

  // TODO: Move ser/de into AggregationFunction interface
  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, @Nullable Object value)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    if (value == null) {
      byteBuffer.putInt(0);
      builder._variableSizeDataOutputStream.writeInt(CustomObject.NULL_TYPE_VALUE);
    } else {
      int objectTypeValue = ObjectSerDeUtils.ObjectType.getObjectType(value).getValue();
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
