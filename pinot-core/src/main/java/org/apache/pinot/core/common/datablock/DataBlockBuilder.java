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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


public class DataBlockBuilder {
  private final DataSchema _dataSchema;
  private final DataBlock.Type _blockType;
  private final int _numRows;
  private final int _numColumns;

  private int[] _columnOffsets;
  private int _rowSizeInBytes;
  private int[] _cumulativeColumnOffsetSizeInBytes;
  private int[] _columnSizeInBytes;

  private final Object2IntOpenHashMap<String> _dictionary = new Object2IntOpenHashMap<>();
  private final UnsynchronizedByteArrayOutputStream _fixedSizeDataByteArrayOutputStream;
  private final DataOutputStream _fixedSizeDataOutputStream;
  private final UnsynchronizedByteArrayOutputStream _variableSizeDataByteArrayOutputStream =
      new UnsynchronizedByteArrayOutputStream(8192);
  private final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  private DataBlockBuilder(DataSchema dataSchema, DataBlock.Type blockType, int numRows) {
    _dataSchema = dataSchema;
    _blockType = blockType;
    _numRows = numRows;
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
    int numRows = rows.size();
    DataBlockBuilder rowBuilder = new DataBlockBuilder(dataSchema, DataBlock.Type.ROW, numRows);
    // TODO: consolidate these null utils into data table utils.
    // Selection / Agg / Distinct all have similar code.
    ColumnDataType[] storedTypes = dataSchema.getStoredColumnDataTypes();
    int numColumns = storedTypes.length;
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    Object[] nullPlaceholders = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = new RoaringBitmap();
      nullPlaceholders[colId] = storedTypes[colId].getNullPlaceholder();
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(rowBuilder._rowSizeInBytes);
    for (int rowId = 0; rowId < numRows; rowId++) {
      byteBuffer.clear();
      Object[] row = rows.get(rowId);
      for (int colId = 0; colId < numColumns; colId++) {
        Object value = row[colId];
        if (value == null) {
          nullBitmaps[colId].add(rowId);
          value = nullPlaceholders[colId];
        }

        // NOTE:
        // We intentionally make the type casting very strict here (e.g. only accepting Integer for INT) to ensure the
        // rows conform to the data schema. This can help catch the unexpected data type issues early.
        switch (storedTypes[colId]) {
          // Single-value column
          case INT:
            byteBuffer.putInt((int) value);
            break;
          case LONG:
            byteBuffer.putLong((long) value);
            break;
          case FLOAT:
            byteBuffer.putFloat((float) value);
            break;
          case DOUBLE:
            byteBuffer.putDouble((double) value);
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
          case VECTOR:
            setColumn(rowBuilder, byteBuffer, (Vector) value);
            break;
          // Multi-value column
          case INT_ARRAY:
            setColumn(rowBuilder, byteBuffer, (int[]) value);
            break;
          case LONG_ARRAY:
            setColumn(rowBuilder, byteBuffer, (long[]) value);
            break;
          case FLOAT_ARRAY:
            setColumn(rowBuilder, byteBuffer, (float[]) value);
            break;
          case DOUBLE_ARRAY:
            setColumn(rowBuilder, byteBuffer, (double[]) value);
            break;
          case STRING_ARRAY:
            setColumn(rowBuilder, byteBuffer, (String[]) value);
            break;

          // Special intermediate result for aggregation function
          case OBJECT:
            setColumn(rowBuilder, byteBuffer, value);
            break;

          // Null
          case UNKNOWN:
            setColumn(rowBuilder, byteBuffer, (Object) null);
            break;

          default:
            throw new IllegalStateException(
                String.format("Unsupported stored type: %s for column: %s", storedTypes[colId],
                    dataSchema.getColumnName(colId)));
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
    ColumnDataType[] storedTypes = dataSchema.getStoredColumnDataTypes();
    int numColumns = storedTypes.length;
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    Object[] nullPlaceholders = new Object[numColumns];
    for (int colId = 0; colId < numColumns; colId++) {
      nullBitmaps[colId] = new RoaringBitmap();
      nullPlaceholders[colId] = storedTypes[colId].getNullPlaceholder();
    }
    for (int colId = 0; colId < numColumns; colId++) {
      Object[] column = columns.get(colId);
      ByteBuffer byteBuffer = ByteBuffer.allocate(numRows * columnarBuilder._columnSizeInBytes[colId]);
      Object value;

      // NOTE:
      // We intentionally make the type casting very strict here (e.g. only accepting Integer for INT) to ensure the
      // rows conform to the data schema. This can help catch the unexpected data type issues early.
      switch (storedTypes[colId]) {
        // Single-value column
        case INT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putInt((int) value);
          }
          break;
        case LONG:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putLong((long) value);
          }
          break;
        case FLOAT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putFloat((float) value);
          }
          break;
        case DOUBLE:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            byteBuffer.putDouble((double) value);
          }
          break;
        case BIG_DECIMAL:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (BigDecimal) value);
          }
          break;
        case STRING:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (String) value);
          }
          break;
        case BYTES:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (ByteArray) value);
          }
          break;
        case VECTOR:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (Vector) value);
          }
          break;
        // Multi-value column
        case INT_ARRAY:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (int[]) value);
          }
          break;
        case LONG_ARRAY:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (long[]) value);
          }
          break;
        case FLOAT_ARRAY:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (float[]) value);
          }
          break;
        case DOUBLE_ARRAY:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (double[]) value);
          }
          break;
        case STRING_ARRAY:
          for (int rowId = 0; rowId < numRows; rowId++) {
            value = column[rowId];
            if (value == null) {
              nullBitmaps[colId].add(rowId);
              value = nullPlaceholders[colId];
            }
            setColumn(columnarBuilder, byteBuffer, (String[]) value);
          }
          break;

        // Special intermediate result for aggregation function
        case OBJECT:
          for (int rowId = 0; rowId < numRows; rowId++) {
            setColumn(columnarBuilder, byteBuffer, column[rowId]);
          }
          break;

        // Null
        case UNKNOWN:
          for (int rowId = 0; rowId < numRows; rowId++) {
            setColumn(columnarBuilder, byteBuffer, (Object) null);
          }
          break;

        default:
          throw new IllegalStateException(
              String.format("Unsupported stored type: %s for column: %s", storedTypes[colId],
                  dataSchema.getColumnName(colId)));
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

  private static void setColumn(DataBlockBuilder builder, ByteBuffer byteBuffer, Vector value)
      throws IOException {
    byteBuffer.putInt(builder._variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = value.toBytes();
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
