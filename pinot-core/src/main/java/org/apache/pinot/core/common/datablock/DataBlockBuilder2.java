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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


public class DataBlockBuilder2 {
  private final DataSchema _dataSchema;
  private final DataBlock.Type _blockType;
  private final int _numRows;
  private final int _numColumns;

  private int[] _columnOffsets;
  private int _rowSizeInBytes;
  private int[] _cumulativeColumnOffsetSizeInBytes;
  private int[] _columnSizeInBytes;

  private final Object2IntOpenHashMap<String> _dictionary = new Object2IntOpenHashMap<>();
  private final List<ByteBuffer> _fixedSizeBuffers = new ArrayList<>();
  private int _writtenFixedSizeBytes = 0;
  private final PagedPinotOutputStream _varSizeDataOutputStream
      = new PagedPinotOutputStream(PagedPinotOutputStream.HeapPageAllocator.createSmall());
  private final List<ByteBuffer> _varSizeBuffers = new ArrayList<>();
  private int _writtenVarSizeBytes = 0;

  private DataBlockBuilder2(DataSchema dataSchema, DataBlock.Type blockType, int numRows) {
    _dataSchema = dataSchema;
    _blockType = blockType;
    _numRows = numRows;
    _numColumns = dataSchema.size();
    if (_blockType == DataBlock.Type.ROW) {
      _columnOffsets = new int[_numColumns];
      _rowSizeInBytes = DataBlockUtils.computeColumnOffsets(dataSchema, _columnOffsets);

//      int nullBytes = _numColumns * 8; // we need 2 ints per column to store the roaring bitmaps offsets
//      int expectedFixedSizeStreamSize = (_rowSizeInBytes + nullBytes) * numRows;
//      _fixedSizeDataByteArrayOutputStream = new UnsynchronizedByteArrayOutputStream(expectedFixedSizeStreamSize);
//      _fixedSizeDataOutputStream = new DataOutputStream(_fixedSizeDataByteArrayOutputStream);
    } else {
//      _fixedSizeDataByteArrayOutputStream = new UnsynchronizedByteArrayOutputStream(8192);
//      _fixedSizeDataOutputStream = new DataOutputStream(_fixedSizeDataByteArrayOutputStream);

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

  private void writeVarOffsetInFixed(ByteBuffer fixed)
      throws IOException {
    long offsetInVar = _varSizeDataOutputStream.getCurrentOffset();
    Preconditions.checkState(offsetInVar <= Integer.MAX_VALUE,
        "Cannot handle variable size output stream larger than 2GB");
    fixed.putInt((int) offsetInVar);
  }

  public void setNullRowIds(RoaringBitmap[] nullVectors)
      throws IOException {
    int fixedBufSize = nullVectors.length * Integer.BYTES * 2;
    ByteBuffer fixedSize = ByteBuffer.allocate(fixedBufSize)
        .order(ByteOrder.BIG_ENDIAN);

    int varBufSize = Arrays.stream(nullVectors)
        .mapToInt(bitmap -> bitmap == null ? 0 : bitmap.serializedSizeInBytes())
        .sum();
    ByteBuffer variableSize = ByteBuffer.allocate(varBufSize)
        .order(ByteOrder.BIG_ENDIAN);

    int startVariableOffset = _writtenVarSizeBytes;
    for (RoaringBitmap nullRowIds : nullVectors) {
      int writtenVarBytes = variableSize.position();
      fixedSize.putInt(startVariableOffset + writtenVarBytes);
      if (nullRowIds == null || nullRowIds.isEmpty()) {
        fixedSize.putInt(0);
      } else {
        RoaringBitmapUtils.serialize(nullRowIds, variableSize);
        fixedSize.putInt(variableSize.position() - writtenVarBytes);
      }
    }
  }

  public static RowDataBlock buildFromRows(List<Object[]> rows, DataSchema dataSchema)
      throws IOException {
    int numRows = rows.size();
    DataBlockBuilder2 rowBuilder = new DataBlockBuilder2(dataSchema, DataBlock.Type.ROW, numRows);
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
    ByteBuffer byteBuffer = ByteBuffer.allocate(rowBuilder._rowSizeInBytes * numRows);
    for (int rowId = 0; rowId < numRows; rowId++) {
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
    }
    rowBuilder.addFixedBuffer(byteBuffer);
    // Write null bitmaps after writing data.
    rowBuilder.setNullRowIds(nullBitmaps);
    return buildRowBlock(rowBuilder);
  }

  public static ColumnarDataBlock buildFromColumns(List<Object[]> columns, DataSchema dataSchema)
      throws IOException {
    int numRows = columns.isEmpty() ? 0 : columns.get(0).length;
    DataBlockBuilder2 columnarBuilder = new DataBlockBuilder2(dataSchema, DataBlock.Type.COLUMNAR, numRows);
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
      columnarBuilder.addFixedBuffer(byteBuffer);
    }
    // Write null bitmaps after writing data.
    columnarBuilder.setNullRowIds(nullBitmaps);
    return buildColumnarBlock(columnarBuilder);
  }

  private void addFixedBuffer(ByteBuffer byteBuffer) {
    byteBuffer.flip();
    _fixedSizeBuffers.add(byteBuffer);
    _writtenFixedSizeBytes += byteBuffer.remaining();
  }

  private void addVariableBuffer(ByteBuffer byteBuffer) {
    byteBuffer.flip();
    _varSizeBuffers.add(byteBuffer);
    _writtenVarSizeBytes += byteBuffer.remaining();
  }

  private static RowDataBlock buildRowBlock(DataBlockBuilder2 builder) {
    return new RowDataBlock(builder._numRows, builder._dataSchema, getReverseDictionary(builder._dictionary),
        new CompoundDataBuffer(builder._fixedSizeBuffers.toArray(new ByteBuffer[0]), ByteOrder.BIG_ENDIAN, true),
        new CompoundDataBuffer(builder._varSizeBuffers.toArray(new ByteBuffer[0]), ByteOrder.BIG_ENDIAN, true)
    );
  }

  private static ColumnarDataBlock buildColumnarBlock(DataBlockBuilder2 builder) {
    return new ColumnarDataBlock(builder._numRows, builder._dataSchema, getReverseDictionary(builder._dictionary),
        new CompoundDataBuffer(builder._fixedSizeBuffers.toArray(new ByteBuffer[0]), ByteOrder.BIG_ENDIAN, true),
        new CompoundDataBuffer(builder._varSizeBuffers.toArray(new ByteBuffer[0]), ByteOrder.BIG_ENDIAN, true)
    );
  }

  private static String[] getReverseDictionary(Object2IntOpenHashMap<String> dictionary) {
    String[] reverseDictionary = new String[dictionary.size()];
    for (Object2IntMap.Entry<String> entry : dictionary.object2IntEntrySet()) {
      reverseDictionary[entry.getIntValue()] = entry.getKey();
    }
    return reverseDictionary;
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, BigDecimal value)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    // This can be slightly optimized with a specialized serialization method
    byte[] bytes = BigDecimalUtils.serialize(value);
    byteBuffer.putInt(bytes.length);
    builder._varSizeDataOutputStream.write(bytes);
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, String value) {
    Object2IntOpenHashMap<String> dictionary = builder._dictionary;
    int dictId = dictionary.computeIntIfAbsent(value, k -> dictionary.size());
    byteBuffer.putInt(dictId);
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, ByteArray value)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    byte[] bytes = value.getBytes();
    byteBuffer.putInt(bytes.length);
    builder._varSizeDataOutputStream.write(bytes);
  }

  // TODO: Move ser/de into AggregationFunction interface
  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, @Nullable Object value)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    if (value == null) {
      byteBuffer.putInt(0);
      builder._varSizeDataOutputStream.writeInt(CustomObject.NULL_TYPE_VALUE);
    } else {
      int objectTypeValue = ObjectSerDeUtils.ObjectType.getObjectType(value).getValue();
      byte[] bytes = ObjectSerDeUtils.serialize(value, objectTypeValue);
      byteBuffer.putInt(bytes.length);
      builder._varSizeDataOutputStream.writeInt(objectTypeValue);
      builder._varSizeDataOutputStream.write(bytes);
    }
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, int[] values)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    byteBuffer.putInt(values.length);
    for (int value : values) {
      builder._varSizeDataOutputStream.writeInt(value);
    }
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, long[] values)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    byteBuffer.putInt(values.length);
    for (long value : values) {
      builder._varSizeDataOutputStream.writeLong(value);
    }
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, float[] values)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    byteBuffer.putInt(values.length);
    for (float value : values) {
      builder._varSizeDataOutputStream.writeFloat(value);
    }
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, double[] values)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    byteBuffer.putInt(values.length);
    for (double value : values) {
      builder._varSizeDataOutputStream.writeDouble(value);
    }
  }

  private static void setColumn(DataBlockBuilder2 builder, ByteBuffer byteBuffer, String[] values)
      throws IOException {
    builder.writeVarOffsetInFixed(byteBuffer);
    byteBuffer.putInt(values.length);
    Object2IntOpenHashMap<String> dictionary = builder._dictionary;
    for (String value : values) {
      int dictId = dictionary.computeIntIfAbsent(value, k -> dictionary.size());
      builder._varSizeDataOutputStream.writeInt(dictId);
    }
  }
}
