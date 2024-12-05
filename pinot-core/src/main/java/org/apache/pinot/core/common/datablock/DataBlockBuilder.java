/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.MapUtils;
import org.roaringbitmap.RoaringBitmap;


public class DataBlockBuilder {

  private DataBlockBuilder() {
  }

  public static RowDataBlock buildFromRows(List<Object[]> rows, DataSchema dataSchema)
      throws IOException {
    return buildFromRows(rows, dataSchema, PagedPinotOutputStream.HeapPageAllocator.createSmall());
  }

  public static RowDataBlock buildFromRows(List<Object[]> rows, DataSchema dataSchema,
      PagedPinotOutputStream.PageAllocator allocator)
      throws IOException {
    int numRows = rows.size();

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
    int nullFixedBytes = numColumns * Integer.BYTES * 2;
    int rowSizeInBytes = calculateBytesPerRow(dataSchema);
    int fixedBytesRequired = rowSizeInBytes * numRows + nullFixedBytes;
    ByteBuffer fixedSize = ByteBuffer.allocate(fixedBytesRequired)
        .order(ByteOrder.BIG_ENDIAN);

    PagedPinotOutputStream varSize = new PagedPinotOutputStream(allocator);
    Object2IntOpenHashMap<String> dictionary = new Object2IntOpenHashMap<>();

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
            fixedSize.putInt((int) value);
            break;
          case LONG:
            fixedSize.putLong((long) value);
            break;
          case FLOAT:
            fixedSize.putFloat((float) value);
            break;
          case DOUBLE:
            fixedSize.putDouble((double) value);
            break;
          case BIG_DECIMAL:
            setColumn(fixedSize, varSize, (BigDecimal) value);
            break;
          case STRING:
            int dictId = dictionary.computeIfAbsent((String) value, k -> dictionary.size());
            fixedSize.putInt(dictId);
            break;
          case BYTES:
            setColumn(fixedSize, varSize, (ByteArray) value);
            break;
          case MAP:
            setColumn(fixedSize, varSize, (Map) value);
            break;
          // Multi-value column
          case INT_ARRAY:
            if (value instanceof IntArrayList) {
              setColumn(fixedSize, varSize, ((IntArrayList) value).elements());
            } else {
              setColumn(fixedSize, varSize, (int[]) value);
            }
            break;
          case LONG_ARRAY:
            if (value instanceof LongArrayList) {
              setColumn(fixedSize, varSize, ((LongArrayList) value).elements());
            } else {
              setColumn(fixedSize, varSize, (long[]) value);
            }
            break;
          case FLOAT_ARRAY:
            if (value instanceof FloatArrayList) {
              setColumn(fixedSize, varSize, ((FloatArrayList) value).elements());
            } else {
              setColumn(fixedSize, varSize, (float[]) value);
            }
            break;
          case DOUBLE_ARRAY:
            if (value instanceof DoubleArrayList) {
              setColumn(fixedSize, varSize, ((DoubleArrayList) value).elements());
            } else {
              setColumn(fixedSize, varSize, (double[]) value);
            }
            break;
          case STRING_ARRAY:
            setColumn(fixedSize, varSize, (String[]) value, dictionary);
            break;

          // Special intermediate result for aggregation function
          case OBJECT:
            setColumn(fixedSize, varSize, value);
            break;

          // Null
          case UNKNOWN:
            setColumn(fixedSize, varSize, (Object) null);
            break;

          default:
            throw new IllegalStateException("Unsupported stored type: " + storedTypes[colId] + " for column: "
                + dataSchema.getColumnName(colId));
        }
      }
    }

    CompoundDataBuffer.Builder varBufferBuilder = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN, true)
        .addPagedOutputStream(varSize);

    // Write null bitmaps after writing data.
    setNullRowIds(nullBitmaps, fixedSize, varBufferBuilder);
    return buildRowBlock(numRows, dataSchema, getReverseDictionary(dictionary), fixedSize, varBufferBuilder);
  }

  public static ColumnarDataBlock buildFromColumns(List<Object[]> columns, DataSchema dataSchema)
      throws IOException {
    return buildFromColumns(columns, dataSchema, PagedPinotOutputStream.HeapPageAllocator.createSmall());
  }

  public static ColumnarDataBlock buildFromColumns(List<Object[]> columns, DataSchema dataSchema,
      PagedPinotOutputStream.PageAllocator allocator)
      throws IOException {
    int numRows = columns.isEmpty() ? 0 : columns.get(0).length;

    int fixedBytesPerRow = calculateBytesPerRow(dataSchema);
    int nullFixedBytes = dataSchema.size() * Integer.BYTES * 2;
    int fixedBytesRequired = fixedBytesPerRow * numRows + nullFixedBytes;

    Object2IntOpenHashMap<String> dictionary = new Object2IntOpenHashMap<>();

    // TODO: consolidate these null utils into data table utils.
    // Selection / Agg / Distinct all have similar code.
    int numColumns = dataSchema.size();

    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    ByteBuffer fixedSize = ByteBuffer.allocate(fixedBytesRequired);
    CompoundDataBuffer.Builder varBufferBuilder = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN, true);

    try (PagedPinotOutputStream varSize = new PagedPinotOutputStream(allocator)) {
      for (int colId = 0; colId < numColumns; colId++) {
        RoaringBitmap nullBitmap = new RoaringBitmap();
        nullBitmaps[colId] = nullBitmap;
        serializeColumnData(columns, dataSchema, colId, fixedSize, varSize, nullBitmap, dictionary);
      }
      varBufferBuilder.addPagedOutputStream(varSize);
    }
    // Write null bitmaps after writing data.
    setNullRowIds(nullBitmaps, fixedSize, varBufferBuilder);
    return buildColumnarBlock(numRows, dataSchema, getReverseDictionary(dictionary), fixedSize, varBufferBuilder);
  }

  private static void serializeColumnData(List<Object[]> columns, DataSchema dataSchema, int colId,
      ByteBuffer fixedSize, PagedPinotOutputStream varSize, RoaringBitmap nullBitmap,
      Object2IntOpenHashMap<String> dictionary)
      throws IOException {
    ColumnDataType storedType = dataSchema.getColumnDataType(colId).getStoredType();
    int numRows = columns.get(colId).length;

    Object[] column = columns.get(colId);

    // NOTE:
    // We intentionally make the type casting very strict here (e.g. only accepting Integer for INT) to ensure the
    // rows conform to the data schema. This can help catch the unexpected data type issues early.
    switch (storedType) {
      // Single-value column
      case INT: {
        int nullPlaceholder = (int) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            fixedSize.putInt(nullPlaceholder);
          } else {
            fixedSize.putInt((int) value);
          }
        }
        break;
      }
      case LONG: {
        long nullPlaceholder = (long) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            fixedSize.putLong(nullPlaceholder);
          } else {
            fixedSize.putLong((long) value);
          }
        }
        break;
      }
      case FLOAT: {
        float nullPlaceholder = (float) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            fixedSize.putFloat(nullPlaceholder);
          } else {
            fixedSize.putFloat((float) value);
          }
        }
        break;
      }
      case DOUBLE: {
        double nullPlaceholder = (double) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            fixedSize.putDouble(nullPlaceholder);
          } else {
            fixedSize.putDouble((double) value);
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal nullPlaceholder = (BigDecimal) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder);
          } else {
            setColumn(fixedSize, varSize, (BigDecimal) value);
          }
        }
        break;
      }
      case STRING: {
        ToIntFunction<String> didSupplier = k -> dictionary.size();
        int nullPlaceHolder = dictionary.computeIfAbsent((String) storedType.getNullPlaceholder(), didSupplier);

        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            fixedSize.putInt(nullPlaceHolder);
          } else {
            int dictId = dictionary.computeIfAbsent((String) value, didSupplier);
            fixedSize.putInt(dictId);
          }
        }
        break;
      }
      case BYTES: {
        ByteArray nullPlaceholder = (ByteArray) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder);
          } else {
            setColumn(fixedSize, varSize, (ByteArray) value);
          }
        }
        break;
      }
      case MAP: {
        Map nullPlaceholder = (Map) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder);
          } else {
            setColumn(fixedSize, varSize, (Map) value);
          }
        }
        break;
      }
      // Multi-value column
      case INT_ARRAY: {
        int[] nullPlaceholder = (int[]) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder);
          } else {
            setColumn(fixedSize, varSize, (int[]) value);
          }
        }
        break;
      }
      case LONG_ARRAY: {
        long[] nullPlaceholder = (long[]) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder);
          } else {
            setColumn(fixedSize, varSize, (long[]) value);
          }
        }
        break;
      }
      case FLOAT_ARRAY: {
        float[] nullPlaceholder = (float[]) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder);
          } else {
            setColumn(fixedSize, varSize, (float[]) value);
          }
        }
        break;
      }
      case DOUBLE_ARRAY: {
        double[] nullPlaceholder = (double[]) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder);
          } else {
            setColumn(fixedSize, varSize, (double[]) value);
          }
        }
        break;
      }
      case STRING_ARRAY: {
        String[] nullPlaceholder = (String[]) storedType.getNullPlaceholder();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object value = column[rowId];
          if (value == null) {
            nullBitmap.add(rowId);
            setColumn(fixedSize, varSize, nullPlaceholder, dictionary);
          } else {
            setColumn(fixedSize, varSize, (String[]) value, dictionary);
          }
        }
        break;
      }

      // Special intermediate result for aggregation function
      case OBJECT: {
        for (int rowId = 0; rowId < numRows; rowId++) {
          setColumn(fixedSize, varSize, column[rowId]);
        }
        break;
      }
      // Null
      case UNKNOWN:
        for (int rowId = 0; rowId < numRows; rowId++) {
          setColumn(fixedSize, varSize, (Object) null);
        }
        break;

      default:
        throw new IllegalStateException("Unsupported stored type: " + storedType + " for column: "
            + dataSchema.getColumnName(colId));
    }
  }

  private static int calculateBytesPerRow(DataSchema dataSchema) {
    int rowSizeInBytes = 0;
    for (ColumnDataType columnDataType : dataSchema.getColumnDataTypes()) {
      switch (columnDataType) {
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

  private static void writeVarOffsetInFixed(ByteBuffer fixedSize, PagedPinotOutputStream varSize) {
    long offsetInVar = varSize.getCurrentOffset();
    Preconditions.checkState(offsetInVar <= Integer.MAX_VALUE,
        "Cannot handle variable size output stream larger than 2GB");
    fixedSize.putInt((int) offsetInVar);
  }

  private static void setNullRowIds(RoaringBitmap[] nullVectors, ByteBuffer fixedSize,
      CompoundDataBuffer.Builder varBufferBuilder)
      throws IOException {
    int varBufSize = Arrays.stream(nullVectors)
        .mapToInt(bitmap -> bitmap == null ? 0 : bitmap.serializedSizeInBytes())
        .sum();
    ByteBuffer variableSize = ByteBuffer.allocate(varBufSize)
        .order(ByteOrder.BIG_ENDIAN);

    long varWrittenBytes = varBufferBuilder.getWrittenBytes();
    Preconditions.checkArgument(varWrittenBytes < Integer.MAX_VALUE,
        "Cannot handle variable size output stream larger than 2GB but found {} written bytes", varWrittenBytes);
    int startVariableOffset = (int) varWrittenBytes;
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
    varBufferBuilder.addBuffer(variableSize);
  }

  private static RowDataBlock buildRowBlock(int numRows, DataSchema dataSchema, String[] dictionary,
      ByteBuffer fixedSize, CompoundDataBuffer.Builder varBufferBuilder) {
    return new RowDataBlock(numRows, dataSchema, dictionary, PinotByteBuffer.wrap(fixedSize), varBufferBuilder.build());
  }

  private static ColumnarDataBlock buildColumnarBlock(int numRows, DataSchema dataSchema, String[] dictionary,
      ByteBuffer fixedSize, CompoundDataBuffer.Builder varBufferBuilder) {
    return new ColumnarDataBlock(numRows, dataSchema, dictionary,
        PinotByteBuffer.wrap(fixedSize), varBufferBuilder.build());
  }

  private static String[] getReverseDictionary(Object2IntOpenHashMap<String> dictionary) {
    String[] reverseDictionary = new String[dictionary.size()];
    for (Object2IntMap.Entry<String> entry : dictionary.object2IntEntrySet()) {
      reverseDictionary[entry.getIntValue()] = entry.getKey();
    }
    return reverseDictionary;
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, BigDecimal value)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    // This can be slightly optimized with a specialized serialization method
    byte[] bytes = BigDecimalUtils.serialize(value);
    fixedSize.putInt(bytes.length);
    varSize.write(bytes);
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, ByteArray value)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    byte[] bytes = value.getBytes();
    fixedSize.putInt(bytes.length);
    varSize.write(bytes);
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, Map value)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    byte[] bytes = MapUtils.serializeMap(value);
    fixedSize.putInt(bytes.length);
    varSize.write(bytes);
  }

  // TODO: Move ser/de into AggregationFunction interface
  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, @Nullable Object value)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    if (value == null) {
      fixedSize.putInt(0);
      varSize.writeInt(CustomObject.NULL_TYPE_VALUE);
    } else {
      int objectTypeValue = ObjectSerDeUtils.ObjectType.getObjectType(value).getValue();
      byte[] bytes = ObjectSerDeUtils.serialize(value, objectTypeValue);
      fixedSize.putInt(bytes.length);
      varSize.writeInt(objectTypeValue);
      varSize.write(bytes);
    }
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, int[] values)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    fixedSize.putInt(values.length);
    for (int value : values) {
      varSize.writeInt(value);
    }
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, long[] values)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    fixedSize.putInt(values.length);
    for (long value : values) {
      varSize.writeLong(value);
    }
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, float[] values)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    fixedSize.putInt(values.length);
    for (float value : values) {
      varSize.writeFloat(value);
    }
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, double[] values)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    fixedSize.putInt(values.length);
    for (double value : values) {
      varSize.writeDouble(value);
    }
  }

  private static void setColumn(ByteBuffer fixedSize, PagedPinotOutputStream varSize, String[] values,
      Object2IntOpenHashMap<String> dictionary)
      throws IOException {
    writeVarOffsetInFixed(fixedSize, varSize);
    fixedSize.putInt(values.length);
    for (String value : values) {
      int dictId = dictionary.computeIfAbsent(value, k -> dictionary.size());
      varSize.writeInt(dictId);
    }
  }
}
