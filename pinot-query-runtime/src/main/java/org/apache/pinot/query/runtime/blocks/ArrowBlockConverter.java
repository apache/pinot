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
package org.apache.pinot.query.runtime.blocks;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.roaringbitmap.RoaringBitmap;


/**
 * Converts a legacy {@link DataBlock} (row-heap or serialized columnar) into an {@link ArrowBlock}.
 *
 * <p>Each column is extracted using {@link DataBlockExtractUtils} and written column-by-column into the appropriate
 * Arrow vector. String columns are stored as plain {@code VarCharVector} (no dictionary encoding) for simplicity;
 * the {@link org.apache.pinot.query.runtime.operator.join.ArrowLookupTable} decodes dictionaries during the merge
 * phase anyway.
 */
public final class ArrowBlockConverter {
  private ArrowBlockConverter() {
  }

  /**
   * Converts the given {@link MseBlock.Data} to an {@link ArrowBlock}.
   * If the block is already an {@link ArrowBlock}, returns it unchanged.
   *
   * @param allocator the Arrow allocator to use for new off-heap buffers
   */
  public static ArrowBlock toArrowBlock(MseBlock.Data block, BufferAllocator allocator) {
    if (block instanceof ArrowBlock) {
      return (ArrowBlock) block;
    }
    DataBlock dataBlock = block.asSerialized().getDataBlock();
    DataSchema schema = block.getDataSchema();
    return fromDataBlock(dataBlock, schema, allocator);
  }

  /**
   * Converts a raw {@link DataBlock} with the given schema into an {@link ArrowBlock}.
   *
   * @param allocator the Arrow allocator to use for new off-heap buffers
   */
  public static ArrowBlock fromDataBlock(DataBlock dataBlock, DataSchema schema, BufferAllocator allocator) {
    int numRows = dataBlock.getNumberOfRows();
    int numCols = schema.size();
    ColumnDataType[] storedTypes = schema.getStoredColumnDataTypes();

    // Build a non-dictionary Arrow schema (plain VarChar for strings)
    Schema arrowSchema = buildPlainSchema(schema);
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);

    for (int colId = 0; colId < numCols; colId++) {
      FieldVector vector = root.getVector(colId);
      RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
      writeColumn(dataBlock, vector, storedTypes[colId], colId, numRows, nullBitmap);
    }
    root.setRowCount(numRows);
    return new ArrowBlock(new ArrowDataBlock(root));
  }

  // ----- schema builder (plain VarChar, no dictionary) -----

  private static Schema buildPlainSchema(DataSchema schema) {
    List<Field> fields = new ArrayList<>(schema.size());
    String[] names = schema.getColumnNames();
    ColumnDataType[] types = schema.getColumnDataTypes();
    for (int i = 0; i < names.length; i++) {
      fields.add(buildPlainField(names[i], types[i]));
    }
    return new Schema(fields);
  }

  private static Field buildPlainField(String name, ColumnDataType type) {
    switch (type) {
      case BOOLEAN:
        return Field.nullable(name, new ArrowType.Bool());
      case INT:
        return Field.nullable(name, new ArrowType.Int(32, true));
      case LONG:
      case TIMESTAMP:
        return Field.nullable(name, new ArrowType.Int(64, true));
      case FLOAT:
        return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
      case DOUBLE:
        return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
      case STRING:
      case JSON:
      case BIG_DECIMAL:
        return Field.nullable(name, new ArrowType.Utf8());
      case BYTES:
      case MAP:
      case OBJECT:
        return Field.nullable(name, new ArrowType.Binary());
      default:
        return Field.nullable(name, new ArrowType.Null());
    }
  }

  // ----- column writers -----

  private static void writeColumn(DataBlock dataBlock, FieldVector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    vector.setInitialCapacity(numRows);
    vector.allocateNew();

    if (numRows == 0) {
      vector.setValueCount(0);
      return;
    }

    switch (storedType) {
      case BOOLEAN:
      case INT:
        writeIntColumn(dataBlock, (IntVector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case LONG:
      case TIMESTAMP:
        writeLongColumn(dataBlock, (BigIntVector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case FLOAT:
        writeFloatColumn(dataBlock, (Float4Vector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case DOUBLE:
        writeDoubleColumn(dataBlock, (Float8Vector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case STRING:
      case JSON:
      case BIG_DECIMAL:
        writeStringColumn(dataBlock, (VarCharVector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case BYTES:
        writeBytesColumn(dataBlock, (VarBinaryVector) vector, colId, numRows, nullBitmap);
        break;
      default:
        // UNKNOWN / unsupported — leave as null vector
        vector.setValueCount(numRows);
        break;
    }
  }

  private static void writeIntColumn(DataBlock dataBlock, IntVector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    // Fast path: for ColumnarDataBlock without nulls, read directly from the backing buffer
    // with inline BIG_ENDIAN → LITTLE_ENDIAN byte swap. Eliminates the intermediate int[] heap array.
    if (dataBlock instanceof ColumnarDataBlock && nullBitmap == null
        && storedType == ColumnDataType.INT) {
      ColumnarDataBlock colBlock = (ColumnarDataBlock) dataBlock;
      DataBuffer fixedData = colBlock.getFixedData();
      int byteOffset = colBlock.getColumnByteOffset(colId);
      ArrowBuf dstBuf = vector.getDataBuffer();
      for (int row = 0; row < numRows; row++) {
        dstBuf.setInt((long) row * 4,
            Integer.reverseBytes(fixedData.getInt(byteOffset + (long) row * 4)));
      }
      setAllValid(vector, numRows);
      vector.setValueCount(numRows);
      return;
    }
    // Fallback: extract to heap array then write
    int[] values = DataBlockExtractUtils.extractIntColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    for (int row = 0; row < numRows; row++) {
      if (nullBitmap != null && nullBitmap.contains(row)) {
        vector.setNull(row);
      } else {
        vector.set(row, values[row]);
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeLongColumn(DataBlock dataBlock, BigIntVector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    // Fast path for ColumnarDataBlock without nulls
    if (dataBlock instanceof ColumnarDataBlock && nullBitmap == null
        && storedType == ColumnDataType.LONG) {
      ColumnarDataBlock colBlock = (ColumnarDataBlock) dataBlock;
      DataBuffer fixedData = colBlock.getFixedData();
      int byteOffset = colBlock.getColumnByteOffset(colId);
      ArrowBuf dstBuf = vector.getDataBuffer();
      for (int row = 0; row < numRows; row++) {
        dstBuf.setLong((long) row * 8,
            Long.reverseBytes(fixedData.getLong(byteOffset + (long) row * 8)));
      }
      setAllValid(vector, numRows);
      vector.setValueCount(numRows);
      return;
    }
    long[] values = DataBlockExtractUtils.extractLongColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    for (int row = 0; row < numRows; row++) {
      if (nullBitmap != null && nullBitmap.contains(row)) {
        vector.setNull(row);
      } else {
        vector.set(row, values[row]);
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeFloatColumn(DataBlock dataBlock, Float4Vector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    // Fast path: byte-swap INT representation of float (float and int have same 4-byte layout)
    if (dataBlock instanceof ColumnarDataBlock && nullBitmap == null
        && storedType == ColumnDataType.FLOAT) {
      ColumnarDataBlock colBlock = (ColumnarDataBlock) dataBlock;
      DataBuffer fixedData = colBlock.getFixedData();
      int byteOffset = colBlock.getColumnByteOffset(colId);
      ArrowBuf dstBuf = vector.getDataBuffer();
      for (int row = 0; row < numRows; row++) {
        dstBuf.setInt((long) row * 4,
            Integer.reverseBytes(fixedData.getInt(byteOffset + (long) row * 4)));
      }
      setAllValid(vector, numRows);
      vector.setValueCount(numRows);
      return;
    }
    float[] values = DataBlockExtractUtils.extractFloatColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    for (int row = 0; row < numRows; row++) {
      if (nullBitmap != null && nullBitmap.contains(row)) {
        vector.setNull(row);
      } else {
        vector.set(row, values[row]);
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeDoubleColumn(DataBlock dataBlock, Float8Vector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    // Fast path: byte-swap LONG representation of double (double and long have same 8-byte layout)
    if (dataBlock instanceof ColumnarDataBlock && nullBitmap == null
        && storedType == ColumnDataType.DOUBLE) {
      ColumnarDataBlock colBlock = (ColumnarDataBlock) dataBlock;
      DataBuffer fixedData = colBlock.getFixedData();
      int byteOffset = colBlock.getColumnByteOffset(colId);
      ArrowBuf dstBuf = vector.getDataBuffer();
      for (int row = 0; row < numRows; row++) {
        dstBuf.setLong((long) row * 8,
            Long.reverseBytes(fixedData.getLong(byteOffset + (long) row * 8)));
      }
      setAllValid(vector, numRows);
      vector.setValueCount(numRows);
      return;
    }
    double[] values = DataBlockExtractUtils.extractDoubleColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    for (int row = 0; row < numRows; row++) {
      if (nullBitmap != null && nullBitmap.contains(row)) {
        vector.setNull(row);
      } else {
        vector.set(row, values[row]);
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeStringColumn(DataBlock dataBlock, VarCharVector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    String[] values = DataBlockExtractUtils.extractStringColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    for (int row = 0; row < numRows; row++) {
      if (nullBitmap != null && nullBitmap.contains(row)) {
        vector.setNull(row);
      } else if (values[row] != null) {
        vector.setSafe(row, values[row].getBytes(StandardCharsets.UTF_8));
      } else {
        vector.setNull(row);
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeBytesColumn(DataBlock dataBlock, VarBinaryVector vector, int colId,
      int numRows, @Nullable RoaringBitmap nullBitmap) {
    for (int row = 0; row < numRows; row++) {
      if (nullBitmap != null && nullBitmap.contains(row)) {
        vector.setNull(row);
      } else {
        byte[] bytes = dataBlock.getBytes(row, colId).getBytes();
        vector.setSafe(row, bytes);
      }
    }
    vector.setValueCount(numRows);
  }

  /**
   * Sets all validity bits to 1 (non-null) for the given vector. Used by the fast-path writers that bypass
   * per-row {@code set()} calls and instead write directly to the data buffer.
   */
  private static void setAllValid(FieldVector vector, int numRows) {
    ArrowBuf validityBuf = vector.getValidityBuffer();
    int fullBytes = numRows / 8;
    for (int i = 0; i < fullBytes; i++) {
      validityBuf.setByte(i, 0xFF);
    }
    int remaining = numRows % 8;
    if (remaining > 0) {
      validityBuf.setByte(fullBytes, (1 << remaining) - 1);
    }
  }
}
