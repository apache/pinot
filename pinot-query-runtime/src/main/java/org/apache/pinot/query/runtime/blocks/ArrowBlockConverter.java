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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
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
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * Converts a legacy {@link DataBlock} (row-heap or serialized columnar) into an {@link ArrowBlock}.
 *
 * <p>Each column is extracted using {@link DataBlockExtractUtils} and written column-by-column into the appropriate
 * Arrow vector. String columns are stored as plain {@code VarCharVector} (no dictionary encoding) for simplicity.
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
    DataSchema schema = block.getDataSchema();
    // Validate the schema before forcing serialization — a RowHeapDataBlock with an unsupported
    // column (MAP, BIG_DECIMAL, etc.) should fail fast without paying asSerialized()'s full
    // row-to-buffer materialization cost.
    buildArrowSchema(schema);
    DataBlock dataBlock = block.asSerialized().getDataBlock();
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

    Schema arrowSchema = buildArrowSchema(schema);
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
    // Close the root on any failure during population so partially-allocated off-heap buffers
    // don't leak for the lifetime of the allocator.
    try {
      for (int colId = 0; colId < numCols; colId++) {
        FieldVector vector = root.getVector(colId);
        RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
        writeColumn(dataBlock, vector, storedTypes[colId], colId, numRows, nullBitmap);
      }
      root.setRowCount(numRows);
      return new ArrowBlock(new ArrowDataBlock(root, schema));
    } catch (Throwable t) {
      root.close();
      throw t;
    }
  }

  private static Schema buildArrowSchema(DataSchema schema) {
    List<Field> fields = new ArrayList<>(schema.size());
    String[] names = schema.getColumnNames();
    ColumnDataType[] types = schema.getColumnDataTypes();
    for (int i = 0; i < names.length; i++) {
      fields.add(buildArrowField(names[i], types[i]));
    }
    return new Schema(fields);
  }

  private static Field buildArrowField(String name, ColumnDataType type) {
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
        return Field.nullable(name, new ArrowType.Utf8());
      case BYTES:
        return Field.nullable(name, new ArrowType.Binary());
      default:
        throw new UnsupportedOperationException(
            "Arrow block conversion does not yet support column type " + type
                + " (column '" + name + "')");
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

    // BOOLEAN columns are backed by BitVector while their stored type is INT; handle them before the
    // storedType dispatch so we don't ClassCastException casting a BitVector to IntVector.
    if (vector instanceof BitVector) {
      writeBitColumn(dataBlock, (BitVector) vector, storedType, colId, numRows, nullBitmap);
      return;
    }
    switch (storedType) {
      case INT:
        writeIntColumn(dataBlock, (IntVector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case FLOAT:
        writeFloatColumn(dataBlock, (Float4Vector) vector, colId, numRows, nullBitmap);
        break;
      case LONG:
      case TIMESTAMP:
        writeLongColumn(dataBlock, (BigIntVector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case DOUBLE:
        writeDoubleColumn(dataBlock, (Float8Vector) vector, colId, numRows, nullBitmap);
        break;
      case STRING:
      case JSON:
        writeStringColumn(dataBlock, (VarCharVector) vector, storedType, colId, numRows, nullBitmap);
        break;
      case BYTES:
        writeBytesColumn(dataBlock, (VarBinaryVector) vector, colId, numRows, nullBitmap);
        break;
      default:
        // Should never be reached — buildArrowField already rejects unsupported types.
        throw new UnsupportedOperationException("Arrow block conversion does not support type " + storedType);
    }
  }

  private static void writeBitColumn(DataBlock dataBlock, BitVector vector, ColumnDataType storedType, int colId,
      int numRows, @Nullable RoaringBitmap nullBitmap) {
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

  private static void writeIntColumn(DataBlock dataBlock, IntVector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
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

  private static void writeFloatColumn(DataBlock dataBlock, Float4Vector vector,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    float[] values = DataBlockExtractUtils.extractFloatColumn(
        ColumnDataType.FLOAT.toDataType(), dataBlock, colId, nullBitmap);
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

  private static void writeDoubleColumn(DataBlock dataBlock, Float8Vector vector,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    double[] values = DataBlockExtractUtils.extractDoubleColumn(
        ColumnDataType.DOUBLE.toDataType(), dataBlock, colId, nullBitmap);
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
      } else {
        vector.setSafe(row, values[row].getBytes(StandardCharsets.UTF_8));
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
}
