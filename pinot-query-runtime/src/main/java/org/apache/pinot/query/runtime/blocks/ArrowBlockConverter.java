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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
 * Arrow vector. STRING/JSON columns are dictionary-encoded (an integer index vector plus a per-column dictionary
 * held in a {@link MapDictionaryProvider}) so low-cardinality columns avoid storing repeated values; an all-distinct
 * column falls back to a plain {@code VarCharVector} since a dictionary would only add overhead there.
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
    ColumnDataType[] columnTypes = schema.getColumnDataTypes();
    ColumnDataType[] storedTypes = schema.getStoredColumnDataTypes();

    // Build each column vector explicitly (rather than VectorSchemaRoot.create) so STRING/JSON columns can
    // be dictionary-encoded. On any failure mid-build, close the vectors built so far and the dictionary
    // provider so partially-allocated off-heap buffers don't leak for the lifetime of the allocator.
    List<FieldVector> vectors = new ArrayList<>(numCols);
    MapDictionaryProvider dictionaryProvider = new MapDictionaryProvider();
    boolean success = false;
    try {
      for (int colId = 0; colId < numCols; colId++) {
        RoaringBitmap nullBitmap = dataBlock.getNullRowIds(colId);
        String columnName = schema.getColumnName(colId);
        ColumnDataType columnType = columnTypes[colId];
        if (columnType == ColumnDataType.STRING || columnType == ColumnDataType.JSON) {
          vectors.add(buildStringColumn(dataBlock, columnName, colId, numRows, nullBitmap, dictionaryProvider,
              allocator));
        } else {
          FieldVector vector = buildArrowField(columnName, columnType).createVector(allocator);
          writeColumn(dataBlock, vector, storedTypes[colId], colId, numRows, nullBitmap);
          vectors.add(vector);
        }
      }
      VectorSchemaRoot root = new VectorSchemaRoot(vectors);
      root.setRowCount(numRows);
      ArrowBlock block = new ArrowBlock(new ArrowDataBlock(root, schema, dictionaryProvider));
      success = true;
      return block;
    } finally {
      if (!success) {
        for (FieldVector vector : vectors) {
          vector.close();
        }
        dictionaryProvider.close();
      }
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
      case BYTES:
        writeBytesColumn(dataBlock, (VarBinaryVector) vector, colId, numRows, nullBitmap);
        break;
      default:
        // STRING/JSON are dictionary-encoded by buildStringColumn and never reach here; anything else was
        // already rejected by buildArrowField.
        throw new UnsupportedOperationException("Arrow block conversion does not support type " + storedType);
    }
  }

  private static void writeBitColumn(DataBlock dataBlock, BitVector vector, ColumnDataType storedType, int colId,
      int numRows, @Nullable RoaringBitmap nullBitmap) {
    int[] values = DataBlockExtractUtils.extractIntColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    // Hoist the null check out of the loop so the no-null fast path is a tight, autovectorizable loop.
    if (nullBitmap == null) {
      for (int row = 0; row < numRows; row++) {
        vector.set(row, values[row]);
      }
    } else {
      for (int row = 0; row < numRows; row++) {
        if (nullBitmap.contains(row)) {
          vector.setNull(row);
        } else {
          vector.set(row, values[row]);
        }
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeIntColumn(DataBlock dataBlock, IntVector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    int[] values = DataBlockExtractUtils.extractIntColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    // Hoist the null check out of the loop so the no-null fast path is a tight, autovectorizable loop.
    if (nullBitmap == null) {
      for (int row = 0; row < numRows; row++) {
        vector.set(row, values[row]);
      }
    } else {
      for (int row = 0; row < numRows; row++) {
        if (nullBitmap.contains(row)) {
          vector.setNull(row);
        } else {
          vector.set(row, values[row]);
        }
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeFloatColumn(DataBlock dataBlock, Float4Vector vector,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    float[] values = DataBlockExtractUtils.extractFloatColumn(
        ColumnDataType.FLOAT.toDataType(), dataBlock, colId, nullBitmap);
    // Hoist the null check out of the loop so the no-null fast path is a tight, autovectorizable loop.
    if (nullBitmap == null) {
      for (int row = 0; row < numRows; row++) {
        vector.set(row, values[row]);
      }
    } else {
      for (int row = 0; row < numRows; row++) {
        if (nullBitmap.contains(row)) {
          vector.setNull(row);
        } else {
          vector.set(row, values[row]);
        }
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeLongColumn(DataBlock dataBlock, BigIntVector vector, ColumnDataType storedType,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    long[] values = DataBlockExtractUtils.extractLongColumn(storedType.toDataType(), dataBlock, colId, nullBitmap);
    // Hoist the null check out of the loop so the no-null fast path is a tight, autovectorizable loop.
    if (nullBitmap == null) {
      for (int row = 0; row < numRows; row++) {
        vector.set(row, values[row]);
      }
    } else {
      for (int row = 0; row < numRows; row++) {
        if (nullBitmap.contains(row)) {
          vector.setNull(row);
        } else {
          vector.set(row, values[row]);
        }
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeDoubleColumn(DataBlock dataBlock, Float8Vector vector,
      int colId, int numRows, @Nullable RoaringBitmap nullBitmap) {
    double[] values = DataBlockExtractUtils.extractDoubleColumn(
        ColumnDataType.DOUBLE.toDataType(), dataBlock, colId, nullBitmap);
    // Hoist the null check out of the loop so the no-null fast path is a tight, autovectorizable loop.
    if (nullBitmap == null) {
      for (int row = 0; row < numRows; row++) {
        vector.set(row, values[row]);
      }
    } else {
      for (int row = 0; row < numRows; row++) {
        if (nullBitmap.contains(row)) {
          vector.setNull(row);
        } else {
          vector.set(row, values[row]);
        }
      }
    }
    vector.setValueCount(numRows);
  }

  /**
   * Builds the Arrow vector for a STRING/JSON column. The column is dictionary-encoded — an integer index
   * {@link IntVector} whose field carries a {@link DictionaryEncoding}, with the distinct values registered
   * in {@code dictionaryProvider} under a per-column id ({@code colId}, unique within the block). An
   * all-distinct (or all-null/empty) column is stored as a plain {@link VarCharVector} instead, since a
   * dictionary would only add an index vector on top of the same values. Returns the vector to place in the
   * {@link VectorSchemaRoot}; the read path branches on the field's encoding.
   */
  private static FieldVector buildStringColumn(DataBlock dataBlock, String name, int colId, int numRows,
      @Nullable RoaringBitmap nullBitmap, MapDictionaryProvider dictionaryProvider, BufferAllocator allocator) {
    String[] values =
        DataBlockExtractUtils.extractStringColumn(ColumnDataType.STRING.toDataType(), dataBlock, colId, nullBitmap);

    // Map each distinct non-null value to a dictionary index, in first-seen order.
    Map<String, Integer> valueToIndex = new LinkedHashMap<>();
    for (int row = 0; row < numRows; row++) {
      if (nullBitmap != null && nullBitmap.contains(row)) {
        continue;
      }
      valueToIndex.computeIfAbsent(values[row], k -> valueToIndex.size());
    }
    int distinct = valueToIndex.size();

    if (distinct == numRows || distinct == 0) {
      // All-distinct (dictionary is pure overhead) or all-null/empty: store a plain VarCharVector.
      VarCharVector plain = new VarCharVector(name, allocator);
      try {
        plain.setInitialCapacity(numRows);
        plain.allocateNew();
        writeVarChar(plain, values, numRows, nullBitmap);
        return plain;
      } catch (Throwable t) {
        plain.close();
        throw t;
      }
    }

    VarCharVector dictionaryVector = new VarCharVector(name + "_dict", allocator);
    boolean dictionaryRegistered = false;
    IntVector indices = null;
    try {
      dictionaryVector.setInitialCapacity(distinct);
      dictionaryVector.allocateNew();
      for (Map.Entry<String, Integer> entry : valueToIndex.entrySet()) {
        dictionaryVector.setSafe(entry.getValue(), entry.getKey().getBytes(StandardCharsets.UTF_8));
      }
      dictionaryVector.setValueCount(distinct);

      // colId is unique within the block, so each dictionary-encoded column gets a distinct dictionary id —
      // this is what avoids the apache/pinot#18207 bug where every column shared dictionary id 0.
      DictionaryEncoding encoding = new DictionaryEncoding(colId, false, new ArrowType.Int(32, true));
      dictionaryProvider.put(new Dictionary(dictionaryVector, encoding));
      dictionaryRegistered = true;

      indices = (IntVector) new Field(name, new FieldType(true, new ArrowType.Int(32, true), encoding), null)
          .createVector(allocator);
      indices.allocateNew(numRows);
      if (nullBitmap == null) {
        for (int row = 0; row < numRows; row++) {
          indices.set(row, valueToIndex.get(values[row]));
        }
      } else {
        for (int row = 0; row < numRows; row++) {
          if (nullBitmap.contains(row)) {
            indices.setNull(row);
          } else {
            indices.set(row, valueToIndex.get(values[row]));
          }
        }
      }
      indices.setValueCount(numRows);
      return indices;
    } catch (Throwable t) {
      if (indices != null) {
        indices.close();
      }
      // Once registered, the dictionary vector is owned by the provider and closed by the caller's cleanup.
      if (!dictionaryRegistered) {
        dictionaryVector.close();
      }
      throw t;
    }
  }

  private static void writeVarChar(VarCharVector vector, String[] values, int numRows,
      @Nullable RoaringBitmap nullBitmap) {
    if (nullBitmap == null) {
      for (int row = 0; row < numRows; row++) {
        vector.setSafe(row, values[row].getBytes(StandardCharsets.UTF_8));
      }
    } else {
      for (int row = 0; row < numRows; row++) {
        if (nullBitmap.contains(row)) {
          vector.setNull(row);
        } else {
          vector.setSafe(row, values[row].getBytes(StandardCharsets.UTF_8));
        }
      }
    }
    vector.setValueCount(numRows);
  }

  private static void writeBytesColumn(DataBlock dataBlock, VarBinaryVector vector, int colId,
      int numRows, @Nullable RoaringBitmap nullBitmap) {
    if (nullBitmap == null) {
      for (int row = 0; row < numRows; row++) {
        vector.setSafe(row, dataBlock.getBytes(row, colId).getBytes());
      }
    } else {
      for (int row = 0; row < numRows; row++) {
        if (nullBitmap.contains(row)) {
          vector.setNull(row);
        } else {
          vector.setSafe(row, dataBlock.getBytes(row, colId).getBytes());
        }
      }
    }
    vector.setValueCount(numRows);
  }
}
