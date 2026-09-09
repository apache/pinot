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
package org.apache.pinot.query.runtime.operator.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * Single-threaded column gathers into independently owned Arrow output, without row objects or boxed cells.
 * Low-cardinality string payloads retain encoding with compact, independently owned output dictionaries.
 */
public final class ArrowJoinOutput {
  // Amortize vector setup across duplicate fanout without changing key-scan cancellation intervals.
  public static final int MAX_ROWS_PER_BLOCK = 4 * ArrowJoinKeys.BATCH_SIZE;

  private final DataSchema _resultSchema;
  private final Schema _arrowSchema;
  private final ArrowQueryContext _context;
  private final Runnable _checkTermination;
  private final Column[] _buildColumns;
  @Nullable
  private Column[] _probeColumns;
  private final int[] _probeBlockIds;
  private final long[] _dictionaryValues;

  public ArrowJoinOutput(DataSchema resultSchema, DataSchema rightSchema, List<ArrowBlock> buildBlocks,
      ArrowQueryContext context, Runnable checkTermination) {
    this(resultSchema, context, checkTermination, columns(rightSchema, buildBlocks, checkTermination),
        MAX_ROWS_PER_BLOCK);
  }

  private ArrowJoinOutput(DataSchema resultSchema, ArrowQueryContext context, Runnable checkTermination,
      Column[] buildColumns, int capacity) {
    _resultSchema = resultSchema;
    _arrowSchema = ArrowJoinSupport.arrowSchema(resultSchema);
    _context = context;
    _buildColumns = buildColumns;
    _checkTermination = checkTermination;
    _probeBlockIds = new int[capacity];
    _dictionaryValues = new long[capacity];
  }

  public void setProbe(ArrowBlock block) {
    _probeColumns = columns(block.getDataSchema(), List.of(block), _checkTermination);
  }

  public void clearProbe() {
    _probeColumns = null;
  }

  public ArrowBlock gather(int[] probeRows, int[] buildBlockIds, int[] buildRows, int count, boolean unmatched) {
    Column[] probeColumns = _probeColumns;
    if (probeColumns == null) {
      throw new IllegalStateException("No probe block");
    }
    List<Field> fields = new ArrayList<>(_arrowSchema.getFields());
    boolean encoded = false;
    for (int col = 0; col < fields.size(); col++) {
      Column column = col < probeColumns.length ? probeColumns[col] : _buildColumns[col - probeColumns.length];
      if (col < probeColumns.length ? column.canEncode(_probeBlockIds, probeRows, count)
          : column.canEncode(buildBlockIds, buildRows, count)) {
        Field field = fields.get(col);
        ArrowType.Int indexType = new ArrowType.Int(32, true);
        fields.set(col, new Field(field.getName(),
            new FieldType(field.isNullable(), indexType, new DictionaryEncoding(col, false, indexType),
                field.getMetadata()), null));
        encoded = true;
      }
    }
    VectorSchemaRoot root = VectorSchemaRoot.create(encoded ? new Schema(fields) : _arrowSchema,
        _context.getAllocator());
    MapDictionaryProvider dictionaries = encoded ? new MapDictionaryProvider() : null;
    boolean transferred = false;
    try {
      for (int col = 0; col < _resultSchema.size(); col++) {
        FieldVector target = root.getVector(col);
        if (target.getField().getDictionary() != null) {
          if (col < probeColumns.length) {
            probeColumns[col].copyDictionary((IntVector) target, _probeBlockIds, probeRows, count, dictionaries,
                _dictionaryValues);
          } else {
            _buildColumns[col - probeColumns.length].copyDictionary((IntVector) target, buildBlockIds, buildRows,
                count, dictionaries, _dictionaryValues);
          }
        } else if (col < probeColumns.length) {
          probeColumns[col].copy(target, _probeBlockIds, probeRows, count, false);
        } else {
          _buildColumns[col - probeColumns.length].copy(target, buildBlockIds, buildRows, count, unmatched);
        }
        target.setValueCount(count);
      }
      root.setRowCount(count);
      ArrowBlock block = _context.createBlock(new ArrowDataBlock(root, _resultSchema, dictionaries));
      transferred = true;
      return block;
    } finally {
      if (!transferred) {
        try {
          root.close();
        } finally {
          if (dictionaries != null) {
            dictionaries.close();
          }
        }
      }
    }
  }

  /**
   * Copies only accepted build rows on BREAK, so the rejected suffix is not retained in join state.
   */
  public static ArrowBlock copyPrefix(ArrowBlock block, int count, ArrowQueryContext context,
      Runnable checkTermination) {
    ArrowJoinOutput output =
        new ArrowJoinOutput(block.getDataSchema(), context, checkTermination, new Column[0], count);
    output.setProbe(block);
    int[] rows = new int[count];
    for (int start = 0; start < count; start += ArrowJoinKeys.BATCH_SIZE) {
      checkTermination.run();
      int end = Math.min(start + ArrowJoinKeys.BATCH_SIZE, count);
      for (int row = start; row < end; row++) {
        rows[row] = row;
      }
    }
    return output.gather(rows, new int[0], new int[0], count, false);
  }

  private static Column[] columns(DataSchema schema, List<ArrowBlock> blocks, Runnable checkTermination) {
    Column[] columns = new Column[schema.size()];
    for (int col = 0; col < columns.length; col++) {
      columns[col] = new Column(schema.getColumnDataType(col), blocks, col, checkTermination);
    }
    return columns;
  }

  private static final class Column {
    private final ColumnDataType _type;
    private final ArrowBuf[] _data;
    private final ArrowBuf[] _validity;
    private final FieldVector[] _vectors;
    private final IntVector[] _dictionaryIndices;
    private final boolean[] _packedBoolean;
    private final boolean _hasNulls;
    private final boolean _canEncode;
    private final Runnable _checkTermination;
    @Nullable
    private int[][] _dictionaryMappings;

    private Column(ColumnDataType type, List<ArrowBlock> blocks, int col, Runnable checkTermination) {
      _type = type;
      _checkTermination = checkTermination;
      int size = blocks.size();
      _data = new ArrowBuf[size];
      _validity = new ArrowBuf[size];
      _vectors = new FieldVector[size];
      _dictionaryIndices = new IntVector[size];
      _packedBoolean = new boolean[size];
      boolean hasNulls = false;
      boolean canEncode = size > 0 && (type == ColumnDataType.STRING || type == ColumnDataType.JSON);
      for (int i = 0; i < size; i++) {
        ArrowDataBlock block = blocks.get(i).getDataBlock();
        FieldVector vector = block.getRoot().getVector(col);
        _data[i] = vector.getDataBuffer();
        _validity[i] = vector.getValidityBuffer();
        _vectors[i] = vector;
        _packedBoolean[i] = vector instanceof BitVector;
        hasNulls |= vector.getNullCount() != 0;
        DictionaryEncoding encoding = vector.getField().getDictionary();
        if (encoding != null) {
          if ((type != ColumnDataType.STRING && type != ColumnDataType.JSON) || !(vector instanceof IntVector)
              || block.getDictionaryProvider() == null
              || block.getDictionaryProvider().lookup(encoding.getId()) == null) {
            throw new IllegalArgumentException("Unsupported Arrow join dictionary column: " + col);
          }
          _dictionaryIndices[i] = (IntVector) vector;
          _vectors[i] = block.getDictionaryProvider().lookup(encoding.getId()).getVector();
          if (!(_vectors[i] instanceof VarCharVector)) {
            throw new IllegalArgumentException("Arrow join requires a string payload dictionary");
          }
        }
        // Require repeated values and bound lookup storage by retained rows, not a larger shared dictionary.
        canEncode &= _dictionaryIndices[i] != null && _vectors[i].getValueCount() < vector.getValueCount();
      }
      _hasNulls = hasNulls;
      _canEncode = canEncode;
    }

    private boolean canEncode(int[] blockIds, int[] rows, int count) {
      if (_canEncode) {
        return true;
      }
      if (_vectors.length == 0 || (_type != ColumnDataType.STRING && _type != ColumnDataType.JSON)) {
        return false;
      }
      // Mixed build encodings must not expand a dictionary because of blocks absent from this output.
      for (int i = 0; i < count; i++) {
        if (i % ArrowJoinKeys.BATCH_SIZE == 0) {
          _checkTermination.run();
        }
        int block = blockIds[i];
        if (isValid(block, rows[i])) {
          IntVector indices = _dictionaryIndices[block];
          if (indices == null || _vectors[block].getValueCount() >= indices.getValueCount()) {
            return false;
          }
        }
      }
      return true;
    }

    private void copyDictionary(IntVector target, int[] blockIds, int[] rows, int count,
        MapDictionaryProvider dictionaries, long[] selectedValues) {
      int[][] mappings = _dictionaryMappings;
      if (mappings == null) {
        mappings = new int[_vectors.length][];
        _dictionaryMappings = mappings;
      }
      target.allocateNew(count);
      VarCharVector values = new VarCharVector(target.getName(), target.getAllocator());
      dictionaries.put(new Dictionary(values, target.getField().getDictionary()));
      int unique = 0;
      long bytes = 0;
      try {
        for (int i = 0; i < count; i++) {
          if (i % ArrowJoinKeys.BATCH_SIZE == 0) {
            _checkTermination.run();
          }
          int block = blockIds[i];
          int row = rows[i];
          if (!isValid(block, row)) {
            continue;
          }
          int valueRow = _dictionaryIndices[block].get(row);
          FieldVector source = _vectors[block];
          if (source.isNull(valueRow)) {
            continue;
          }
          int[] mapping = mappings[block];
          if (mapping == null) {
            mapping = new int[source.getValueCount()];
            Arrays.fill(mapping, -1);
            mappings[block] = mapping;
          }
          int id = mapping[valueRow];
          if (id < 0) {
            id = unique;
            selectedValues[unique++] = ((long) block << 32) | (valueRow & 0xffffffffL);
            mapping[valueRow] = id;
            ArrowBuf offsets = source.getOffsetBuffer();
            bytes += offsets.getInt(((long) valueRow + 1) * Integer.BYTES)
                - offsets.getInt((long) valueRow * Integer.BYTES);
          }
          target.set(i, id);
        }
        if (bytes > Integer.MAX_VALUE) {
          throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
              "Arrow join output dictionary is too large");
        }
        values.allocateNew(bytes, unique);
        for (int i = 0; i < unique; i++) {
          if (i % ArrowJoinKeys.BATCH_SIZE == 0) {
            _checkTermination.run();
          }
          long selected = selectedValues[i];
          values.copyFromSafe((int) selected, i, (VarCharVector) _vectors[(int) (selected >>> 32)]);
        }
        values.setValueCount(unique);
      } finally {
        for (int i = 0; i < unique; i++) {
          long selected = selectedValues[i];
          mappings[(int) (selected >>> 32)][(int) selected] = -1;
        }
      }
    }

    private void copy(FieldVector target, int[] blockIds, int[] rows, int count, boolean unmatched) {
      if (target instanceof BaseVariableWidthVector) {
        ((BaseVariableWidthVector) target).allocateNew(variableBytes(blockIds, rows, count), count);
      } else {
        target.setInitialCapacity(count);
        target.allocateNew();
      }
      for (int start = 0; start < count; start += ArrowJoinKeys.BATCH_SIZE) {
        _checkTermination.run();
        int end = Math.min(start + ArrowJoinKeys.BATCH_SIZE, count);
        switch (_type) {
          case INT:
          case FLOAT:
            copy32(target, blockIds, rows, start, end, unmatched);
            break;
          case LONG:
          case TIMESTAMP:
          case DOUBLE:
            copy64(target, blockIds, rows, start, end, unmatched);
            break;
          case BOOLEAN:
            copyBoolean((BitVector) target, blockIds, rows, start, end);
            break;
          case STRING:
          case JSON:
          case BYTES:
            copyVariable((BaseVariableWidthVector) target, blockIds, rows, start, end);
            break;
          default:
            throw new IllegalArgumentException("Unsupported Arrow join payload type: " + _type);
        }
      }
    }

    private long variableBytes(int[] blockIds, int[] rows, int count) {
      long bytes = 0;
      for (int start = 0; start < count; start += ArrowJoinKeys.BATCH_SIZE) {
        _checkTermination.run();
        int end = Math.min(start + ArrowJoinKeys.BATCH_SIZE, count);
        for (int i = start; i < end; i++) {
          int block = blockIds[i];
          int row = rows[i];
          if (isValid(block, row)) {
            IntVector indices = _dictionaryIndices[block];
            int valueRow = indices == null ? row : indices.get(row);
            ArrowBuf offsets = _vectors[block].getOffsetBuffer();
            bytes += offsets.getInt(((long) valueRow + 1) * Integer.BYTES)
                - offsets.getInt((long) valueRow * Integer.BYTES);
          }
        }
      }
      if (bytes > Integer.MAX_VALUE) {
        throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
            "Arrow join variable-width output is too large");
      }
      return bytes;
    }

    private void copy32(FieldVector target, int[] blockIds, int[] rows, int start, int end, boolean unmatched) {
      ArrowBuf data = target.getDataBuffer();
      ArrowBuf validity = target.getValidityBuffer();
      if (!_hasNulls && !unmatched) {
        validity.setOne((long) start >>> 3, BitVectorHelper.getValidityBufferSizeFromCount(end - start));
        for (int i = start; i < end; i++) {
          data.setInt((long) i * Integer.BYTES, _data[blockIds[i]].getInt((long) rows[i] * Integer.BYTES));
        }
      } else {
        for (int i = start; i < end; i++) {
          if (isValid(blockIds[i], rows[i])) {
            data.setInt((long) i * Integer.BYTES, _data[blockIds[i]].getInt((long) rows[i] * Integer.BYTES));
            BitVectorHelper.setBit(validity, i);
          }
        }
      }
    }

    private void copy64(FieldVector target, int[] blockIds, int[] rows, int start, int end, boolean unmatched) {
      ArrowBuf data = target.getDataBuffer();
      ArrowBuf validity = target.getValidityBuffer();
      if (!_hasNulls && !unmatched) {
        validity.setOne((long) start >>> 3, BitVectorHelper.getValidityBufferSizeFromCount(end - start));
        for (int i = start; i < end; i++) {
          data.setLong((long) i * Long.BYTES, _data[blockIds[i]].getLong((long) rows[i] * Long.BYTES));
        }
      } else {
        for (int i = start; i < end; i++) {
          if (isValid(blockIds[i], rows[i])) {
            data.setLong((long) i * Long.BYTES, _data[blockIds[i]].getLong((long) rows[i] * Long.BYTES));
            BitVectorHelper.setBit(validity, i);
          }
        }
      }
    }

    private void copyBoolean(BitVector target, int[] blockIds, int[] rows, int start, int end) {
      for (int i = start; i < end; i++) {
        int block = blockIds[i];
        int row = rows[i];
        if (isValid(block, row)) {
          target.set(i, _packedBoolean[block] ? (_data[block].getByte(row >>> 3) >>> (row & 7)) & 1
              : _data[block].getInt((long) row * Integer.BYTES));
        }
      }
    }

    private void copyVariable(BaseVariableWidthVector target, int[] blockIds, int[] rows, int start, int end) {
      for (int i = start; i < end; i++) {
        int block = blockIds[i];
        int row = rows[i];
        if (isValid(block, row)) {
          IntVector indices = _dictionaryIndices[block];
          target.copyFromSafe(indices == null ? row : indices.get(row), i, _vectors[block]);
        } else {
          target.setNull(i);
        }
      }
    }

    private boolean isValid(int block, int row) {
      return row >= 0 && ((_validity[block].getByte(row >>> 3) >>> (row & 7)) & 1) != 0;
    }
  }
}
