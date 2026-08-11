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
package org.apache.pinot.core.common.datatable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTableUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.MapUtils;
import org.roaringbitmap.RoaringBitmap;


/// Base DataTableBuilder implementation.
///
/// Null values are always supported, independent of the query's null-handling option. [#setNull] accepts any column
/// type: types that carry their own in-band null encoding keep it, and every other type is written as the column's
/// null placeholder with the row recorded in a per-column null bitmap. The bitmap section is appended by
/// [#writeNullBitmaps], which subclasses must invoke from `build()` after the last row.
///
/// The bitmap section is emitted only when at least one null was recorded, so a table without nulls is byte-for-byte
/// identical to one produced before null bitmaps existed. Readers detect the section by buffer length and cannot
/// distinguish "no section because no nulls" from "no section because the writer never emitted one" -- both mean the
/// same thing.
public abstract class BaseDataTableBuilder implements DataTableBuilder {
  /// Column types whose null value is already representable in-band, and which therefore need no bitmap entry:
  /// - `OBJECT` and `UNKNOWN` are read back via `DataTable.getCustomObject`, which returns `null` for the
  ///   [CustomObject#NULL_TYPE_VALUE] marker.
  /// - `MAP` is read back via `DataTable.getMap`, which returns `null` for a zero-length entry.
  ///
  /// These are also the only types for which the legacy `setNull` encoding was ever correct: it writes an 8-byte
  /// (offset, length) pair, which overflows or corrupts the 4-byte slot of an `INT` / `FLOAT` / `STRING` column and
  /// decodes as garbage in an 8-byte `LONG` / `DOUBLE` one.
  private static final EnumSet<ColumnDataType> IN_BAND_NULL_TYPES =
      EnumSet.of(ColumnDataType.OBJECT, ColumnDataType.UNKNOWN, ColumnDataType.MAP);

  protected final DataSchema _dataSchema;
  protected final int _version;
  protected final int[] _columnOffsets;
  protected final int _rowSizeInBytes;
  protected final ByteArrayOutputStream _fixedSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  protected final DataOutputStream _fixedSizeDataOutputStream =
      new DataOutputStream(_fixedSizeDataByteArrayOutputStream);
  protected final ByteArrayOutputStream _variableSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
  protected final DataOutputStream _variableSizeDataOutputStream =
      new DataOutputStream(_variableSizeDataByteArrayOutputStream);

  private final ColumnDataType[] _storedColumnDataTypes;
  /// Entries are allocated lazily so a table without nulls pays nothing.
  private final RoaringBitmap[] _nullBitmaps;
  private boolean _hasNulls;
  private boolean _nullBitmapsWritten;
  /// Cursor for the positional [#setNullRowIds] contract.
  private int _nullRowIdsColId;

  protected int _numRows;
  protected ByteBuffer _currentRowDataByteBuffer;

  public BaseDataTableBuilder(DataSchema dataSchema, int version) {
    _dataSchema = dataSchema;
    _version = version;
    _columnOffsets = new int[dataSchema.size()];
    _rowSizeInBytes = DataTableUtils.computeColumnOffsets(dataSchema, _columnOffsets, _version);
    _storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    _nullBitmaps = new RoaringBitmap[dataSchema.size()];
  }

  @Override
  public void startRow() {
    _numRows++;
    _currentRowDataByteBuffer = ByteBuffer.allocate(_rowSizeInBytes);
  }

  @Override
  public void setColumn(int colId, int value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(value);
  }

  @Override
  public void setColumn(int colId, long value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putLong(value);
  }

  @Override
  public void setColumn(int colId, float value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putFloat(value);
  }

  @Override
  public void setColumn(int colId, double value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putDouble(value);
  }

  @Override
  public void setColumn(int colId, BigDecimal value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = BigDecimalUtils.serialize(value);
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataByteArrayOutputStream.write(bytes);
  }

  @Override
  public void setColumn(int colId, @Nullable Map<String, Object> value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    if (value == null) {
      _currentRowDataByteBuffer.putInt(0);
    } else {
      byte[] bytes = MapUtils.serializeMap(value, false);
      _currentRowDataByteBuffer.putInt(bytes.length);
      _variableSizeDataByteArrayOutputStream.write(bytes);
    }
  }

  @Override
  public void setColumn(int colId, int[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (int value : values) {
      _variableSizeDataOutputStream.writeInt(value);
    }
  }

  @Override
  public void setColumn(int colId, long[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (long value : values) {
      _variableSizeDataOutputStream.writeLong(value);
    }
  }

  @Override
  public void setColumn(int colId, float[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (float value : values) {
      _variableSizeDataOutputStream.writeFloat(value);
    }
  }

  @Override
  public void setColumn(int colId, double[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (double value : values) {
      _variableSizeDataOutputStream.writeDouble(value);
    }
  }

  @Override
  public void setColumn(int colId, BigDecimal[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (BigDecimal value : values) {
      byte[] bytes = BigDecimalUtils.serialize(value);
      _variableSizeDataOutputStream.writeInt(bytes.length);
      _variableSizeDataByteArrayOutputStream.write(bytes);
    }
  }

  @Override
  public void setColumn(int colId, ByteArray[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (ByteArray value : values) {
      byte[] bytes = value.getBytes();
      _variableSizeDataOutputStream.writeInt(bytes.length);
      _variableSizeDataByteArrayOutputStream.write(bytes);
    }
  }

  @Override
  public void setColumn(int colId, AggregationFunction.SerializedIntermediateResult value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    int type = value.getType();
    byte[] bytes = value.getBytes();
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataOutputStream.writeInt(type);
    _variableSizeDataByteArrayOutputStream.write(bytes);
  }

  @Override
  public void setNull(int colId)
      throws IOException {
    ColumnDataType storedColumnDataType = _storedColumnDataTypes[colId];
    if (IN_BAND_NULL_TYPES.contains(storedColumnDataType)) {
      _currentRowDataByteBuffer.position(_columnOffsets[colId]);
      _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
      _currentRowDataByteBuffer.putInt(0);
      _variableSizeDataOutputStream.writeInt(CustomObject.NULL_TYPE_VALUE);
      return;
    }
    // Resolved on the logical type, not the stored type: UUID overrides getNullPlaceholder() to return the nil UUID,
    // whereas its stored type BYTES would yield a zero-length placeholder that is not a valid UUID.
    Object nullPlaceholder = _dataSchema.getColumnDataType(colId).getNullPlaceholder();
    assert nullPlaceholder != null;
    DataTableBuilderUtils.setColumn(this, storedColumnDataType, colId, nullPlaceholder);
    RoaringBitmap nullBitmap = _nullBitmaps[colId];
    if (nullBitmap == null) {
      nullBitmap = new RoaringBitmap();
      _nullBitmaps[colId] = nullBitmap;
      _hasNulls = true;
    }
    // startRow() has already incremented _numRows for the row being written.
    nullBitmap.add(_numRows - 1);
  }

  @Override
  public void finishRow()
      throws IOException {
    _fixedSizeDataByteArrayOutputStream.write(_currentRowDataByteBuffer.array());
  }

  @Override
  public void setNullRowIds(@Nullable RoaringBitmap nullRowIds) {
    int colId = _nullRowIdsColId++;
    if (nullRowIds == null || nullRowIds.isEmpty()) {
      return;
    }
    RoaringBitmap nullBitmap = _nullBitmaps[colId];
    if (nullBitmap == null) {
      // Copy rather than alias: the caller retains ownership of the bitmap it passed in.
      nullBitmap = new RoaringBitmap();
      _nullBitmaps[colId] = nullBitmap;
      _hasNulls = true;
    }
    nullBitmap.or(nullRowIds);
  }

  /// Appends the per-column null bitmap section to the fixed and variable size buffers. Subclasses must invoke this
  /// from `build()`, after every row has been written and before the buffers are handed off.
  ///
  /// Writes nothing when no null was recorded, keeping such tables byte-for-byte identical to the pre-null-bitmap
  /// format. The section is all-or-nothing across columns because readers index into it at a fixed stride.
  ///
  /// Idempotent, so that `build()` can be invoked more than once on the same builder.
  protected void writeNullBitmaps()
      throws IOException {
    if (!_hasNulls || _nullBitmapsWritten) {
      return;
    }
    _nullBitmapsWritten = true;
    for (RoaringBitmap nullBitmap : _nullBitmaps) {
      _fixedSizeDataOutputStream.writeInt(_variableSizeDataByteArrayOutputStream.size());
      if (nullBitmap == null) {
        _fixedSizeDataOutputStream.writeInt(0);
      } else {
        byte[] bitmapBytes = RoaringBitmapUtils.serialize(nullBitmap);
        _fixedSizeDataOutputStream.writeInt(bitmapBytes.length);
        _variableSizeDataByteArrayOutputStream.write(bitmapBytes);
      }
    }
  }
}
