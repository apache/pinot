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
package org.apache.pinot.core.query.aggregation.utils.exprminmax;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.query.aggregation.utils.ParentAggregationFunctionResultObject;
import org.apache.pinot.core.util.DataBlockExtractUtils;


/// The result of an `EXPR_MIN` / `EXPR_MAX` aggregation: the extremum key seen so far over the measuring columns, and
/// the projection columns of every row that holds it. The object has no key until the first one is set, which is what
/// a server returns when it has no row for the query.
///
/// The serialized form is a header int, `0` without a key and `1` with one, followed by a data block holding the key
/// as its single row and a data block holding the projection rows. Deserialization reads the values out of the data
/// blocks, so a deserialized object is the same as an aggregated one: it can be merged, extended and serialized again.
@SuppressWarnings({"rawtypes", "unchecked"})
public class ExprMinMaxObject implements ParentAggregationFunctionResultObject {
  private static final int NULL_HEADER = 0;
  private static final int NON_NULL_HEADER = 1;

  private final DataSchema _measuringSchema;
  private final DataSchema _projectionSchema;
  private final int _numMeasuringColumns;
  private final int _numProjectionColumns;

  // The extremum key, one value per measuring column; null until the first key is set
  private Comparable[] _extremumKey;
  // The projection columns of the rows that hold the extremum key
  private final List<Object[]> _extremumProjectionValues;

  public ExprMinMaxObject(DataSchema measuringSchema, DataSchema projectionSchema) {
    _measuringSchema = measuringSchema;
    _projectionSchema = projectionSchema;
    _numMeasuringColumns = measuringSchema.size();
    _numProjectionColumns = projectionSchema.size();
    _extremumProjectionValues = new ArrayList<>();
  }

  /// Reads the object that [#toBytes] wrote from the current position of the big-endian `byteBuffer`, and leaves the
  /// position at the end of the object. The values are copied out of the buffer, so the object does not reference the
  /// buffer afterwards.
  public ExprMinMaxObject(ByteBuffer byteBuffer)
      throws IOException {
    int header = byteBuffer.getInt();
    DataBlock measuringKeys = DataBlockUtils.readFrom(byteBuffer);
    DataBlock projectionValues = DataBlockUtils.readFrom(byteBuffer);
    _measuringSchema = measuringKeys.getDataSchema();
    _projectionSchema = projectionValues.getDataSchema();
    _numMeasuringColumns = _measuringSchema.size();
    _numProjectionColumns = _projectionSchema.size();
    if (header == NULL_HEADER) {
      _extremumProjectionValues = new ArrayList<>();
    } else {
      Object[] key = DataBlockExtractUtils.extractRows(measuringKeys).get(0);
      _extremumKey = Arrays.copyOf(key, _numMeasuringColumns, Comparable[].class);
      _extremumProjectionValues = DataBlockExtractUtils.extractRows(projectionValues);
    }
  }

  /// Returns the serialized form of this object.
  public byte[] toBytes()
      throws IOException {
    int header = _extremumKey != null ? NON_NULL_HEADER : NULL_HEADER;
    List<Object[]> keyRows = _extremumKey != null ? List.<Object[]>of(_extremumKey) : List.of();
    List<ByteBuffer> buffers =
        new ArrayList<>(DataBlockUtils.serialize(DataBlockBuilder.buildFromRows(keyRows, _measuringSchema)));
    buffers.addAll(
        DataBlockUtils.serialize(DataBlockBuilder.buildFromRows(_extremumProjectionValues, _projectionSchema)));
    int size = Integer.BYTES;
    for (ByteBuffer buffer : buffers) {
      size = Math.addExact(size, buffer.remaining());
    }
    byte[] bytes = new byte[size];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(header);
    for (ByteBuffer buffer : buffers) {
      byteBuffer.put(buffer);
    }
    return bytes;
  }

  /// Compares the key at `offset` of the measuring columns with the extremum key, and makes it the extremum key when
  /// it wins or when there is no key yet. Returns:
  /// - a positive value when the key replaced the extremum key
  /// - `0` when the key equals the extremum key, or is the first key
  /// - a negative value when the extremum key stays
  public int compareAndSetKey(List<ExprMinMaxMeasuringValSetWrapper> measuringValSets, int offset, boolean isMax) {
    if (_extremumKey == null) {
      _extremumKey = new Comparable[_numMeasuringColumns];
      setKey(measuringValSets, offset);
      return 0;
    }
    for (int i = 0; i < _numMeasuringColumns; i++) {
      int result = measuringValSets.get(i).compare(offset, _extremumKey[i]);
      if (result != 0) {
        if (isMax ? result < 0 : result > 0) {
          setKey(measuringValSets, offset);
          return 1;
        }
        return -1;
      }
    }
    return 0;
  }

  private void setKey(List<ExprMinMaxMeasuringValSetWrapper> measuringValSets, int offset) {
    for (int i = 0; i < _numMeasuringColumns; i++) {
      _extremumKey[i] = measuringValSets.get(i).getComparable(offset);
    }
  }

  /// Replaces the projection rows with the row at `offset` of the projection columns, after the key was replaced.
  public void setToNewVal(List<ExprMinMaxProjectionValSetWrapper> projectionValSets, int offset) {
    _extremumProjectionValues.clear();
    addVal(projectionValSets, offset);
  }

  /// Adds the row at `offset` of the projection columns, after the key was matched.
  public void addVal(List<ExprMinMaxProjectionValSetWrapper> projectionValSets, int offset) {
    Object[] values = new Object[_numProjectionColumns];
    for (int i = 0; i < _numProjectionColumns; i++) {
      values[i] = projectionValSets.get(i).getValue(offset);
    }
    _extremumProjectionValues.add(values);
  }

  /// Returns the extremum key, or `null` when no key has been set.
  @Nullable
  public Comparable[] getExtremumKey() {
    return _extremumKey;
  }

  @Override
  public Object getField(int rowId, int colId) {
    return _extremumProjectionValues.get(rowId)[colId];
  }

  /// Merges `other` into this object and returns the object with the winning key. When the keys are equal, the
  /// projection rows of `other` are appended to this object.
  public ExprMinMaxObject merge(ExprMinMaxObject other, boolean isMax) {
    if (other._extremumKey == null) {
      return this;
    }
    if (_extremumKey == null) {
      return other;
    }
    for (int i = 0; i < _numMeasuringColumns; i++) {
      int result = _extremumKey[i].compareTo(other._extremumKey[i]);
      if (result != 0) {
        return (isMax ? result > 0 : result < 0) ? this : other;
      }
    }
    _extremumProjectionValues.addAll(other._extremumProjectionValues);
    return this;
  }

  @Override
  public int getNumberOfRows() {
    return _extremumProjectionValues.size();
  }

  /// Returns the schema of the projection columns, the only columns the final result exposes.
  @Override
  public DataSchema getSchema() {
    return _projectionSchema;
  }

  @Override
  public int compareTo(ParentAggregationFunctionResultObject o) {
    return getNumberOfRows() - o.getNumberOfRows();
  }
}
