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

import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.query.aggregation.utils.ParentAggregationFunctionResultObject;


public class ExprMinMaxObject implements ParentAggregationFunctionResultObject {

  // if the object is created but not yet populated, this happens e.g. when a server has no data for
  // the query and returns a default value
  enum ObjectNullState {
    NULL(0),
    NON_NULL(1);

    final int _state;

    ObjectNullState(int i) {
      _state = i;
    }

    int getState() {
      return _state;
    }
  }

  // if the object contains non null values
  private boolean _isNull;

  // if the value is stored in a mutable list, this is false only when the Object is deserialized from a byte buffer
  // if the object is mutable, it means that the object is read only and the values are stored in
  // _immutableMeasuringKeys and _immutableProjectionVals, otherwise we read and write from _extremumMeasuringKeys
  // and _extremumProjectionValues
  private boolean _mutable;

  // the schema of the measuring columns
  private final DataSchema _measuringSchema;
  // the schema of the projection columns
  private final DataSchema _projectionSchema;

  // the size of the extremum key cols and value cols
  private final int _sizeOfExtremumMeasuringKeys;
  private final int _sizeOfExtremumProjectionVals;

  // the current extremum keys, keys are the extremum values of the measuring columns,
  // used for comparison
  private Comparable[] _extremumMeasuringKeys = null;
  // the current extremum values, values are the values of the projection columns
  // associated with the minimum measuring column, used for projection
  private final List<Object[]> _extremumProjectionValues = new ArrayList<>();

  // used for ser/de
  private DataBlock _immutableMeasuringKeys;
  private DataBlock _immutableProjectionVals;

  public ExprMinMaxObject(DataSchema measuringSchema, DataSchema projectionSchema) {
    _isNull = true;
    _mutable = true;

    _measuringSchema = measuringSchema;
    _projectionSchema = projectionSchema;

    _sizeOfExtremumMeasuringKeys = _measuringSchema.size();
    _sizeOfExtremumProjectionVals = _projectionSchema.size();
  }

  public ExprMinMaxObject(ByteBuffer byteBuffer)
      throws IOException {
    _mutable = false;
    _isNull = byteBuffer.getInt() == ObjectNullState.NULL.getState();
    byteBuffer = byteBuffer.slice();
    _immutableMeasuringKeys = DataBlockUtils.getDataBlock(byteBuffer);
    byteBuffer = byteBuffer.slice();
    _immutableProjectionVals = DataBlockUtils.getDataBlock(byteBuffer);

    _measuringSchema = _immutableMeasuringKeys.getDataSchema();
    _projectionSchema = _immutableProjectionVals.getDataSchema();

    _sizeOfExtremumMeasuringKeys = _measuringSchema.size();
    _sizeOfExtremumProjectionVals = _projectionSchema.size();
  }

  public static ExprMinMaxObject fromBytes(byte[] bytes)
      throws IOException {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static ExprMinMaxObject fromByteBuffer(ByteBuffer byteBuffer)
      throws IOException {
    return new ExprMinMaxObject(byteBuffer);
  }

  // used for result serialization
  @Nonnull
  public byte[] toBytes()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    if (_isNull) {
      // serialize the null object with schemas
      dataOutputStream.writeInt(ObjectNullState.NULL.getState());
      _immutableMeasuringKeys = DataBlockBuilder.buildFromRows(Collections.emptyList(), _measuringSchema);
      _immutableProjectionVals = DataBlockBuilder.buildFromRows(Collections.emptyList(), _projectionSchema);
    } else {
      dataOutputStream.writeInt(ObjectNullState.NON_NULL.getState());
      _immutableMeasuringKeys =
          DataBlockBuilder.buildFromRows(Collections.singletonList(_extremumMeasuringKeys), _measuringSchema);
      _immutableProjectionVals = DataBlockBuilder.buildFromRows(_extremumProjectionValues, _projectionSchema);
    }
    dataOutputStream.write(_immutableMeasuringKeys.toBytes());
    dataOutputStream.write(_immutableProjectionVals.toBytes());
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Used during segment processing
   * Compare the current key with the new key, and return the comparison result.
   * > 0: the key is replaced because the new key is the new extremum
   * = 0: new key is the same as the current extremum
   * < 0: current key is still the extremum
   */
  public int compareAndSetKey(List<ExprMinMaxMeasuringValSetWrapper> argMinMaxWrapperValSets, int offset,
      boolean isMax) {
    Preconditions.checkState(_mutable, "Cannot compare and set key after the object is serialized");
    if (!_isNull) {
      for (int i = 0; i < _sizeOfExtremumMeasuringKeys; i++) {
        ExprMinMaxMeasuringValSetWrapper argMinMaxWrapperValSet = argMinMaxWrapperValSets.get(i);
        int result = argMinMaxWrapperValSet.compare(offset, _extremumMeasuringKeys[i]);
        if (result != 0) {
          if (isMax ? result < 0 : result > 0) {
            for (int j = 0; j < _sizeOfExtremumMeasuringKeys; j++) {
              _extremumMeasuringKeys[j] = argMinMaxWrapperValSets.get(j).getComparable(offset);
            }
            return 1;
          }
          return -1;
        }
      }
    } else {
      _isNull = false;
      _extremumMeasuringKeys = new Comparable[_sizeOfExtremumMeasuringKeys];
      for (int i = 0; i < _sizeOfExtremumMeasuringKeys; i++) {
        _extremumMeasuringKeys[i] = argMinMaxWrapperValSets.get(i).getComparable(offset);
      }
    }
    return 0;
  }

  /**
   * Used during segment processing with compareAndSetKey
   * Set the vals to the new vals if the key is replaced.
   */
  public void setToNewVal(List<ExprMinMaxProjectionValSetWrapper> exprMinMaxProjectionValSetWrappers, int offset) {
    _extremumProjectionValues.clear();
    addVal(exprMinMaxProjectionValSetWrappers, offset);
  }

  /**
   * Used during segment processing with compareAndSetKey
   * Add the vals to the list of vals if the key is the same.
   */
  public void addVal(List<ExprMinMaxProjectionValSetWrapper> exprMinMaxProjectionValSetWrappers, int offset) {
    Object[] val = new Object[_projectionSchema.size()];
    for (int i = 0; i < _projectionSchema.size(); i++) {
      val[i] = exprMinMaxProjectionValSetWrappers.get(i).getValue(offset);
    }
    _extremumProjectionValues.add(val);
  }

  public Comparable[] getExtremumKey() {
    if (_mutable) {
      return _extremumMeasuringKeys;
    } else {
      Comparable[] extremumKeys = new Comparable[_sizeOfExtremumMeasuringKeys];
      for (int i = 0; i < _sizeOfExtremumMeasuringKeys; i++) {
        switch (_measuringSchema.getColumnDataType(i)) {
          case INT:
          case BOOLEAN:
            extremumKeys[i] = _immutableMeasuringKeys.getInt(0, i);
            break;
          case LONG:
          case TIMESTAMP:
            extremumKeys[i] = _immutableMeasuringKeys.getLong(0, i);
            break;
          case FLOAT:
            extremumKeys[i] = _immutableMeasuringKeys.getFloat(0, i);
            break;
          case DOUBLE:
            extremumKeys[i] = _immutableMeasuringKeys.getDouble(0, i);
            break;
          case STRING:
            extremumKeys[i] = _immutableMeasuringKeys.getString(0, i);
            break;
          case BIG_DECIMAL:
            extremumKeys[i] = _immutableMeasuringKeys.getBigDecimal(0, i);
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + _measuringSchema.getColumnDataType(i));
        }
      }
      return extremumKeys;
    }
  }

  /**
   * Get the field from a projection column
   */
  @Override
  public Object getField(int rowId, int colId) {
    if (_mutable) {
      return _extremumProjectionValues.get(rowId)[colId];
    } else {
      switch (_projectionSchema.getColumnDataType(colId)) {
        case BOOLEAN:
        case INT:
          return _immutableProjectionVals.getInt(rowId, colId);
        case TIMESTAMP:
        case LONG:
          return _immutableProjectionVals.getLong(rowId, colId);
        case FLOAT:
          return _immutableProjectionVals.getFloat(rowId, colId);
        case DOUBLE:
          return _immutableProjectionVals.getDouble(rowId, colId);
        case JSON:
        case STRING:
          return _immutableProjectionVals.getString(rowId, colId);
        case BYTES:
          return _immutableProjectionVals.getBytes(rowId, colId);
        case BIG_DECIMAL:
          return _immutableProjectionVals.getBigDecimal(rowId, colId);
        case BOOLEAN_ARRAY:
        case INT_ARRAY:
          return _immutableProjectionVals.getIntArray(rowId, colId);
        case TIMESTAMP_ARRAY:
        case LONG_ARRAY:
          return _immutableProjectionVals.getLongArray(rowId, colId);
        case FLOAT_ARRAY:
          return _immutableProjectionVals.getFloatArray(rowId, colId);
        case DOUBLE_ARRAY:
          return _immutableProjectionVals.getDoubleArray(rowId, colId);
        case STRING_ARRAY:
        case BYTES_ARRAY:
          return _immutableProjectionVals.getStringArray(rowId, colId);
        default:
          throw new IllegalStateException("Unsupported data type: " + _projectionSchema.getColumnDataType(colId));
      }
    }
  }

  /**
   * Merge two ArgMinMaxObjects
   */
  public ExprMinMaxObject merge(ExprMinMaxObject other, boolean isMax) {
    if (_isNull && other._isNull) {
      return this;
    } else if (_isNull) {
      return other;
    } else if (other._isNull) {
      return this;
    } else {
      int result;
      Comparable[] key = getExtremumKey();
      Comparable[] otherKey = other.getExtremumKey();
      for (int i = 0; i < _sizeOfExtremumMeasuringKeys; i++) {
        result = key[i].compareTo(otherKey[i]);
        if (result != 0) {
          // If the keys are not equal, return the object with the extremum key
          if (isMax) {
            return result > 0 ? this : other;
          } else {
            return result < 0 ? this : other;
          }
        }
      }
      // If the keys are equal, add the values of the other object to this object
      if (!_mutable) {
        // If the result is immutable, we need to copy the values from the serialized result to the mutable result
        _mutable = true;
        for (int i = 0; i < getNumberOfRows(); i++) {
          Object[] val = new Object[_sizeOfExtremumProjectionVals];
          for (int j = 0; j < _sizeOfExtremumProjectionVals; j++) {
            val[j] = getField(i, j);
          }
          _extremumProjectionValues.add(val);
        }
      }
      for (int i = 0; i < other.getNumberOfRows(); i++) {
        Object[] val = new Object[_sizeOfExtremumProjectionVals];
        for (int j = 0; j < _sizeOfExtremumProjectionVals; j++) {
          val[j] = other.getField(i, j);
        }
        _extremumProjectionValues.add(val);
      }
      return this;
    }
  }

  /**
   * get the number of rows in the projection data
   */
  @Override
  public int getNumberOfRows() {
    if (_mutable) {
      return _extremumProjectionValues.size();
    } else {
      return _immutableProjectionVals.getNumberOfRows();
    }
  }

  /**
   * return the schema of the projection data
   */
  @Override
  public DataSchema getSchema() {
    // the final parent aggregation result only cares about the projection columns
    return _projectionSchema;
  }

  @Override
  public int compareTo(ParentAggregationFunctionResultObject o) {
    return this.getNumberOfRows() - o.getNumberOfRows();
  }
}
