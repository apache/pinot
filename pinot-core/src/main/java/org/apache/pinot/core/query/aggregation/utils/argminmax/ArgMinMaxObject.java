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

package org.apache.pinot.core.query.aggregation.utils.argminmax;

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


public class ArgMinMaxObject implements ParentAggregationFunctionResultObject {

  // if the object is created but not yet populated, this happens e.g. when a server has no data for
  // the query and returns a default value
  public static final int NOT_NULL_OBJECT = 1;
  public static final int IS_NULL_OBJECT = 0;
  // if the object contains non null values
  private boolean _isNull;

  // if the value is stored in a mutable list, this is true only when the Object is deserialized
  // from a byte buffer
  private boolean _mutable;

  // the schema of the measuring columns
  private final DataSchema _keySchema;
  // the schema of the projection columns
  private final DataSchema _valSchema;

  // the size of the extremum key cols and value clos
  private final int _sizeOfExtremumKeys;
  private final int _sizeOfExtremumVals;

  // the current extremum keys
  private Comparable[] _extremumKeys = null;
  // the current extremum values
  private final List<Object[]> _extremumValues = new ArrayList<>();

  // used for ser/de
  private DataBlock _immutableKeys;
  private DataBlock _immutableVals;

  public ArgMinMaxObject(DataSchema keySchema, DataSchema valSchema) {
    _isNull = true;
    _mutable = true;

    _keySchema = keySchema;
    _valSchema = valSchema;

    _sizeOfExtremumKeys = _keySchema.size();
    _sizeOfExtremumVals = _valSchema.size();
  }

  public ArgMinMaxObject(ByteBuffer byteBuffer)
      throws IOException {
    _mutable = false;
    _isNull = byteBuffer.getInt() == IS_NULL_OBJECT;
    byteBuffer = byteBuffer.slice();
    _immutableKeys = DataBlockUtils.getDataBlock(byteBuffer);
    byteBuffer = byteBuffer.slice();
    _immutableVals = DataBlockUtils.getDataBlock(byteBuffer);

    _keySchema = _immutableKeys.getDataSchema();
    _valSchema = _immutableVals.getDataSchema();

    _sizeOfExtremumKeys = _keySchema.size();
    _sizeOfExtremumVals = _valSchema.size();
  }

  public static ArgMinMaxObject fromBytes(byte[] bytes)
      throws IOException {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static ArgMinMaxObject fromByteBuffer(ByteBuffer byteBuffer)
      throws IOException {
    return new ArgMinMaxObject(byteBuffer);
  }

  // used for result serialization
  @Nonnull
  public byte[] toBytes()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    if (_isNull) {
      // serialize the null object with schemas
      dataOutputStream.writeInt(IS_NULL_OBJECT);
      _immutableKeys = DataBlockBuilder.buildFromRows(Collections.emptyList(), _keySchema);
      _immutableVals = DataBlockBuilder.buildFromRows(Collections.emptyList(), _valSchema);
    } else {
      dataOutputStream.writeInt(NOT_NULL_OBJECT);
      _immutableKeys = DataBlockBuilder.buildFromRows(Collections.singletonList(_extremumKeys), _keySchema);
      _immutableVals = DataBlockBuilder.buildFromRows(_extremumValues, _valSchema);
    }
    dataOutputStream.write(_immutableKeys.toBytes());
    dataOutputStream.write(_immutableVals.toBytes());
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Used during segment processing
   * Compare the current key with the new key, and return the comparison result.
   * > 0: the key is replaced because the new key is the new extremum
   * = 0: new key is the same as the current extremum
   * < 0: current key is still the extremum
   */
  public int compareAndSetKey(List<ArgMinMaxWrapperValSet> argMinMaxWrapperValSets, int offset, boolean isMax) {
    Preconditions.checkState(_mutable, "Cannot compare and set key after the object is serialized");
    if (!_isNull) {
      for (int i = 0; i < _sizeOfExtremumKeys; i++) {
        ArgMinMaxWrapperValSet argMinMaxWrapperValSet = argMinMaxWrapperValSets.get(i);
        int result = argMinMaxWrapperValSet.compare(offset, _extremumKeys[i]);
        if (result != 0) {
          if (isMax ? result < 0 : result > 0) {
            for (int j = 0; j < _sizeOfExtremumKeys; j++) {
              _extremumKeys[j] = argMinMaxWrapperValSets.get(j).getComparable(offset);
            }
            return 1;
          }
          return -1;
        }
      }
    } else {
      _isNull = false;
      _extremumKeys = new Comparable[_sizeOfExtremumKeys];
      for (int i = 0; i < _sizeOfExtremumKeys; i++) {
        _extremumKeys[i] = argMinMaxWrapperValSets.get(i).getComparable(offset);
      }
    }
    return 0;
  }

  /**
   * Used during segment processing with compareAndSetKey
   * Set the vals to the new vals if the key is replaced.
   */
  public void setToNewVal(List<ArgMinMaxWrapperValSet> argMinMaxWrapperValSets, int offset) {
    _extremumValues.clear();
    addVal(argMinMaxWrapperValSets, offset);
  }

  /**
   * Used during segment processing with compareAndSetKey
   * Add the vals to the list of vals if the key is the same.
   */
  public void addVal(List<ArgMinMaxWrapperValSet> argMinMaxWrapperValSets, int offset) {
    Object[] val = new Object[_valSchema.size()];
    for (int i = 0; i < _valSchema.size(); i++) {
      val[i] = argMinMaxWrapperValSets.get(i).getValue(offset);
    }
    _extremumValues.add(val);
  }

  public Comparable[] getExtremumKey() {
    if (_mutable) {
      return _extremumKeys;
    } else {
      Comparable[] extremumKeys = new Comparable[_sizeOfExtremumKeys];
      for (int i = 0; i < _sizeOfExtremumKeys; i++) {
        switch (_keySchema.getColumnDataType(i)) {
          case INT:
          case BOOLEAN:
            extremumKeys[i] = _immutableKeys.getInt(0, i);
            break;
          case LONG:
          case TIMESTAMP:
            extremumKeys[i] = _immutableKeys.getLong(0, i);
            break;
          case FLOAT:
            extremumKeys[i] = _immutableKeys.getFloat(0, i);
            break;
          case DOUBLE:
            extremumKeys[i] = _immutableKeys.getDouble(0, i);
            break;
          case STRING:
            extremumKeys[i] = _immutableKeys.getString(0, i);
            break;
          case BIG_DECIMAL:
            extremumKeys[i] = _immutableKeys.getBigDecimal(0, i);
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + _keySchema.getColumnDataType(i));
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
      return _extremumValues.get(rowId)[colId];
    } else {
      switch (_valSchema.getColumnDataType(colId)) {
        case BOOLEAN:
        case INT:
          return _immutableVals.getInt(rowId, colId);
        case TIMESTAMP:
        case LONG:
          return _immutableVals.getLong(rowId, colId);
        case FLOAT:
          return _immutableVals.getFloat(rowId, colId);
        case DOUBLE:
          return _immutableVals.getDouble(rowId, colId);
        case JSON:
        case STRING:
          return _immutableVals.getString(rowId, colId);
        case BYTES:
          return _immutableVals.getBytes(rowId, colId);
        case BIG_DECIMAL:
          return _immutableVals.getBigDecimal(rowId, colId);
        case BOOLEAN_ARRAY:
        case INT_ARRAY:
          return _immutableVals.getIntArray(rowId, colId);
        case TIMESTAMP_ARRAY:
        case LONG_ARRAY:
          return _immutableVals.getLongArray(rowId, colId);
        case FLOAT_ARRAY:
          return _immutableVals.getFloatArray(rowId, colId);
        case DOUBLE_ARRAY:
          return _immutableVals.getDoubleArray(rowId, colId);
        case STRING_ARRAY:
        case BYTES_ARRAY:
          return _immutableVals.getStringArray(rowId, colId);
        default:
          throw new IllegalStateException("Unsupported data type: " + _valSchema.getColumnDataType(colId));
      }
    }
  }

  /**
   * Merge two ArgMinMaxObjects
   */
  public ArgMinMaxObject merge(ArgMinMaxObject other, boolean isMax) {
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
      for (int i = 0; i < _sizeOfExtremumKeys; i++) {
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
          Object[] val = new Object[_sizeOfExtremumVals];
          for (int j = 0; j < _sizeOfExtremumVals; j++) {
            val[j] = getField(i, j);
          }
          _extremumValues.add(val);
        }
      }
      for (int i = 0; i < other.getNumberOfRows(); i++) {
        Object[] val = new Object[_sizeOfExtremumVals];
        for (int j = 0; j < _sizeOfExtremumVals; j++) {
          val[j] = other.getField(i, j);
        }
        _extremumValues.add(val);
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
      return _extremumValues.size();
    } else {
      return _immutableVals.getNumberOfRows();
    }
  }

  /**
   * return the schema of the projection data
   */
  @Override
  public DataSchema getSchema() {
    // the final parent aggregation result only cares about the projection columns
    return _valSchema;
  }

  @Override
  public int compareTo(ParentAggregationFunctionResultObject o) {
    return this.getNumberOfRows() - o.getNumberOfRows();
  }
}
