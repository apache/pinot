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
package org.apache.pinot.core.segment.processing.serde;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.StringUtils;


/**
 * Utility class to deserialize the {@link GenericRow}.
 * The bytes are read in NATIVE order. The data should be serialized by the {@link GenericRowSerializer} on the same
 * host to ensure that both of them are using the same byte order.
 */
public class GenericRowDeserializer {
  private final PinotDataBuffer _dataBuffer;
  private final int _numFields;
  private final String[] _fieldNames;
  private final boolean[] _isSingleValueFields;
  private final DataType[] _storedTypes;
  private final boolean _includeNullFields;

  public GenericRowDeserializer(PinotDataBuffer dataBuffer, List<FieldSpec> fieldSpecs, boolean includeNullFields) {
    _dataBuffer = dataBuffer;
    _numFields = fieldSpecs.size();
    _fieldNames = new String[_numFields];
    _isSingleValueFields = new boolean[_numFields];
    _storedTypes = new DataType[_numFields];
    for (int i = 0; i < _numFields; i++) {
      FieldSpec fieldSpec = fieldSpecs.get(i);
      _fieldNames[i] = fieldSpec.getName();
      _isSingleValueFields[i] = fieldSpec.isSingleValueField();
      _storedTypes[i] = fieldSpec.getDataType().getStoredType();
    }
    _includeNullFields = includeNullFields;
  }

  /**
   * Deserializes the {@link GenericRow} at the given offset.
   */
  public GenericRow deserialize(long offset, GenericRow reuse) {
    reuse.clear();

    for (int i = 0; i < _numFields; i++) {
      String fieldName = _fieldNames[i];

      if (_isSingleValueFields[i]) {
        switch (_storedTypes[i]) {
          case INT:
            int intValue = _dataBuffer.getInt(offset);
            reuse.putValue(fieldName, intValue);
            offset += Integer.BYTES;
            break;
          case LONG:
            long longValue = _dataBuffer.getLong(offset);
            reuse.putValue(fieldName, longValue);
            offset += Long.BYTES;
            break;
          case FLOAT:
            float floatValue = _dataBuffer.getFloat(offset);
            reuse.putValue(fieldName, floatValue);
            offset += Float.BYTES;
            break;
          case DOUBLE:
            double doubleValue = _dataBuffer.getDouble(offset);
            reuse.putValue(fieldName, doubleValue);
            offset += Double.BYTES;
            break;
          case STRING: {
            int numBytes = _dataBuffer.getInt(offset);
            offset += Integer.BYTES;
            byte[] stringBytes = new byte[numBytes];
            _dataBuffer.copyTo(offset, stringBytes);
            offset += numBytes;
            reuse.putValue(fieldName, StringUtils.decodeUtf8(stringBytes));
            break;
          }
          case BYTES: {
            int numBytes = _dataBuffer.getInt(offset);
            offset += Integer.BYTES;
            byte[] bytes = new byte[numBytes];
            _dataBuffer.copyTo(offset, bytes);
            offset += numBytes;
            reuse.putValue(fieldName, bytes);
            break;
          }
          default:
            throw new IllegalStateException("Unsupported SV stored type: " + _storedTypes[i]);
        }
      } else {
        int numValues = _dataBuffer.getInt(offset);
        offset += Integer.BYTES;
        Object[] multiValue = new Object[numValues];

        switch (_storedTypes[i]) {
          case INT:
            for (int j = 0; j < numValues; j++) {
              multiValue[j] = _dataBuffer.getInt(offset);
              offset += Integer.BYTES;
            }
            break;
          case LONG:
            for (int j = 0; j < numValues; j++) {
              multiValue[j] = _dataBuffer.getLong(offset);
              offset += Long.BYTES;
            }
            break;
          case FLOAT:
            for (int j = 0; j < numValues; j++) {
              multiValue[j] = _dataBuffer.getFloat(offset);
              offset += Float.BYTES;
            }
            break;
          case DOUBLE:
            for (int j = 0; j < numValues; j++) {
              multiValue[j] = _dataBuffer.getDouble(offset);
              offset += Double.BYTES;
            }
            break;
          case STRING:
            for (int j = 0; j < numValues; j++) {
              int numBytes = _dataBuffer.getInt(offset);
              offset += Integer.BYTES;
              byte[] stringBytes = new byte[numBytes];
              _dataBuffer.copyTo(offset, stringBytes);
              offset += numBytes;
              multiValue[j] = StringUtils.decodeUtf8(stringBytes);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported MV stored type: " + _storedTypes[i]);
        }

        reuse.putValue(fieldName, multiValue);
      }
    }

    // Deserialize null fields if enabled
    if (_includeNullFields) {
      int numNullFields = _dataBuffer.getInt(offset);
      offset += Integer.BYTES;
      for (int i = 0; i < numNullFields; i++) {
        reuse.addNullValueField(_fieldNames[_dataBuffer.getInt(offset)]);
        offset += Integer.BYTES;
      }
    }

    return reuse;
  }

  /**
   * Deserializes the first several fields at the given offset. This method can be used to sort the generic rows without
   * fully deserialize the whole row for each comparison. The selected fields should all be single-valued.
   */
  public Object[] partialDeserialize(long offset, int numFields) {
    Object[] values = new Object[numFields];

    for (int i = 0; i < numFields; i++) {
      Preconditions.checkState(_isSingleValueFields[i], "Partial deserialize should not be applied to MV column: %s",
          _fieldNames[i]);
      switch (_storedTypes[i]) {
        case INT:
          values[i] = _dataBuffer.getInt(offset);
          offset += Integer.BYTES;
          break;
        case LONG:
          values[i] = _dataBuffer.getLong(offset);
          offset += Long.BYTES;
          break;
        case FLOAT:
          values[i] = _dataBuffer.getFloat(offset);
          offset += Float.BYTES;
          break;
        case DOUBLE:
          values[i] = _dataBuffer.getDouble(offset);
          offset += Double.BYTES;
          break;
        case STRING: {
          int numBytes = _dataBuffer.getInt(offset);
          offset += Integer.BYTES;
          byte[] stringBytes = new byte[numBytes];
          _dataBuffer.copyTo(offset, stringBytes);
          offset += numBytes;
          values[i] = StringUtils.decodeUtf8(stringBytes);
          break;
        }
        case BYTES: {
          int numBytes = _dataBuffer.getInt(offset);
          offset += Integer.BYTES;
          byte[] bytes = new byte[numBytes];
          _dataBuffer.copyTo(offset, bytes);
          offset += numBytes;
          values[i] = bytes;
          break;
        }
        default:
          throw new IllegalStateException("Unsupported SV stored type: " + _storedTypes[i]);
      }
    }

    return values;
  }
}
