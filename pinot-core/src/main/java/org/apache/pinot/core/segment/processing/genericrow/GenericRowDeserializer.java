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
package org.apache.pinot.core.segment.processing.genericrow;

import java.util.List;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;

import static java.nio.charset.StandardCharsets.UTF_8;


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
  public void deserialize(long offset, GenericRow buffer) {
    for (int i = 0; i < _numFields; i++) {
      String fieldName = _fieldNames[i];

      if (_isSingleValueFields[i]) {
        switch (_storedTypes[i]) {
          case INT:
            buffer.putValue(fieldName, _dataBuffer.getInt(offset));
            offset += Integer.BYTES;
            break;
          case LONG:
            buffer.putValue(fieldName, _dataBuffer.getLong(offset));
            offset += Long.BYTES;
            break;
          case FLOAT:
            buffer.putValue(fieldName, _dataBuffer.getFloat(offset));
            offset += Float.BYTES;
            break;
          case DOUBLE:
            buffer.putValue(fieldName, _dataBuffer.getDouble(offset));
            offset += Double.BYTES;
            break;
          case STRING: {
            int numBytes = _dataBuffer.getInt(offset);
            offset += Integer.BYTES;
            byte[] stringBytes = new byte[numBytes];
            _dataBuffer.copyTo(offset, stringBytes);
            offset += numBytes;
            buffer.putValue(fieldName, new String(stringBytes, UTF_8));
            break;
          }
          case BYTES: {
            int numBytes = _dataBuffer.getInt(offset);
            offset += Integer.BYTES;
            byte[] bytes = new byte[numBytes];
            _dataBuffer.copyTo(offset, bytes);
            offset += numBytes;
            buffer.putValue(fieldName, bytes);
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
              multiValue[j] = new String(stringBytes, UTF_8);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported MV stored type: " + _storedTypes[i]);
        }

        buffer.putValue(fieldName, multiValue);
      }
    }

    // Deserialize null fields if enabled
    if (_includeNullFields) {
      int numNullFields = _dataBuffer.getInt(offset);
      offset += Integer.BYTES;
      for (int i = 0; i < numNullFields; i++) {
        buffer.addNullValueField(_fieldNames[_dataBuffer.getInt(offset)]);
        offset += Integer.BYTES;
      }
    }
  }

  /**
   * Compares the rows at the given offsets.
   */
  public int compare(long offset1, long offset2, int numFieldsToCompare) {
    for (int i = 0; i < numFieldsToCompare; i++) {
      if (_isSingleValueFields[i]) {
        switch (_storedTypes[i]) {
          case INT: {
            int result = Integer.compare(_dataBuffer.getInt(offset1), _dataBuffer.getInt(offset2));
            if (result != 0) {
              return result;
            }
            offset1 += Integer.BYTES;
            offset2 += Integer.BYTES;
            break;
          }
          case LONG: {
            int result = Long.compare(_dataBuffer.getLong(offset1), _dataBuffer.getLong(offset2));
            if (result != 0) {
              return result;
            }
            offset1 += Long.BYTES;
            offset2 += Long.BYTES;
            break;
          }
          case FLOAT: {
            int result = Float.compare(_dataBuffer.getFloat(offset1), _dataBuffer.getFloat(offset2));
            if (result != 0) {
              return result;
            }
            offset1 += Float.BYTES;
            offset2 += Float.BYTES;
            break;
          }
          case DOUBLE: {
            int result = Double.compare(_dataBuffer.getDouble(offset1), _dataBuffer.getDouble(offset2));
            if (result != 0) {
              return result;
            }
            offset1 += Double.BYTES;
            offset2 += Double.BYTES;
            break;
          }
          case STRING: {
            int numBytes1 = _dataBuffer.getInt(offset1);
            offset1 += Integer.BYTES;
            byte[] stringBytes1 = new byte[numBytes1];
            _dataBuffer.copyTo(offset1, stringBytes1);
            int numBytes2 = _dataBuffer.getInt(offset2);
            offset2 += Integer.BYTES;
            byte[] stringBytes2 = new byte[numBytes2];
            _dataBuffer.copyTo(offset2, stringBytes2);
            int result = new String(stringBytes1, UTF_8).compareTo(new String(stringBytes2, UTF_8));
            if (result != 0) {
              return result;
            }
            offset1 += numBytes1;
            offset2 += numBytes2;
            break;
          }
          case BYTES: {
            int numBytes1 = _dataBuffer.getInt(offset1);
            offset1 += Integer.BYTES;
            byte[] bytes1 = new byte[numBytes1];
            _dataBuffer.copyTo(offset1, bytes1);
            int numBytes2 = _dataBuffer.getInt(offset2);
            offset2 += Integer.BYTES;
            byte[] bytes2 = new byte[numBytes2];
            _dataBuffer.copyTo(offset2, bytes2);
            int result = ByteArray.compare(bytes1, bytes2);
            if (result != 0) {
              return result;
            }
            offset1 += numBytes1;
            offset2 += numBytes2;
            break;
          }
          default:
            throw new IllegalStateException("Unsupported SV stored type: " + _storedTypes[i]);
        }
      } else {
        int numValues = _dataBuffer.getInt(offset1);
        int numValues2 = _dataBuffer.getInt(offset2);
        if (numValues != numValues2) {
          return Integer.compare(numValues, numValues2);
        }
        offset1 += Integer.BYTES;
        offset2 += Integer.BYTES;

        switch (_storedTypes[i]) {
          case INT:
            for (int j = 0; j < numValues; j++) {
              int result = Integer.compare(_dataBuffer.getInt(offset1), _dataBuffer.getInt(offset2));
              if (result != 0) {
                return result;
              }
              offset1 += Integer.BYTES;
              offset2 += Integer.BYTES;
            }
            break;
          case LONG:
            for (int j = 0; j < numValues; j++) {
              int result = Long.compare(_dataBuffer.getLong(offset1), _dataBuffer.getLong(offset2));
              if (result != 0) {
                return result;
              }
              offset1 += Long.BYTES;
              offset2 += Long.BYTES;
            }
            break;
          case FLOAT:
            for (int j = 0; j < numValues; j++) {
              int result = Float.compare(_dataBuffer.getFloat(offset1), _dataBuffer.getFloat(offset2));
              if (result != 0) {
                return result;
              }
              offset1 += Float.BYTES;
              offset2 += Float.BYTES;
            }
            break;
          case DOUBLE:
            for (int j = 0; j < numValues; j++) {
              int result = Double.compare(_dataBuffer.getDouble(offset1), _dataBuffer.getDouble(offset2));
              if (result != 0) {
                return result;
              }
              offset1 += Double.BYTES;
              offset2 += Double.BYTES;
            }
            break;
          case STRING:
            for (int j = 0; j < numValues; j++) {
              int numBytes1 = _dataBuffer.getInt(offset1);
              offset1 += Integer.BYTES;
              byte[] stringBytes1 = new byte[numBytes1];
              _dataBuffer.copyTo(offset1, stringBytes1);
              int numBytes2 = _dataBuffer.getInt(offset2);
              offset2 += Integer.BYTES;
              byte[] stringBytes2 = new byte[numBytes2];
              _dataBuffer.copyTo(offset2, stringBytes2);
              int result = new String(stringBytes1, UTF_8).compareTo(new String(stringBytes2, UTF_8));
              if (result != 0) {
                return result;
              }
              offset1 += numBytes1;
              offset2 += numBytes2;
            }
            break;
          default:
            throw new IllegalStateException("Unsupported MV stored type: " + _storedTypes[i]);
        }
      }
    }
    return 0;
  }
}
