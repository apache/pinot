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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Utility class to serialize the {@link GenericRow}.
 * The bytes are stored in NATIVE order. The data should be deserialized by the {@link GenericRowDeserializer} on the
 * same host to ensure that both of them are using the same byte order.
 */
public class GenericRowSerializer {
  private final int _numFields;
  private final String[] _fieldNames;
  private final boolean[] _isSingleValueFields;
  private final DataType[] _storedTypes;
  // Cache the encoded string bytes
  private final Object[] _stringBytes;
  // Store index for null fields
  private final Map<String, Integer> _fieldIndexMap;
  private final int[] _nullFieldIndexes;

  public GenericRowSerializer(List<FieldSpec> fieldSpecs, boolean includeNullFields) {
    _numFields = fieldSpecs.size();
    _fieldNames = new String[_numFields];
    _isSingleValueFields = new boolean[_numFields];
    _storedTypes = new DataType[_numFields];
    _stringBytes = new Object[_numFields];
    for (int i = 0; i < _numFields; i++) {
      FieldSpec fieldSpec = fieldSpecs.get(i);
      _fieldNames[i] = fieldSpec.getName();
      _isSingleValueFields[i] = fieldSpec.isSingleValueField();
      _storedTypes[i] = fieldSpec.getDataType().getStoredType();
    }
    if (includeNullFields) {
      _fieldIndexMap = new HashMap<>();
      for (int i = 0; i < _numFields; i++) {
        _fieldIndexMap.put(_fieldNames[i], i);
      }
      _nullFieldIndexes = new int[_numFields];
    } else {
      _fieldIndexMap = null;
      _nullFieldIndexes = null;
    }
  }

  /**
   * Serializes the given {@link GenericRow}.
   */
  public byte[] serialize(GenericRow row) {
    int numBytes = 0;

    // First pass: calculate the number of bytes required
    for (int i = 0; i < _numFields; i++) {
      Object value = row.getValue(_fieldNames[i]);

      if (_isSingleValueFields[i]) {
        switch (_storedTypes[i]) {
          case INT:
            numBytes += Integer.BYTES;
            break;
          case LONG:
            numBytes += Long.BYTES;
            break;
          case FLOAT:
            numBytes += Float.BYTES;
            break;
          case DOUBLE:
            numBytes += Double.BYTES;
            break;
          case STRING:
            byte[] stringBytes = ((String) value).getBytes(UTF_8);
            numBytes += Integer.BYTES + stringBytes.length;
            _stringBytes[i] = stringBytes;
            break;
          case BYTES:
            numBytes += Integer.BYTES + ((byte[]) value).length;
            break;
          default:
            throw new IllegalStateException("Unsupported SV stored type: " + _storedTypes[i]);
        }
      } else {
        Object[] multiValue = (Object[]) value;
        int numValues = multiValue.length;
        numBytes += Integer.BYTES; // Number of values

        switch (_storedTypes[i]) {
          case INT:
            numBytes += Integer.BYTES * numValues;
            break;
          case LONG:
            numBytes += Long.BYTES * numValues;
            break;
          case FLOAT:
            numBytes += Float.BYTES * numValues;
            break;
          case DOUBLE:
            numBytes += Double.BYTES * numValues;
            break;
          case STRING:
            numBytes += Integer.BYTES * numValues;
            byte[][] stringBytesArray = new byte[numValues][];
            for (int j = 0; j < numValues; j++) {
              byte[] stringBytes = ((String) multiValue[j]).getBytes(UTF_8);
              numBytes += stringBytes.length;
              stringBytesArray[j] = stringBytes;
            }
            _stringBytes[i] = stringBytesArray;
            break;
          default:
            throw new IllegalStateException("Unsupported MV stored type: " + _storedTypes[i]);
        }
      }
    }

    // Serialize null fields if enabled
    int numNullFields = 0;
    if (_fieldIndexMap != null) {
      Set<String> nullFields = row.getNullValueFields();
      for (String nullField : nullFields) {
        Integer nullFieldIndex = _fieldIndexMap.get(nullField);
        if (nullFieldIndex != null) {
          _nullFieldIndexes[numNullFields++] = nullFieldIndex;
        }
      }
      Arrays.sort(_nullFieldIndexes, 0, numNullFields);
      numBytes += Integer.BYTES * (1 + numNullFields);
    }

    byte[] serializedBytes = new byte[numBytes];
    ByteBuffer byteBuffer = ByteBuffer.wrap(serializedBytes).order(PinotDataBuffer.NATIVE_ORDER);

    // Second pass: serialize the values
    for (int i = 0; i < _numFields; i++) {
      Object value = row.getValue(_fieldNames[i]);

      if (_isSingleValueFields[i]) {
        switch (_storedTypes[i]) {
          case INT:
            byteBuffer.putInt((int) value);
            break;
          case LONG:
            byteBuffer.putLong((long) value);
            break;
          case FLOAT:
            byteBuffer.putFloat((float) value);
            break;
          case DOUBLE:
            byteBuffer.putDouble((double) value);
            break;
          case STRING:
            byte[] stringBytes = (byte[]) _stringBytes[i];
            byteBuffer.putInt(stringBytes.length);
            byteBuffer.put(stringBytes);
            break;
          case BYTES:
            byte[] bytes = (byte[]) value;
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
            break;
          default:
            throw new IllegalStateException("Unsupported SV stored type: " + _storedTypes[i]);
        }
      } else {
        Object[] multiValue = (Object[]) value;
        byteBuffer.putInt(multiValue.length);

        switch (_storedTypes[i]) {
          case INT:
            for (Object element : multiValue) {
              byteBuffer.putInt((int) element);
            }
            break;
          case LONG:
            for (Object element : multiValue) {
              byteBuffer.putLong((long) element);
            }
            break;
          case FLOAT:
            for (Object element : multiValue) {
              byteBuffer.putFloat((float) element);
            }
            break;
          case DOUBLE:
            for (Object element : multiValue) {
              byteBuffer.putDouble((double) element);
            }
            break;
          case STRING:
            byte[][] stringBytesArray = (byte[][]) _stringBytes[i];
            for (byte[] stringBytes : stringBytesArray) {
              byteBuffer.putInt(stringBytes.length);
              byteBuffer.put(stringBytes);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported MV stored type: " + _storedTypes[i]);
        }
      }
    }

    // Serialize null fields if enabled
    if (_fieldIndexMap != null) {
      byteBuffer.putInt(numNullFields);
      for (int i = 0; i < numNullFields; i++) {
        byteBuffer.putInt(_nullFieldIndexes[i]);
      }
    }

    return serializedBytes;
  }
}
