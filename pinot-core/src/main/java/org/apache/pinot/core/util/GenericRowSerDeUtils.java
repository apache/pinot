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
package org.apache.pinot.core.util;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.StringUtils;


/**
 * Utility methods for serde of {@link GenericRow}
 * Deserialization assumes it is deserializing from a {@link PinotDataBuffer}
 */
public final class GenericRowSerDeUtils {

  private GenericRowSerDeUtils() {

  }

  /**
   * Serialize the given GenericRow. The data is stored in native byte order.
   * @param genericRow GenericRow to serialize
   * @param fieldSpecs the fields to serialize
   * @return serialized bytes
   */
  public static byte[] serializeGenericRow(GenericRow genericRow, List<FieldSpec> fieldSpecs) {
    int numBytes = 0;

    for (FieldSpec fieldSpec : fieldSpecs) {
      Object value = genericRow.getValue(fieldSpec.getName());

      if (fieldSpec.isSingleValueField()) {
        switch (fieldSpec.getDataType().getStoredType()) {

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
            byte[] stringBytes = StringUtils.encodeUtf8((String) value);
            numBytes += Integer.BYTES; // string length
            numBytes += stringBytes.length;
            break;
          case BYTES:
            numBytes += Integer.BYTES; // byte array length
            numBytes += ((byte[]) value).length;
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("DataType '%s' not supported", fieldSpec.getDataType()));
        }
      } else {
        Object[] multiValue = (Object[]) value;
        numBytes += Integer.BYTES; // array length

        switch (fieldSpec.getDataType().getStoredType()) {
          case INT:
            numBytes += Integer.BYTES * multiValue.length;
            break;
          case LONG:
            numBytes += Long.BYTES * multiValue.length;
            break;
          case FLOAT:
            numBytes += Float.BYTES * multiValue.length;
            break;
          case DOUBLE:
            numBytes += Double.BYTES * multiValue.length;
            break;
          case STRING:
            for (Object element : multiValue) {
              byte[] stringBytes = StringUtils.encodeUtf8((String) element);
              numBytes += Integer.BYTES; // string length
              numBytes += stringBytes.length;
            }
            break;
          case BYTES:
            for (Object element : multiValue) {
              numBytes += Integer.BYTES; // byte array length
              numBytes += ((byte[]) element).length;
            }
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("DataType '%s' not supported", fieldSpec.getDataType()));
        }
      }
    }

    byte[] genericRowBytes = new byte[numBytes];
    ByteBuffer byteBuffer = ByteBuffer.wrap(genericRowBytes).order(PinotDataBuffer.NATIVE_ORDER);

    for (FieldSpec fieldSpec : fieldSpecs) {
      Object value = genericRow.getValue(fieldSpec.getName());

      if (fieldSpec.isSingleValueField()) {
        switch (fieldSpec.getDataType().getStoredType()) {

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
            byte[] stringBytes = StringUtils.encodeUtf8((String) value);
            byteBuffer.putInt(stringBytes.length);
            byteBuffer.put(stringBytes);
            break;
          case BYTES:
            byte[] bytes = (byte[]) value;
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("DataType '%s' not supported", fieldSpec.getDataType()));
        }
      } else {
        Object[] multiValue = (Object[]) value;
        byteBuffer.putInt(multiValue.length);

        switch (fieldSpec.getDataType().getStoredType()) {

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
            for (Object element : multiValue) {
              byte[] stringBytes = StringUtils.encodeUtf8((String) element);
              byteBuffer.putInt(stringBytes.length);
              byteBuffer.put(stringBytes);
            }
            break;
          case BYTES:
            for (Object element : multiValue) {
              byte[] bytes = (byte[]) element;
              byteBuffer.putInt(bytes.length);
              byteBuffer.put(bytes);
            }
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("DataType '%s' not supported", fieldSpec.getDataType()));
        }
      }
    }
    return genericRowBytes;
  }

  /**
   * Deserializes bytes from the native order data buffer to GenericRow
   * @param dataBuffer the pinot data buffer
   * @param offset offset to begin reading from
   * @param fieldSpecs list of field specs to determine fields in deserialization
   * @param reuse GenericRow object for returning
   * @return Deserialized GenericRow
   */
  public static GenericRow deserializeGenericRow(PinotDataBuffer dataBuffer, long offset, List<FieldSpec> fieldSpecs,
      GenericRow reuse) {
    for (FieldSpec fieldSpec : fieldSpecs) {
      String fieldName = fieldSpec.getName();
      if (fieldSpec.isSingleValueField()) {
        switch (fieldSpec.getDataType().getStoredType()) {

          case INT:
            int intValue = dataBuffer.getInt(offset);
            reuse.putValue(fieldName, intValue);
            offset += Integer.BYTES;
            break;
          case LONG:
            long longValue = dataBuffer.getLong(offset);
            reuse.putValue(fieldName, longValue);
            offset += Long.BYTES;
            break;
          case FLOAT:
            float floatValue = dataBuffer.getFloat(offset);
            reuse.putValue(fieldName, floatValue);
            offset += Float.BYTES;
            break;
          case DOUBLE:
            double doubleValue = dataBuffer.getDouble(offset);
            reuse.putValue(fieldName, doubleValue);
            offset += Double.BYTES;
            break;
          case STRING:
            int stringSize = dataBuffer.getInt(offset);
            offset += Integer.BYTES;
            byte[] stringBytes = new byte[stringSize];
            dataBuffer.copyTo(offset, stringBytes);
            offset += stringSize;
            reuse.putValue(fieldName, StringUtils.decodeUtf8(stringBytes));
            break;
          case BYTES:
            int bytesSize = dataBuffer.getInt(offset);
            offset += Integer.BYTES;
            byte[] bytes = new byte[bytesSize];
            dataBuffer.copyTo(offset, bytes);
            offset += bytesSize;
            reuse.putValue(fieldName, bytes);
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("DataType '%s' not supported", fieldSpec.getDataType()));
        }
      } else {

        int numMultiValues = dataBuffer.getInt(offset);
        offset += Integer.BYTES;
        Object[] values = new Object[numMultiValues];

        switch (fieldSpec.getDataType().getStoredType()) {

          case INT:
            for (int i = 0; i < numMultiValues; i++) {
              values[i] = dataBuffer.getInt(offset);
              offset += Integer.BYTES;
            }
            break;
          case LONG:
            for (int i = 0; i < numMultiValues; i++) {
              values[i] = dataBuffer.getLong(offset);
              offset += Long.BYTES;
            }
            break;
          case FLOAT:
            for (int i = 0; i < numMultiValues; i++) {
              values[i] = dataBuffer.getFloat(offset);
              offset += Float.BYTES;
            }
            break;
          case DOUBLE:
            for (int i = 0; i < numMultiValues; i++) {
              values[i] = dataBuffer.getDouble(offset);
              offset += Double.BYTES;
            }
            break;
          case STRING:
            for (int i = 0; i < numMultiValues; i++) {
              int stringSize = dataBuffer.getInt(offset);
              offset += Integer.BYTES;
              byte[] stringBytes = new byte[stringSize];
              dataBuffer.copyTo(offset, stringBytes);
              offset += stringSize;
              values[i] = StringUtils.decodeUtf8(stringBytes);
            }
            break;
          case BYTES:
            for (int i = 0; i < numMultiValues; i++) {
              int bytesSize = dataBuffer.getInt(offset);
              offset += Integer.BYTES;
              byte[] bytes = new byte[bytesSize];
              dataBuffer.copyTo(offset, bytes);
              offset += bytesSize;
              values[i] = bytes;
            }
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("DataType '%s' not supported", fieldSpec.getDataType()));
        }
        reuse.putValue(fieldName, values);
      }
    }
    return reuse;
  }
}
