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
package org.apache.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.core.data.GenericRow;


public class RecordReaderUtils {
  private RecordReaderUtils() {
  }

  public static final String GZIP_FILE_EXTENSION = ".gz";

  public static BufferedReader getBufferedReader(File dataFile)
      throws IOException {
    return new BufferedReader(new InputStreamReader(getInputStream(dataFile), StandardCharsets.UTF_8));
  }

  public static BufferedInputStream getBufferedInputStream(File dataFile)
      throws IOException {
    return new BufferedInputStream(getInputStream(dataFile));
  }

  public static InputStream getInputStream(File dataFile)
      throws IOException {
    if (dataFile.getName().endsWith(GZIP_FILE_EXTENSION)) {
      return new GZIPInputStream(new FileInputStream(dataFile));
    } else {
      return new FileInputStream(dataFile);
    }
  }

  /**
   * Extracts all field specs from the given schema.
   * <p>For time field spec:
   * <ul>
   *   <li>If incoming and outgoing time column name are the same, use incoming time field spec</li>
   *   <li>If incoming and outgoing time column name are different, put both of them as time field spec</li>
   *   <li>
   *     We keep both incoming and outgoing time column to handle cases where the input file contains time values that
   *     are already converted
   *   </li>
   * </ul>
   */
  public static List<FieldSpec> extractFieldSpecs(Schema schema) {
    List<FieldSpec> fieldSpecs = new ArrayList<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) {
        TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
        fieldSpecs.add(new TimeFieldSpec(timeFieldSpec.getIncomingGranularitySpec()));
        if (!timeFieldSpec.getOutgoingTimeColumnName().equals(timeFieldSpec.getIncomingTimeColumnName())) {
          fieldSpecs.add(new TimeFieldSpec(timeFieldSpec.getOutgoingGranularitySpec()));
        }
      } else {
        fieldSpecs.add(fieldSpec);
      }
    }
    return fieldSpecs;
  }

  /**
   * Converts the value based on the given field spec.
   */
  public static Object convert(FieldSpec fieldSpec, @Nullable Object value) {
    if (fieldSpec.isSingleValueField()) {
      return convertSingleValue(fieldSpec, value);
    } else {
      return convertMultiValue(fieldSpec, (Collection) value);
    }
  }

  /**
   * Converts the value to a single-valued value based on the given field spec.
   */
  public static Object convertSingleValue(FieldSpec fieldSpec, @Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof GenericData.Record) {
      return convertSingleValue(fieldSpec, ((GenericData.Record) value).get(0));
    }
    DataType dataType = fieldSpec.getDataType();
    if (dataType == FieldSpec.DataType.BYTES) {
      // Avro ByteBuffer maps to byte[]
      if (value instanceof ByteBuffer) {
        ByteBuffer byteBufferValue = (ByteBuffer) value;

        // Use byteBufferValue.remaining() instead of byteBufferValue.capacity() so that it still works when buffer is
        // over-sized
        byte[] bytesValue = new byte[byteBufferValue.remaining()];
        byteBufferValue.get(bytesValue);
        return bytesValue;
      } else {
        Preconditions
            .checkState(value instanceof byte[], "For BYTES data type, value must be either ByteBuffer or byte[]");
        return value;
      }
    }
    if (value instanceof Number) {
      Number numberValue = (Number) value;
      switch (dataType) {
        case INT:
          return numberValue.intValue();
        case LONG:
          return numberValue.longValue();
        case FLOAT:
          return numberValue.floatValue();
        case DOUBLE:
          return numberValue.doubleValue();
        case STRING:
          return numberValue.toString();
        default:
          throw new IllegalStateException("Illegal data type: " + dataType);
      }
    }
    return convertSingleValue(fieldSpec, value.toString());
  }

  /**
   * Converts the string value to a single-valued value based on the given field spec.
   */
  public static Object convertSingleValue(FieldSpec fieldSpec, @Nullable String stringValue) {
    if (stringValue == null) {
      return null;
    }
    DataType dataType = fieldSpec.getDataType();
    // Treat empty string as null for data types other than STRING
    if (stringValue.isEmpty() && dataType != DataType.STRING) {
      return null;
    }
    switch (dataType) {
      case INT:
        return Integer.parseInt(stringValue);
      case LONG:
        return Long.parseLong(stringValue);
      case FLOAT:
        return Float.parseFloat(stringValue);
      case DOUBLE:
        return Double.parseDouble(stringValue);
      case STRING:
        return stringValue;
      default:
        throw new IllegalStateException("Illegal data type: " + dataType);
    }
  }

  /**
   * Converts the values to a multi-valued value based on the given field spec.
   */
  public static Object convertMultiValue(FieldSpec fieldSpec, @Nullable Collection values) {
    if (values == null || values.isEmpty()) {
      return null;
    } else {
      int numValues = values.size();
      Object[] array = new Object[numValues];
      int index = 0;
      for (Object value : values) {
        array[index++] = convertSingleValue(fieldSpec, value);
      }
      return array;
    }
  }

  /**
   * Converts the string values to a multi-valued value based on the given field spec.
   */
  public static Object convertMultiValue(FieldSpec fieldSpec, @Nullable String[] stringValues) {
    if (stringValues == null || stringValues.length == 0) {
      return null;
    } else {
      int numValues = stringValues.length;
      Object[] array = new Object[numValues];
      for (int i = 0; i < numValues; i++) {
        array[i] = convertSingleValue(fieldSpec, stringValues[i]);
      }
      return array;
    }
  }

  public static void copyRow(GenericRow source, GenericRow destination) {
    destination.clear();
    for (Map.Entry<String, Object> entry : source.getEntrySet()) {
      destination.putField(entry.getKey(), entry.getValue());
    }
  }
}
