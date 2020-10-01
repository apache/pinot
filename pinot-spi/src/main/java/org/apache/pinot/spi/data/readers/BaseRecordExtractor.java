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
package org.apache.pinot.spi.data.readers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Base abstract class for extracting and converting the fields of various data formats into supported Pinot data types.
 *
 * @param <T> the format of the input record
 */
public abstract class BaseRecordExtractor<T> implements RecordExtractor<T> {

  /**
   * Converts the field value to either a single value (string, number, byte[]), multi value (Object[]) or a Map.
   * Returns {@code null} if the value is an empty array/collection/map.
   *
   * Natively Pinot only understands single values and multi values.
   * Map is useful only if some ingestion transform functions operates on it in the transformation layer.
   */
  @Nullable
  public Object convert(Object value) {
    Object convertedValue;
    if (isInstanceOfMultiValue(value)) {
      convertedValue = convertMultiValue(value);
    } else if (isInstanceOfMap(value)) {
      convertedValue = convertMap(value);
    } else if (isInstanceOfRecord(value)) {
      convertedValue = convertRecord(value);
    } else {
      convertedValue = convertSingleValue(value);
    }
    return convertedValue;
  }

  /**
   * Returns whether the object is an instance of the data format's base type. Override this method if the extractor
   * can handle the conversion of nested record types.
   */
  protected boolean isInstanceOfRecord(Object value) {
    return false;
  }

  /**
   * Returns whether the object is of a multi-value type. Override this method if the data format represents
   * multi-value objects differently.
   */
  protected boolean isInstanceOfMultiValue(Object value) {
    return value instanceof Collection;
  }

  /**
   * Returns whether the object is of a map type. Override this method if the data format represents map objects
   * differently.
   */
  protected boolean isInstanceOfMap(Object value) {
    return value instanceof Map;
  }

  /**
   * Handles the conversion of every field of the object for the particular data format. Override this method if the
   * extractor can convert nested record types.
   */
  @Nullable
  protected Object convertRecord(Object value) {
    throw new UnsupportedOperationException("Extractor cannot convert record type structures for this data format.");
  }

  /**
   * Handles the conversion of each element of a multi-value object. Returns {@code null} if the field value is
   * {@code null}.
   *
   * This implementation converts the Collection to an Object array. Override this method if the data format
   * requires a different conversion for its multi-value objects.
   */
  @Nullable
  protected Object convertMultiValue(Object value) {
    Collection collection = (Collection) value;
    if (collection.isEmpty()) {
      return null;
    }

    int numValues = collection.size();
    Object[] array = new Object[numValues];
    int index = 0;
    for (Object element : collection) {
      Object convertedValue = null;
      if (element != null) {
        convertedValue = convert(element);
      }
      if (convertedValue != null && !convertedValue.toString().equals("")) {
        array[index++] = convertedValue;
      }
    }

    if (index == numValues) {
      return array;
    } else if (index == 0) {
      return null;
    } else {
      return Arrays.copyOf(array, index);
    }
  }

  /**
   * Handles the conversion of every value of the map. Note that map keys will be handled as a single-value type.
   * Returns {@code null} if the field value is {@code null}. This should be overridden if the data format requires
   * a different conversion for map values.
   */
  @Nullable
  protected Object convertMap(Object value) {
    Map map = (Map) value;
    if (map.isEmpty()) {
      return null;
    }

    Map<Object, Object> convertedMap = new HashMap<>();
    for (Object key : map.keySet()) {
      Object convertedValue = null;
      if (key != null) {
        convertedValue = convert(map.get(key));
      }
      convertedMap.put(convertSingleValue(key), convertedValue);
    }
    return convertedMap;
  }

  /**
   * Converts single value types. This should be overridden if the data format requires
   * a different conversion for its single values. Returns {@code null} for {@code null} input values.
   */
  @Nullable
  protected Object convertSingleValue(@Nullable Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof ByteBuffer) {
      ByteBuffer byteBufferValue = (ByteBuffer) value;

      // Use byteBufferValue.remaining() instead of byteBufferValue.capacity() so that it still works when buffer is
      // over-sized
      byte[] bytesValue = new byte[byteBufferValue.remaining()];
      byteBufferValue.get(bytesValue);
      return bytesValue;
    }
    if (value instanceof Number) {
      return value;
    }
    return value.toString();
  }
}
