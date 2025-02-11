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

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;


/**
 * Base abstract class for extracting and converting the fields of various data formats into supported Pinot data types.
 *
 * @param <T> the format of the input record
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseRecordExtractor<T> implements RecordExtractor<T> {

  /**
   * Converts the field value to either a single value (string, number, byte[]), multi value (Object[]) or a Map.
   */
  public Object convert(Object value) {
    Object convertedValue;
    if (isMultiValue(value)) {
      convertedValue = convertMultiValue(value);
    } else if (isMap(value)) {
      convertedValue = convertMap(value);
    } else if (isRecord(value)) {
      convertedValue = convertRecord(value);
    } else {
      convertedValue = convertSingleValue(value);
    }
    return convertedValue;
  }

  /**
   * Returns whether the object is of the data format's base type. Override this method if the extractor
   * can handle the conversion of nested record types.
   */
  protected boolean isRecord(Object value) {
    return false;
  }

  /**
   * Returns whether the object is of a multi-value type. Override this method if the data format represents
   * multi-value objects differently.
   */
  protected boolean isMultiValue(Object value) {
    return value instanceof Collection || (value.getClass().isArray() && !(value instanceof byte[]));
  }

  /**
   * Returns whether the object is of a map type. Override this method if the data format represents map objects
   * differently.
   */
  protected boolean isMap(Object value) {
    return value instanceof Map;
  }

  /**
   * Handles the conversion of every field of the object for the particular data format. Override this method if the
   * extractor can convert nested record types.
   *
   * @param value should be verified to be a record type prior to calling this method
   */
  protected Map<Object, Object> convertRecord(Object value) {
    throw new UnsupportedOperationException("Extractor cannot convert record type structures for this data format.");
  }

  /**
   * Handles the conversion of each element of a multi-value object, and returns an Object array. Override this method
   * if the data format requires a different conversion for its multi-value objects.
   *
   * @param value should be verified to be a multi-value type prior to calling this method
   */
  protected Object[] convertMultiValue(Object value) {
    if (value instanceof Collection) {
      return convertCollection((Collection) value);
    }
    if (value instanceof Object[]) {
      return convertArray((Object[]) value);
    }
    return convertPrimitiveArray(value);
  }

  protected Object[] convertCollection(Collection collection) {
    int numValues = collection.size();
    Object[] convertedValues = new Object[numValues];
    int index = 0;
    for (Object value : collection) {
      Object convertedValue = value != null ? convert(value) : null;
      convertedValues[index++] = convertedValue;
    }
    return convertedValues;
  }

  protected Object[] convertArray(Object[] array) {
    int numValues = array.length;
    Object[] convertedValues = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      Object value = array[i];
      Object convertedValue = value != null ? convert(value) : null;
      convertedValues[i] = convertedValue;
    }
    return convertedValues;
  }

  protected Object[] convertPrimitiveArray(Object array) {
    if (array instanceof int[]) {
      return ArrayUtils.toObject((int[]) array);
    }
    if (array instanceof long[]) {
      return ArrayUtils.toObject((long[]) array);
    }
    if (array instanceof float[]) {
      return ArrayUtils.toObject((float[]) array);
    }
    if (array instanceof double[]) {
      return ArrayUtils.toObject((double[]) array);
    }
    throw new IllegalArgumentException("Unsupported primitive array type: " + array.getClass().getName());
  }

  /**
   * Handles the conversion of every value of the map. Note that map keys will be handled as a single-value type. This
   * should be overridden if the data format requires a different conversion for map values.
   *
   * @param value should be verified to be a Map type prior to calling this method
   */
  protected Map<Object, Object> convertMap(Object value) {
    Map<Object, Object> map = (Map) value;
    Map<Object, Object> convertedMap = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      Object mapKey = entry.getKey();
      if (mapKey != null) {
        Object mapValue = entry.getValue();
        Object convertedMapValue = mapValue != null ? convert(mapValue) : null;
        convertedMap.put(convertSingleValue(entry.getKey()), convertedMapValue);
      }
    }
    return convertedMap;
  }

  /**
   * Converts single value types. This should be overridden if the data format requires
   * a different conversion for its single values.
   */
  protected Object convertSingleValue(Object value) {
    if (value instanceof ByteBuffer) {
      // NOTE: ByteBuffer might be reused in some record reader implementation. Make a slice to ensure nothing is
      //       modified in the original buffer
      ByteBuffer slice = ((ByteBuffer) value).slice();
      byte[] bytesValue = new byte[slice.limit()];
      slice.get(bytesValue);
      return bytesValue;
    }
    if (value instanceof Number || value instanceof byte[]) {
      if (value instanceof Short) {
        return Integer.valueOf(value.toString());
      }
      return value;
    }
    return value.toString();
  }
}
