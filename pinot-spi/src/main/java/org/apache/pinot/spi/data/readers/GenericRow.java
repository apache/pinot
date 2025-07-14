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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * The generic row is the value holder returned from {@link RecordReader#next()} and
 * {RecordReader#next(GenericRow)}, and can be modified with {RecordTransformer}. The generic row returned
 * from the {NullValueTransformer} should have {@code defaultNullValue} filled to the fields with {@code null}
 * value, so that for fields with {@code null} value, {@link #getValue(String)} will return the {@code defaultNullValue}
 * and {@link #isNullValue(String)} will return {@code true}.
 *
 * The fixed set of allowed data types for the fields in the GenericRow should be:
 * Integer, Long, Float, Double, String, byte[], Object[] of the single-value types
 * This is the fixed set of data types to be used by RecordExtractor and RecordReader to extract fields from the row,
 * and by the ExpressionEvaluator to evaluate the result
 * FIXME: Based on the current behavior, we support the following data types:
 *  SV: Boolean, Byte, Character, Short, Integer, Long, Float, Double, String, byte[]
 *  MV: Object[] or List of Byte, Character, Short, Integer, Long, Float, Double, String
 *  We should not be using Boolean, Byte, Character and Short to keep it simple
 */
public class GenericRow implements Serializable {

  /// This key is used by [org.apache.pinot.spi.stream.StreamMessageDecoder] to handle the case of single stream message
  /// being decoded into multiple records. If a decoder produces multiple records from a single stream message, they
  /// must be put into the destination [GenericRow] as a [List<GenericRow>] with this key.
  /// TODO: Remove this special key and change decoder interface to return a list of records instead of a single record.
  public static final String MULTIPLE_RECORDS_KEY = "$MULTIPLE_RECORDS_KEY$";

  /// This key is used by [org.apache.pinot.spi.stream.StreamMessageDecoder] to handle the case of a stream message
  /// being decoded into zero record. If a decoder produces no record from a stream message, it can either return `null`
  /// or put `true` for this key into the destination [GenericRow].
  /// TODO: Remove this special key and change decoder interface to return a list of records instead of a single record.
  public static final String SKIP_RECORD_KEY = "$SKIP_RECORD_KEY$";

  private final Map<String, Object> _fieldToValueMap = new HashMap<>();
  private final Set<String> _nullValueFields = new HashSet<>();
  private boolean _incomplete;
  private boolean _sanitized;

  /**
   * Initializes the generic row from the given generic row (shallow copy). The row should be new created or cleared
   * before calling this method.
   */
  public void init(GenericRow row) {
    _fieldToValueMap.putAll(row._fieldToValueMap);
    _nullValueFields.addAll(row._nullValueFields);
    _incomplete = row._incomplete;
    _sanitized = row._sanitized;
  }

  /**
   * Returns the map from fields to values.
   * <p>Before setting the {@code defaultNullValue} for a field by calling {@link #putDefaultNullValue(String, Object)},
   * the value for the field can be {@code null}.
   */
  public Map<String, Object> getFieldToValueMap() {
    return _fieldToValueMap;
  }

  /**
   * Returns the fields with {@code null} value.
   * <p>The {@code nullField} will be set when setting the {@code nullDefaultValue} for field by calling
   * {@link #putDefaultNullValue(String, Object)}.
   */
  public Set<String> getNullValueFields() {
    return _nullValueFields;
  }

  /**
   * Returns the value for the given field.
   * <p>Before setting the {@code defaultNullValue} for a field by calling {@link #putDefaultNullValue(String, Object)},
   * the value for the field can be {@code null}.
   */
  public Object getValue(String fieldName) {
    return _fieldToValueMap.get(fieldName);
  }

  /// Constructs a [PrimaryKey] from the given list of primary key columns.
  public PrimaryKey getPrimaryKey(List<String> primaryKeyColumns) {
    int numPrimaryKeyColumns = primaryKeyColumns.size();
    Object[] values = new Object[numPrimaryKeyColumns];
    for (int i = 0; i < numPrimaryKeyColumns; i++) {
      Object value = getValue(primaryKeyColumns.get(i));
      if (value instanceof byte[]) {
        value = new ByteArray((byte[]) value);
      }
      values[i] = value;
    }
    return new PrimaryKey(values);
  }

  /**
   * Returns whether the value is {@code null} for the given field.
   * <p>The {@code nullField} will be set when setting the {@code nullDefaultValue} for field by calling
   * {@link #putDefaultNullValue(String, Object)}.
   */
  public boolean isNullValue(String fieldName) {
    return _nullValueFields.contains(fieldName);
  }

  /**
   * Returns whether this row has null values for any of the columns
   */
  public boolean hasNullValues() {
    return !_nullValueFields.isEmpty();
  }

  /// Returns `true` if the row has been marked as incomplete.
  /// A row is marked as incomplete when errors occurred during record transform, and default/null value has been put in
  /// place of original value.
  public boolean isIncomplete() {
    return _incomplete;
  }

  /// Returns `true` if the row has been sanitized by SanitizationTransformer.
  public boolean isSanitized() {
    return _sanitized;
  }

  /**
   * @return a deep copy of the generic row
   */
  public GenericRow copy() {
    GenericRow copy = new GenericRow();
    copy.init(this);
    for (Map.Entry<String, Object> entry : copy._fieldToValueMap.entrySet()) {
      entry.setValue(copy(entry.getValue()));
    }
    return copy;
  }

  /**
   * @return a deep copy of the generic row for the given fields
   */
  public GenericRow copy(List<String> fieldsToCopy) {
    GenericRow copy = new GenericRow();
    for (String field : fieldsToCopy) {
      copy.putValue(field, copy(getValue(field)));
    }
    return copy;
  }

  /**
   * @return a deep copy of the object.
   */
  private Object copy(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Map) {
      Map<String, Object> map = new HashMap<>((Map<String, Object>) value);
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        entry.setValue(copy(entry.getValue()));
      }
      return map;
    } else if (value instanceof Collection) {
      List list = new ArrayList(((Collection) value).size());
      for (Object object : (Collection) value) {
        list.add(copy(object));
      }
      return list;
    } else if (value.getClass().isArray()) {
      if (value instanceof byte[]) {
        return ((byte[]) value).clone();
      }
      Object[] array = new Object[((Object[]) value).length];
      int idx = 0;
      for (Object object : (Object[]) value) {
        array[idx++] = copy(object);
      }
      return array;
    } else {
      // other values are of primitive type
      return value;
    }
  }

  /**
   * Sets the value for the given field.
   */
  public void putValue(String fieldName, @Nullable Object value) {
    _fieldToValueMap.put(fieldName, value);
  }

  /// Sets the values per the given map from fields to values.
  public void putValues(Map<String, Object> fieldToValueMap) {
    _fieldToValueMap.putAll(fieldToValueMap);
  }

  /// Removes the value for the given field.
  public Object removeValue(String fieldName) {
    return _fieldToValueMap.remove(fieldName);
  }

  /**
   * Sets the {@code defaultNullValue} for the given {@code nullField}.
   */
  public void putDefaultNullValue(String fieldName, Object defaultNullValue) {
    _fieldToValueMap.put(fieldName, defaultNullValue);
    _nullValueFields.add(fieldName);
  }

  /**
   * Marks a field as {@code null}.
   */
  public void addNullValueField(String fieldName) {
    _nullValueFields.add(fieldName);
  }

  /**
   * Marks a field as {@code non-null} and returns whether the field was marked as {@code null}.
   */
  public boolean removeNullValueField(String fieldName) {
    return _nullValueFields.remove(fieldName);
  }

  /// Marks the row as incomplete.
  public void markIncomplete() {
    _incomplete = true;
  }

  /// Marks the row as sanitized.
  public void markSanitized() {
    _sanitized = true;
  }

  /**
   * Removes all the fields from the row.
   */
  public void clear() {
    _fieldToValueMap.clear();
    _nullValueFields.clear();
    _incomplete = false;
    _sanitized = false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fieldToValueMap, _nullValueFields, _incomplete, _sanitized);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof GenericRow) {
      GenericRow that = (GenericRow) obj;
      return _incomplete == that._incomplete && _sanitized == that._sanitized && _nullValueFields.equals(
          that._nullValueFields) && EqualityUtils.isEqual(_fieldToValueMap, that._fieldToValueMap);
    }
    return false;
  }

  @Override
  public String toString() {
    try {
      return JsonUtils.objectToPrettyString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Deprecated
  public void init(Map<String, Object> fieldToValueMap) {
    putValues(fieldToValueMap);
  }

  @Deprecated
  public void putField(String fieldName, @Nullable Object value) {
    putValue(fieldName, value);
  }
}
