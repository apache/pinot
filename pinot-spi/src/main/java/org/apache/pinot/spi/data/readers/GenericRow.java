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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public class GenericRow {

  /**
   * This key is used by a Decoder/RecordReader to handle 1 record to many records flattening.
   * If a Decoder/RecordReader produces multiple GenericRows from the given record, they must be put into the destination GenericRow as a List<GenericRow> with this key
   * The segment generation drivers handle this key as a special case and process the multiple records
   */
  public static final String MULTIPLE_RECORDS_KEY = "$MULTIPLE_RECORDS_KEY$";
  /**
   * This key is used by the FilterTransformer to skip records during ingestion
   * The FilterTransformer puts this key into the GenericRow with value true, if the record matches the filtering criteria, based on FilterConfig
   */
  public static final String SKIP_RECORD_KEY = "$SKIP_RECORD_KEY$";

  private final Map<String, Object> _fieldToValueMap = new HashMap<>();
  private final Set<String> _nullValueFields = new HashSet<>();

  /**
   * Initializes the generic row from the given generic row (shallow copy). The row should be new created or cleared
   * before calling this method.
   */
  public void init(GenericRow row) {
    _fieldToValueMap.putAll(row._fieldToValueMap);
    _nullValueFields.addAll(row._nullValueFields);
  }

  /**
   * Returns the map from fields to values.
   * <p>Before setting the {@code defaultNullValue} for a field by calling {@link #putDefaultNullValue(String, Object)},
   * the value for the field can be {@code null}.
   */
  public Map<String, Object> getFieldToValueMap() {
    return Collections.unmodifiableMap(_fieldToValueMap);
  }

  /**
   * Returns the fields with {@code null} value.
   * <p>The {@code nullField} will be set when setting the {@code nullDefaultValue} for field by calling
   * {@link #putDefaultNullValue(String, Object)}.
   */
  public Set<String> getNullValueFields() {
    return Collections.unmodifiableSet(_nullValueFields);
  }

  /**
   * Returns the value for the given field.
   * <p>Before setting the {@code defaultNullValue} for a field by calling {@link #putDefaultNullValue(String, Object)},
   * the value for the field can be {@code null}.
   */
  public Object getValue(String fieldName) {
    return _fieldToValueMap.get(fieldName);
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

  /**
   * Sets the value for the given field.
   */
  public void putValue(String fieldName, @Nullable Object value) {
    _fieldToValueMap.put(fieldName, value);
  }

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
   * Sets the {@code defaultNullValue} for the given {@code nullField}.
   */
  public void putDefaultNullValue(String fieldName, Object defaultNullValue) {
    _fieldToValueMap.put(fieldName, defaultNullValue);
    _nullValueFields.add(fieldName);
  }

  /**
   * Removes all the fields from the row.
   */
  public void clear() {
    _fieldToValueMap.clear();
    _nullValueFields.clear();
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(_fieldToValueMap.hashCode(), _nullValueFields.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof GenericRow) {
      GenericRow that = (GenericRow) obj;
      return _fieldToValueMap.equals(that._fieldToValueMap) && _nullValueFields.equals(that._nullValueFields);
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
    _fieldToValueMap.putAll(fieldToValueMap);
  }

  @Deprecated
  @JsonIgnore
  public Set<Map.Entry<String, Object>> getEntrySet() {
    return _fieldToValueMap.entrySet();
  }

  @Deprecated
  @JsonIgnore
  public String[] getFieldNames() {
    return _fieldToValueMap.keySet().toArray(new String[0]);
  }

  @Deprecated
  public void putField(String fieldName, @Nullable Object value) {
    _fieldToValueMap.put(fieldName, value);
  }

  @Deprecated
  public static GenericRow fromBytes(byte[] buffer)
      throws IOException {
    Map<String, Object> fieldMap = JsonUtils.bytesToObject(buffer, Map.class);
    GenericRow genericRow = new GenericRow();
    genericRow.init(fieldMap);
    return genericRow;
  }

  @Deprecated
  public byte[] toBytes()
      throws IOException {
    return JsonUtils.objectToBytes(_fieldToValueMap);
  }

  @Deprecated
  public static GenericRow createOrReuseRow(GenericRow row) {
    if (row == null) {
      return new GenericRow();
    } else {
      row.clear();
      return row;
    }
  }
}
