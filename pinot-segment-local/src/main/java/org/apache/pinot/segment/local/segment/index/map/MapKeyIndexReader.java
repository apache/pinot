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
package org.apache.pinot.segment.local.segment.index.map;

import java.io.IOException;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BigDecimalUtils;


public class MapKeyIndexReader implements ForwardIndexReader {
  private final ForwardIndexReader _forwardIndexReader;
  private final FieldSpec _keyFieldSpec;
  private final String _keyName;
  private final Object _defaultNullValue;

  public MapKeyIndexReader(ForwardIndexReader forwardIndexReader, String keyName, FieldSpec keyFieldSpec) {
    _forwardIndexReader = forwardIndexReader;
    _keyName = keyName;
    _keyFieldSpec = keyFieldSpec;
    _defaultNullValue = keyFieldSpec.getDefaultNullValue();
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getStoredType() {
    return _keyFieldSpec.getDataType().getStoredType();
  }

  // The numeric accessors below fast-path the type Jackson already produced for this JSON shape - Integer for a
  // small integer, Long for a large one, Double for a decimal - instead of formatting it to a string and reparsing.
  // Any other type still goes through the string round trip, so a value that does not match the declared type
  // fails exactly as it did before rather than being silently coerced.

  @Override
  public int getInt(int docId, ForwardIndexReaderContext context) {
    Object value = extractMapValue(docId, context, _keyName);
    return value instanceof Integer ? (Integer) value : Integer.parseInt(value.toString());
  }

  @Override
  public long getLong(int docId, ForwardIndexReaderContext context) {
    Object value = extractMapValue(docId, context, _keyName);
    if (value instanceof Long) {
      return (Long) value;
    }
    return value instanceof Integer ? (Integer) value : Long.parseLong(value.toString());
  }

  @Override
  public float getFloat(int docId, ForwardIndexReaderContext context) {
    Object value = extractMapValue(docId, context, _keyName);
    return value instanceof Float ? (Float) value : Float.parseFloat(value.toString());
  }

  @Override
  public double getDouble(int docId, ForwardIndexReaderContext context) {
    Object value = extractMapValue(docId, context, _keyName);
    return value instanceof Double ? (Double) value : Double.parseDouble(value.toString());
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    String value = _forwardIndexReader.getMapValueAsString(docId, context, _keyName);
    return value != null ? value : _defaultNullValue.toString();
  }

  @Override
  public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
    return (byte[]) extractMapValue(docId, context, _keyName);
  }

  @Override
  public BigDecimal getBigDecimal(int docId, ForwardIndexReaderContext context) {
    return BigDecimalUtils.deserialize((byte[]) extractMapValue(docId, context, _keyName));
  }

  private Object extractMapValue(int docId, ForwardIndexReaderContext context, String key) {
    Object object = _forwardIndexReader.getMapValue(docId, context, key);
    if (object == null) {
      return _defaultNullValue;
    }
    return object;
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  @Nullable
  public ForwardIndexReaderContext createContext() {
    return _forwardIndexReader.createContext();
  }
}
