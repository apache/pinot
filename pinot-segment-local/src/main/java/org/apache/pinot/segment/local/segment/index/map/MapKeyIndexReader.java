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
import org.apache.pinot.spi.utils.MapUtils.PreparedMapKey;


public class MapKeyIndexReader implements ForwardIndexReader {
  private final ForwardIndexReader _forwardIndexReader;
  private final FieldSpec _keyFieldSpec;
  private final PreparedMapKey _mapKey;
  private final Object _defaultNullValue;

  public MapKeyIndexReader(ForwardIndexReader forwardIndexReader, String keyName, FieldSpec keyFieldSpec) {
    _forwardIndexReader = forwardIndexReader;
    _mapKey = new PreparedMapKey(keyName);
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

  @Override
  public int getInt(int docId, ForwardIndexReaderContext context) {
    Object value = extractMapValue(docId, context);
    return value instanceof Integer ? (Integer) value : Integer.parseInt(value.toString());
  }

  @Override
  public long getLong(int docId, ForwardIndexReaderContext context) {
    Object value = extractMapValue(docId, context);
    if (value instanceof Long) {
      return (Long) value;
    }
    return value instanceof Integer ? (Integer) value : Long.parseLong(value.toString());
  }

  /// No fast path here: Jackson's untyped binding never yields a `Float` - a JSON decimal comes back as `Double` -
  /// so a `Float` check would be dead code. Narrowing the `Double` instead is not equivalent: for a double sitting
  /// near the midpoint between two floats the two conversions differ by an ulp, because one rounds the double
  /// directly while the other rounds its shortest decimal. `-1.340092769725468E-17` narrows to `-1.3400928E-17`
  /// but parses to `-1.3400927E-17`. This method has always produced the parsed value, so it keeps doing that.
  @Override
  public float getFloat(int docId, ForwardIndexReaderContext context) {
    return Float.parseFloat(extractMapValue(docId, context).toString());
  }

  @Override
  public double getDouble(int docId, ForwardIndexReaderContext context) {
    Object value = extractMapValue(docId, context);
    return value instanceof Double ? (Double) value : Double.parseDouble(value.toString());
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    String value = _forwardIndexReader.getMapEntryValueAsString(docId, context, _mapKey);
    return value != null ? value : _defaultNullValue.toString();
  }

  @Override
  public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
    return (byte[]) extractMapValue(docId, context);
  }

  @Override
  public BigDecimal getBigDecimal(int docId, ForwardIndexReaderContext context) {
    return BigDecimalUtils.deserialize((byte[]) extractMapValue(docId, context));
  }

  private Object extractMapValue(int docId, ForwardIndexReaderContext context) {
    Object object = _forwardIndexReader.getMapEntryValue(docId, context, _mapKey);
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
