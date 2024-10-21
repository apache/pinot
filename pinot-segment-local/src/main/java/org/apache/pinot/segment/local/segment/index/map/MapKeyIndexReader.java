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
import java.util.Map;
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
    return _keyFieldSpec.getDataType();
  }

  @Override
  public int getInt(int docId, ForwardIndexReaderContext context) {
    return Integer.parseInt(extractMapValue(docId, context, _keyName).toString());
  }

  @Override
  public long getLong(int docId, ForwardIndexReaderContext context) {
    return Long.parseLong(extractMapValue(docId, context, _keyName).toString());
  }

  @Override
  public float getFloat(int docId, ForwardIndexReaderContext context) {
    return Float.parseFloat(extractMapValue(docId, context, _keyName).toString());
  }

  @Override
  public double getDouble(int docId, ForwardIndexReaderContext context) {
    return Double.parseDouble(extractMapValue(docId, context, _keyName).toString());
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    return extractMapValue(docId, context, _keyName).toString();
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
    Map map = _forwardIndexReader.getMap(docId, context);
    Object object = map.get(key);
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
