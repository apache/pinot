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
package org.apache.pinot.segment.local.segment.missing;

import java.io.IOException;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;


final class MissingColumnValueReader implements ForwardIndexReader<ForwardIndexReaderContext> {

  private static final byte[] MISSING_BYTES = new byte[0];
  private static final int MISSING_INT = 0;
  private static final long MISSING_LONG = 0;
  private static final float MISSING_FLOAT = Float.NaN;
  private static final double MISSING_DOUBLE = Double.NaN;
  private static final String MISSING_STRING = "";

  private final FieldSpec _fieldSpec;

  MissingColumnValueReader(FieldSpec fieldSpec) {
    _fieldSpec = fieldSpec;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return _fieldSpec.isSingleValueField();
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _fieldSpec.getDataType();
  }

  @Override
  public void close()
      throws IOException {

  }

  @Override
  public int getInt(int docId, ForwardIndexReaderContext context) {
    return MISSING_INT;
  }

  @Override
  public long getLong(int docId, ForwardIndexReaderContext context) {
    return MISSING_LONG;
  }

  @Override
  public float getFloat(int docId, ForwardIndexReaderContext context) {
    return MISSING_FLOAT;
  }

  @Override
  public double getDouble(int docId, ForwardIndexReaderContext context) {
    return MISSING_DOUBLE;
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    return MISSING_STRING;
  }

  @Override
  public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
    return MISSING_BYTES;
  }
}
