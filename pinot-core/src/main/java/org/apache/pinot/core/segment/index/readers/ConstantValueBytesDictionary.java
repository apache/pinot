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
package org.apache.pinot.core.segment.index.readers;

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * Dictionary of a single bytes ({@code byte[]}) value.
 */
public class ConstantValueBytesDictionary extends BaseImmutableDictionary {
  private final byte[] _value;

  public ConstantValueBytesDictionary(byte[] value) {
    super(1);
    _value = value;
  }

  @Override
  public DataType getValueType() {
    return DataType.BYTES;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int result = ByteArray.compare(BytesUtils.toBytes(stringValue), _value);
    if (result < 0) {
      return -1;
    }
    if (result > 0) {
      return -2;
    }
    return 0;
  }

  @Override
  public ByteArray getMinVal() {
    return new ByteArray(_value);
  }

  @Override
  public ByteArray getMaxVal() {
    return new ByteArray(_value);
  }

  @Override
  public Object getSortedValues() {
    return new ByteArray[]{new ByteArray(_value)};
  }

  @Override
  public byte[] get(int dictId) {
    return _value;
  }

  @Override
  public Object getInternal(int dictId) {
    return new ByteArray(_value);
  }

  @Override
  public int getIntValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int dictId) {
    return BytesUtils.toHexString(_value);
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _value;
  }
}
