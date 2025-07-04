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
package org.apache.pinot.segment.local.segment.index.readers;

import java.math.BigDecimal;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * Extension of {@link BaseImmutableDictionary} that implements immutable dictionary for byte[] type.
 */
public class BytesDictionary extends BaseImmutableDictionary {

  public BytesDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    super(dataBuffer, length, numBytesPerValue);
  }

  @Override
  public DataType getValueType() {
    return DataType.BYTES;
  }

  @Override
  public int indexOf(ByteArray bytesValue) {
    return normalizeIndex(binarySearch(bytesValue.getBytes()));
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return binarySearch(BytesUtils.toBytes(stringValue));
  }

  @Override
  public ByteArray getMinVal() {
    return new ByteArray(getBytes(0));
  }

  @Override
  public ByteArray getMaxVal() {
    return new ByteArray(getBytes(length() - 1));
  }

  @Override
  public byte[] get(int dictId) {
    return getBytes(dictId);
  }

  @Override
  public Object getInternal(int dictId) {
    return new ByteArray(getBytes(dictId));
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
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimalUtils.deserialize(getBytes(dictId));
  }

  @Override
  public String getStringValue(int dictId) {
    return BytesUtils.toHexString(getBytes(dictId));
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return getBytes(dictId);
  }

  @Override
  public void read32BitsMurmur3HashValues(int[] dictIds, int length, int[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = get32BitsMurmur3Hash(dictIds[i], buffer);
    }
  }

  @Override
  public void read64BitsMurmur3HashValues(int[] dictIds, int length, long[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = get64BitsMurmur3Hash(dictIds[i], buffer);
    }
  }

  @Override
  public void read128BitsMurmur3HashValues(int[] dictIds, int length, long[][] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = get128BitsMurmur3HashValue(dictIds[i], buffer);
    }
  }
}
