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

import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;


public class StringDictionary extends BaseImmutableDictionary {

  public StringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    super(dataBuffer, length, numBytesPerValue, paddingByte);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return binarySearch(stringValue);
  }

  @Override
  public DataType getValueType() {
    return DataType.STRING;
  }

  @Override
  public String get(int dictId) {
    return getUnpaddedString(dictId, getBuffer());
  }

  @Override
  public int getIntValue(int dictId) {
    return Integer.parseInt(getUnpaddedString(dictId, getBuffer()));
  }

  @Override
  public long getLongValue(int dictId) {
    return Long.parseLong(getUnpaddedString(dictId, getBuffer()));
  }

  @Override
  public float getFloatValue(int dictId) {
    return Float.parseFloat(getUnpaddedString(dictId, getBuffer()));
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(getUnpaddedString(dictId, getBuffer()));
  }

  @Override
  public String getStringValue(int dictId) {
    return getUnpaddedString(dictId, getBuffer());
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return BytesUtils.toBytes(getUnpaddedString(dictId, getBuffer()));
  }

  @Override
  public void readIntValues(int[] dictIds, int length, int[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Integer.parseInt(getUnpaddedString(dictIds[i], buffer));
    }
  }

  @Override
  public void readLongValues(int[] dictIds, int length, long[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Long.parseLong(getUnpaddedString(dictIds[i], buffer));
    }
  }

  @Override
  public void readFloatValues(int[] dictIds, int length, float[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Float.parseFloat(getUnpaddedString(dictIds[i], buffer));
    }
  }

  @Override
  public void readDoubleValues(int[] dictIds, int length, double[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Double.parseDouble(getUnpaddedString(dictIds[i], buffer));
    }
  }

  @Override
  public void readStringValues(int[] dictIds, int length, String[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = getUnpaddedString(dictIds[i], buffer);
    }
  }

  @Override
  public void readBytesValues(int[] dictIds, int length, byte[][] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = BytesUtils.toBytes(getUnpaddedString(dictIds[i], buffer));
    }
  }
}
