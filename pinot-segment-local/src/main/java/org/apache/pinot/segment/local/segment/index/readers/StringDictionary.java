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

import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;


public class StringDictionary extends BaseImmutableDictionary {

  private static final int MAX_INTERNED_BYTES = 10 * 1024 * 1024; // 10MB
  private final String[] _internTable;

  public StringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    super(dataBuffer, length, numBytesPerValue, paddingByte);
    _internTable = length * numBytesPerValue < MAX_INTERNED_BYTES ? new String[length] : null;
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
    return internStringValue(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return Integer.parseInt(internStringValue(dictId));
  }

  @Override
  public long getLongValue(int dictId) {
    return Long.parseLong(internStringValue(dictId));
  }

  @Override
  public float getFloatValue(int dictId) {
    return Float.parseFloat(internStringValue(dictId));
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(internStringValue(dictId));
  }

  @Override
  public String getStringValue(int dictId) {
    return internStringValue(dictId);
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return BytesUtils.toBytes(internStringValue(dictId, getBuffer()));
  }

  @Override
  public void readIntValues(int[] dictIds, int length, int[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Integer.parseInt(internStringValue(dictIds[i], buffer));
    }
  }

  @Override
  public void readLongValues(int[] dictIds, int length, long[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Long.parseLong(internStringValue(dictIds[i], buffer));
    }
  }

  @Override
  public void readFloatValues(int[] dictIds, int length, float[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Float.parseFloat(internStringValue(dictIds[i], buffer));
    }
  }

  @Override
  public void readDoubleValues(int[] dictIds, int length, double[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = Double.parseDouble(internStringValue(dictIds[i], buffer));
    }
  }

  @Override
  public void readStringValues(int[] dictIds, int length, String[] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = internStringValue(dictIds[i], buffer);
    }
  }

  @Override
  public void readBytesValues(int[] dictIds, int length, byte[][] outValues) {
    byte[] buffer = getBuffer();
    for (int i = 0; i < length; i++) {
      outValues[i] = BytesUtils.toBytes(internStringValue(dictIds[i], buffer));
    }
  }

  private String internStringValue(int dictId) {
    if (_internTable == null) {
      return getUnpaddedString(dictId, getBuffer());
    }
    String interned = _internTable[dictId];
    if (interned == null) {
      interned = getUnpaddedString(dictId, getBuffer());
      _internTable[dictId] = interned;
    }
    return interned;
  }

  private String internStringValue(int dictId, byte[] buffer) {
    if (_internTable == null) {
      return getUnpaddedString(dictId, buffer);
    }
    String interned = _internTable[dictId];
    if (interned == null) {
      interned = getUnpaddedString(dictId, buffer);
      _internTable[dictId] = interned;
    }
    return interned;
  }

  @Override
  public void close()
      throws IOException {
    if (_internTable != null) {
      Arrays.fill(_internTable, null);
    }
    super.close();
  }
}
