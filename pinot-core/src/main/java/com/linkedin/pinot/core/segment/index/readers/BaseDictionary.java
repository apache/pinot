/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.readers;

public abstract class BaseDictionary implements Dictionary {

  @Override
  public Object get(int dictId) {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long readIntValues(int[] dictIds, int inStartPos, int length, int[] outValues, int outStartPos) {
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = getIntValue(dictIds[i]);
    }
    return length * Integer.BYTES;
  }

  @Override
  public long readLongValues(int[] dictIds, int inStartPos, int length, long[] outValues, int outStartPos) {
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = getLongValue(dictIds[i]);
    }
    return length * Long.BYTES;
  }

  @Override
  public long readFloatValues(int[] dictIds, int inStartPos, int length, float[] outValues, int outStartPos) {
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = getFloatValue(dictIds[i]);
    }
    return length * Float.BYTES;
  }

  @Override
  public long readDoubleValues(int[] dictIds, int inStartPos, int length, double[] outValues, int outStartPos) {
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = getDoubleValue(dictIds[i]);
    }
    return length * Double.BYTES;
  }

  @Override
  public long readStringValues(int[] dictIds, int inStartPos, int length, String[] outValues, int outStartPos) {
    long bytesRead = 0;
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      String str = getStringValue(dictIds[i]);
      outValues[outStartPos++] = str;
      // NOTE: we use string length as an approximation for bytes-read here
      // In some cases (OnHeap dictionary for example), the strings are not
      // stored as corresponding bytes - hence we rely on an approximation
      // based on string length instead of paying a decode penalty to get
      // actual bytes read Where applicable, derived-classes can override
      // and return the right values.
      bytesRead += str.length();
    }
    return bytesRead;
  }

  @Override
  public long readBytesValues(int[] dictIds, int inStartPos, int length, byte[][] outValues, int outStartPos) {
    long bytesRead = 0;
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      byte[] val = getBytesValue(dictIds[i]);
      outValues[outStartPos++] = val;
      bytesRead += val.length;
    }
    return bytesRead;
  }
}
