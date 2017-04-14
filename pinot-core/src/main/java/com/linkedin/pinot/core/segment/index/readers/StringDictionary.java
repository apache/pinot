/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;


public class StringDictionary extends ImmutableDictionaryReader {
  private final int lengthofMaxEntry;
  private static Charset UTF_8 = Charset.forName("UTF-8");
  private final char paddingChar;

  public StringDictionary(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    super(dataBuffer, metadata.getCardinality(), metadata.getStringColumnMaxLength());
    lengthofMaxEntry = metadata.getStringColumnMaxLength();
    paddingChar = metadata.getPaddingCharacter();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int indexOf(Object rawValue) {
    final String lookup = (String) rawValue; // This will always be a string

    byte[] lookupBytes = lookup.getBytes(UTF_8);
    if (lookupBytes.length >= lengthofMaxEntry) {
      // No need to pad the string
      return stringIndexOf(lookup);
    }

    // Need to pad the string before looking up
    byte[] dest = new byte[lengthofMaxEntry];
    System.arraycopy(lookupBytes, 0, dest, 0, lookupBytes.length);
    Arrays.fill(dest, lookupBytes.length, dest.length, (byte) paddingChar);
    return stringIndexOf(new String(dest, UTF_8));
  }

  @Override
  public String get(int dictionaryId) {
    if ((dictionaryId == -1) || (dictionaryId >= length())) {
      return "null";
    }
    byte[] bytes = dataFileReader.getBytes(dictionaryId, 0);
    for (int i = lengthofMaxEntry - 1; i >= 0; i--) {
      if (bytes[i] != paddingChar) {
        return new String(bytes, 0, i + 1, UTF_8);
      }
    }
    return "";
  }

  @Override
  public long getLongValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to long");
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to double");
  }

  @Override
  public int getIntValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to int");
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to float");
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return getString(dictionaryId);
  }

  @Override
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to int");
  }

  @Override
  public void readLongValues(int[] dictionaryIds, int startPos, int limit, long[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to long");
  }

  @Override
  public void readFloatValues(int[] dictionaryIds, int startPos, int limit, float[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to float");
  }

  @Override
  public void readDoubleValues(int[] dictionaryIds, int startPos, int limit, double[] outValues, int outStartPos) {
    throw new RuntimeException("Can not convert string to double");
  }

  @Override
  public void readStringValues(int[] dictionaryIds, int startPos, int limit, String[] outValues, int outStartPos) {
    dataFileReader.readStringValues(dictionaryIds, 0/*column*/, startPos, limit, outValues, outStartPos);
    int outEndPos = outStartPos + limit;
    for (int i = outStartPos; i < outEndPos; i++) {
      String val = outValues[i];
      byte[] bytes = val.getBytes(UTF_8);
      for (int j = 0; j < lengthofMaxEntry; j++) {
        if (bytes[j] == paddingChar) {
          outValues[i] = new String(bytes, 0, j, UTF_8);
          break;
        }
      }
    }
  }

  private String getString(int dictionaryId) {
    return dataFileReader.getString(dictionaryId, 0);
  }
}
