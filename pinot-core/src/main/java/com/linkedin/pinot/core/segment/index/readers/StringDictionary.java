/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.Str.STRING_PAD_CHAR;

public class StringDictionary extends ImmutableDictionaryReader {
  private final int lengthofMaxEntry;
  private static Charset UTF_8 = Charset.forName("UTF-8");

  public StringDictionary(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    super(dataBuffer, metadata.getCardinality(), metadata.getStringColumnMaxLength());
    lengthofMaxEntry = metadata.getStringColumnMaxLength();
  }

  @Override
  public int indexOf(Object rawValue) {
    final String lookup = rawValue.toString();
    final int differenceInLength = lengthofMaxEntry - lookup.length();
    final StringBuilder bld = new StringBuilder();
    bld.append(lookup);
    for (int i = 0; i < differenceInLength; i++) {
      bld.append(STRING_PAD_CHAR);
    }
    return stringIndexOf(bld.toString());
  }

  @Override
  public String get(int dictionaryId) {
    if ((dictionaryId == -1) || (dictionaryId >= length())) {
      return "null";
    }
    String val = getString(dictionaryId);
    byte[] bytes = val.getBytes(UTF_8);
    for (int i = 0; i < lengthofMaxEntry; i++) {
      if (bytes[i] == STRING_PAD_CHAR) {
        return new String(bytes, 0, i, UTF_8);
      }
    }
    return val;
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
  public String toString(int dictionaryId) {
    return get(dictionaryId);
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
        if (bytes[j] == STRING_PAD_CHAR) {
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
