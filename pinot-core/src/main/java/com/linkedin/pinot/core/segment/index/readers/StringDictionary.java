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

import com.linkedin.pinot.core.io.util.ValueReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


public class StringDictionary extends ImmutableDictionaryReader {

  public StringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    super(dataBuffer, length, numBytesPerValue, paddingByte);
  }

  public StringDictionary(ValueReader valueReader, int length) {
    super(valueReader, length);
  }

  @Override
  public int indexOf(Object rawValue) {
    int index = insertionIndexOf(rawValue);
    return (index >= 0) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    return binarySearch((String) rawValue);
  }

  @Override
  public String get(int dictId) {
    return getUnpaddedString(dictId, getBuffer());
  }

  @Override
  public String getStringValue(int dictId) {
    return getUnpaddedString(dictId, getBuffer());
  }

  @Override
  public void readStringValues(int[] dictIds, int inStartPos, int length, String[] outValues, int outStartPos) {
    byte[] buffer = getBuffer();
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = getUnpaddedString(dictIds[i], buffer);
    }
  }
}
