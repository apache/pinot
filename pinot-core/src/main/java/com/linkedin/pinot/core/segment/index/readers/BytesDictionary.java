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

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Extension of {@link ImmutableDictionaryReader} that implements immutable dictionary for byte[] type.
 */
public class BytesDictionary extends ImmutableDictionaryReader {

  public BytesDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    super(dataBuffer, length, numBytesPerValue, (byte) 0);
  }

  @Override
  public int indexOf(Object rawValue) {
    int index = insertionIndexOf(rawValue);
    return (index >= 0) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    return binarySearch((byte[]) rawValue);
  }

  @Override
  public byte[] get(int dictId) {
    return getBytes(dictId, getBuffer());
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return getBytes(dictId, getBuffer());
  }

  @Override
  public void readBytesValues(int[] dictIds, int inStartPos, int length, byte[][] outValues, int outStartPos) {
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = getBytes(dictIds[i], getBuffer());
    }
  }
}
