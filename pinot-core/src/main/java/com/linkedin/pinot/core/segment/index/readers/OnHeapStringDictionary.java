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

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.util.Arrays;


/**
 * Implementation of String dictionary that cache all values on-heap.
 * <p>This is useful for String columns that:
 * <ul>
 *   <li>Has low cardinality string dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 * <p>This helps avoid creation of String from byte[], which is expensive as well as creates garbage.
 */
public class OnHeapStringDictionary extends ImmutableDictionaryReader {
  private final byte _paddingByte;
  private final String[] _unpaddedStrings;
  private final String[] _paddedStrings;

  public OnHeapStringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    super(dataBuffer, length, numBytesPerValue, paddingByte);

    _paddingByte = paddingByte;
    byte[] buffer = new byte[numBytesPerValue];
    _unpaddedStrings = new String[length];
    for (int i = 0; i < length; i++) {
      _unpaddedStrings[i] = getUnpaddedString(i, buffer);
    }
    if (paddingByte == 0) {
      _paddedStrings = null;
    } else {
      _paddedStrings = new String[length];
      for (int i = 0; i < length; i++) {
        _paddedStrings[i] = getPaddedString(i, buffer);
      }
    }
  }

  @Override
  public int indexOf(Object rawValue) {
    if (_paddingByte == 0) {
      return Arrays.binarySearch(_unpaddedStrings, rawValue);
    } else {
      String paddedValue = addPadding((String) rawValue);
      return Arrays.binarySearch(_paddedStrings, paddedValue);
    }
  }

  @Override
  public String get(int dictId) {
    return _unpaddedStrings[dictId];
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
    return _unpaddedStrings[dictId];
  }
}
