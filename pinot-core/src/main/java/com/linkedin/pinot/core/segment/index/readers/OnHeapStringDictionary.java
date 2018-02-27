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
import java.util.HashMap;
import java.util.Map;


/**
 * Implementation of String dictionary that cache all values on-heap.
 * <p>This is useful for String columns that:
 * <ul>
 *   <li>Has low cardinality string dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 * <p>This helps avoid creation of String from byte[], which is expensive as well as creates garbage.
 */
public class OnHeapStringDictionary extends OnHeapDictionary {
  private final byte _paddingByte;
  private final String[] _unpaddedStrings;
  private final String[] _paddedStrings;
  private final Map<String, Integer> _paddedStringToIdMap;
  private final Map<String, Integer> _unPaddedStringToIdMap;

  public OnHeapStringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    super(dataBuffer, length, numBytesPerValue, paddingByte);

    _paddingByte = paddingByte;
    byte[] buffer = new byte[numBytesPerValue];
    _unpaddedStrings = new String[length];
    _unPaddedStringToIdMap = new HashMap<>(length);

    for (int i = 0; i < length; i++) {
      _unpaddedStrings[i] = getUnpaddedString(i, buffer);
      _unPaddedStringToIdMap.put(_unpaddedStrings[i], i);
    }

    if (paddingByte == 0) {
      _paddedStrings = null;
      _paddedStringToIdMap = null;
    } else {
      _paddedStrings = new String[length];
      _paddedStringToIdMap = new HashMap<>(length);

      for (int i = 0; i < length; i++) {
        _paddedStrings[i] = getPaddedString(i, buffer);
        _paddedStringToIdMap.put(_paddedStrings[i], i);
      }
    }
  }

  @Override
  public int indexOf(Object rawValue) {
    Map<String, Integer> stringToIdMap = (_paddingByte == 0) ? _unPaddedStringToIdMap : _paddedStringToIdMap;
    Integer index = stringToIdMap.get(rawValue);
    return (index != null) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    if (_paddingByte == 0) {
      Integer id = _unPaddedStringToIdMap.get(rawValue);
      return (id != null) ? id : Arrays.binarySearch(_unpaddedStrings, rawValue);
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
  public String getStringValue(int dictId) {
    return _unpaddedStrings[dictId];
  }
}
