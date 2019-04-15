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
package org.apache.pinot.core.segment.index.readers;

import java.util.Arrays;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public class OnHeapTrieBasedStringDictionary extends OnHeapTrieBasedDictionary {
  private final byte _paddingByte;
  private final String[] _unpaddedStrings;
  private final String[] _paddedStrings;
  private final PatriciaTrie<Integer> _paddedStringToIdTrie;
  private final PatriciaTrie<Integer> _unpaddedStringToIdTrie;

  public OnHeapTrieBasedStringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue,
      byte paddingByte) {
    super(dataBuffer, length, numBytesPerValue, paddingByte);

    _paddingByte = paddingByte;
    byte[] buffer = new byte[numBytesPerValue];
    _unpaddedStrings = new String[length];
    _unpaddedStringToIdTrie = new PatriciaTrie<>();

    for (int i = 0; i < length; i++) {
      _unpaddedStrings[i] = getUnpaddedString(i, buffer);
      _unpaddedStringToIdTrie.put(_unpaddedStrings[i], i);
    }

    if (_paddingByte == 0) {
      _paddedStrings = null;
      _paddedStringToIdTrie = null;
    } else {
      _paddedStrings = new String[length];
      _paddedStringToIdTrie = new PatriciaTrie<>();

      for (int i = 0; i < length; i++) {
        _paddedStrings[i] = getPaddedString(i, buffer);
        _paddedStringToIdTrie.put(_paddedStrings[i], i);
      }
    }
  }

  @Override
  public int indexOf(Object rawValue) {
    PatriciaTrie<Integer> stringToIdTrie = (_paddingByte == 0) ? _unpaddedStringToIdTrie : _paddedStringToIdTrie;
    Integer index = stringToIdTrie.get(rawValue);
    return (index != null) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    if (_paddingByte == 0) {
      Integer id = _unpaddedStringToIdTrie.get(rawValue);
      return (id != null) ? id : Arrays.binarySearch(_unpaddedStrings, rawValue);
    } else {
      String paddedValue = padString((String) rawValue);
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
