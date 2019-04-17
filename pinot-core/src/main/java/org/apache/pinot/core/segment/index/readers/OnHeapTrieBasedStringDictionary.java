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

import com.ning.tr13.KeyValueSource;
import com.ning.tr13.TrieLookup;
import com.ning.tr13.build.SimpleTrieBuilder;
import com.ning.tr13.impl.bytes.ByteBufferBytesTrieLookup;
import com.ning.tr13.impl.bytes.SimpleBytesTrieBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OnHeapTrieBasedStringDictionary extends OnHeapTrieBasedDictionary {
  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapTrieBasedStringDictionary.class);
  private final byte _paddingByte;
  private final String[] _unpaddedStrings;
  private final String[] _paddedStrings;
  private final TrieLookup<byte[]> _trieLookup;

//  private final PatriciaTrie<Integer> _paddedStringToIdTrie;
//  private final PatriciaTrie<Integer> _unpaddedStringToIdTrie;

  public OnHeapTrieBasedStringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue,
      byte paddingByte) throws IOException {
    super(dataBuffer, length, numBytesPerValue, paddingByte);

    _paddingByte = paddingByte;
    byte[] buffer = new byte[numBytesPerValue];
    _unpaddedStrings = new String[length];

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    SimpleBytesTrieBuilder simpleBytesTrieBuilder = new SimpleBytesTrieBuilder(new IndexReader(dataBuffer, length, numBytesPerValue, paddingByte));
    try {
      simpleBytesTrieBuilder.buildAndWrite(byteArrayOutputStream, false);
    } catch (IOException e) {
      LOGGER.error("Exception when building trie tree!", e);
      throw e;
    }

    byte[] raw = byteArrayOutputStream.toByteArray();
    _trieLookup = new ByteBufferBytesTrieLookup(ByteBuffer.wrap(raw), raw.length);
//    _unpaddedStringToIdTrie = new PatriciaTrie<>();

    for (int i = 0; i < length; i++) {
      _unpaddedStrings[i] = getUnpaddedString(i, buffer);
//      _unpaddedStringToIdTrie.put(_unpaddedStrings[i], i);
    }

    if (_paddingByte == 0) {
      _paddedStrings = null;
//      _paddedStringToIdTrie = null;
    } else {
      _paddedStrings = new String[length];
//      _paddedStringToIdTrie = new PatriciaTrie<>();

      for (int i = 0; i < length; i++) {
        _paddedStrings[i] = getPaddedString(i, buffer);
//        _paddedStringToIdTrie.put(_paddedStrings[i], i);
      }
    }
  }

  @Override
  public int indexOf(Object rawValue) {

    byte[] index = _trieLookup.findValue(StringUtil.encodeUtf8((String) rawValue));
    return (index != null) ? convertByteArrayToInt(index) : -1;
//    PatriciaTrie<Integer> stringToIdTrie = (_paddingByte == 0) ? _unpaddedStringToIdTrie : _paddedStringToIdTrie;
//    Integer index = stringToIdTrie.get(rawValue);
//    return (index != null) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    return indexOf(rawValue);
//    if (_paddingByte == 0) {
//      Integer id = _unpaddedStringToIdTrie.get(rawValue);
//      return (id != null) ? id : Arrays.binarySearch(_unpaddedStrings, rawValue);
//    } else {
//      String paddedValue = padString((String) rawValue);
//      return Arrays.binarySearch(_paddedStrings, paddedValue);
//    }
  }

  @Override
  public String get(int dictId) {
    return _unpaddedStrings[dictId];
  }

  @Override
  public String getStringValue(int dictId) {
    return _unpaddedStrings[dictId];
  }


  private class IndexReader extends KeyValueSource<byte[]> {
    private PinotDataBuffer _dataBuffer;
    private int _length;
    private int _numBytesPerValue;
    private byte _paddingByte;
    private int _lineNumber;

    public IndexReader(PinotDataBuffer dataBuffer, int length, int numBytesPerValue,
        byte paddingByte) {
      _dataBuffer = dataBuffer;
      _length = length;
      _numBytesPerValue = numBytesPerValue;
      _paddingByte = paddingByte;
    }

    @Override
    public void readAll(ValueCallback<byte[]> handler) throws IOException {
      for (int i = 0; i < _length; i++) {
        ++_lineNumber;
        byte[] buffer = new byte[_numBytesPerValue];
        byte[] value = ByteBuffer.allocate(4).putInt(i).array();
        handler.handleEntry(getBytes(i, buffer), value);
      }
    }

    @Override
    public int getLineNumber() {
      return _lineNumber;
    }
  }

  private int convertByteArrayToInt(byte[] bytes) {
    ByteBuffer wrapped = ByteBuffer.wrap(bytes);
    return wrapped.getInt();
  }
}
