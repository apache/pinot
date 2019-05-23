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

import java.nio.ByteBuffer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.core.io.util.VarLengthBytesValueReaderWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Extension of {@link ImmutableDictionaryReader} that implements immutable dictionary for byte[] type.
 */
public class BytesDictionary extends ImmutableDictionaryReader {

  public BytesDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    super(new VarLengthBytesValueReaderWriter(dataBuffer), length, numBytesPerValue, (byte) 0);
  }

  @Override
  public int indexOf(Object rawValue) {
    int index = insertionIndexOf(rawValue);
    return (index >= 0) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    byte[] value;
    if (rawValue instanceof byte[]) {
      value = (byte[]) rawValue;
    } else if (rawValue instanceof String) {
      try {
        value = Hex.decodeHex(((String) rawValue).toCharArray());
      } catch (DecoderException e) {
        throw new ClassCastException(String.format("Faield to convert Hex String value: %s to byte[] type.", rawValue));
      }
    } else if (rawValue instanceof ByteBuffer) {
      value = ((ByteBuffer) rawValue).array();
    } else {
      throw new UnsupportedOperationException(String.format("Faield to convert Object: %s to byte[] type.", rawValue));
    }
    return binarySearch(value);
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

  @Override
  public String getStringValue(int dictId) {
    return Hex.encodeHexString(get(dictId));
  }
}
