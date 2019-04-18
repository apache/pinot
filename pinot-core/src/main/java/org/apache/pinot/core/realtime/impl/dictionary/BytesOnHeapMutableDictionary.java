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
package org.apache.pinot.core.realtime.impl.dictionary;

import java.util.Arrays;
import javax.annotation.Nonnull;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.utils.primitive.ByteArray;


/**
 * OnHeap mutable dictionary of Bytes type.
 */
public class BytesOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {

  private ByteArray _min = null;
  private ByteArray _max = null;

  @Override
  public int indexOf(Object rawValue) {
    byte[] bytes = null;
    // Convert hex string to byte[].
    if (rawValue instanceof byte[]) {
      bytes = (byte[]) rawValue;
    } else if (rawValue instanceof String) {
      try {
        bytes = Hex.decodeHex(((String) rawValue).toCharArray());
      } catch (DecoderException e) {
        Utils.rethrowException(e);
      }
    } else {
      assert rawValue instanceof byte[];
    }
    return getDictId(new ByteArray(bytes));
  }

  @Override
  public byte[] get(int dictId) {
    return ((ByteArray) super.get(dictId)).getBytes();
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return get(dictId);
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    byte[] bytes = null;
    // Convert hex string to byte[].
    if (rawValue instanceof String) {
      try {
        bytes = Hex.decodeHex(((String) rawValue).toCharArray());
      } catch (DecoderException e) {
        Utils.rethrowException(e);
      }
    } else {
      assert rawValue instanceof byte[];
      bytes = (byte[]) rawValue;
    }
    ByteArray byteArray = new ByteArray(bytes);
    indexValue(byteArray);
    updateMinMax(byteArray);
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    throw new UnsupportedOperationException("In-range not supported for Bytes data type.");
  }

  @Nonnull
  @Override
  public ByteArray getMinVal() {
    return _min;
  }

  @Nonnull
  @Override
  public ByteArray getMaxVal() {
    return _max;
  }

  @Nonnull
  @Override
  public ByteArray[] getSortedValues() {
    int numValues = length();
    ByteArray[] sortedValues = new ByteArray[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = (ByteArray) super.get(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  private void updateMinMax(ByteArray value) {
    if (_min == null) {
      _min = value;
      _max = value;
    } else {
      if (value.compareTo(_min) < 0) {
        _min = value;
      }
      if (value.compareTo(_max) > 0) {
        _max = value;
      }
    }
  }

  @Override
  public String getStringValue(int dictId) {
    return Hex.encodeHexString(get(dictId));
  }
}
