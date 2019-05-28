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
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.primitive.ByteArray;


/**
 * OnHeap mutable dictionary of Bytes type.
 */
public class BytesOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {

  private ByteArray _min = null;
  private ByteArray _max = null;

  @Override
  public int indexOf(Object rawValue) {
    byte[] bytes = BytesUtils.toBytes(rawValue);
    return getDictId(new ByteArray(bytes));
  }

  @Override
  public byte[] get(int dictId) {
    return getBytesValue(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return BytesUtils.toHexString(getBytesValue(dictId));
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return ((ByteArray) super.get(dictId)).getBytes();
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    byte[] bytes = BytesUtils.toBytes(rawValue);
    ByteArray byteArray = new ByteArray(bytes);
    indexValue(byteArray);
    updateMinMax(byteArray);
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    return valueInRange(new ByteArray(BytesUtils.toBytes(lower)), new ByteArray(BytesUtils.toBytes(upper)),
        includeLower, includeUpper, (ByteArray) super.get(dictIdToCompare));
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

  @Override
  public int compare(int dictId1, int dictId2) {
    return ByteArray.compare(getBytesValue(dictId1), getBytesValue(dictId2));
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
}
