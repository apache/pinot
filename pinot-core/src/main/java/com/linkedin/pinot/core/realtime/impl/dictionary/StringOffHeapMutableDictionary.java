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

package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MutableOffHeapByteArrayStore;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import javax.annotation.Nonnull;


public class StringOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private final MutableOffHeapByteArrayStore _byteStore;
  private String _min = null;
  private String _max = null;

  public StringOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowHashSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext, int avgStringLen) {
    super(estimatedCardinality, maxOverflowHashSize, memoryManager, allocationContext);
    _byteStore = new MutableOffHeapByteArrayStore(memoryManager, allocationContext, estimatedCardinality, avgStringLen);
  }

  @Override
  public void doClose() throws IOException {
    _byteStore.close();
  }

  @Override
  protected void setRawValueAt(int dictId, Object rawValue, byte[] serializedValue) {
    _byteStore.add(serializedValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return new String(_byteStore.get(dictionaryId));
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    if (rawValue instanceof String) {
      // Single value
      byte[] serializedValue =  ((String) rawValue).getBytes(UTF_8);
      indexValue(rawValue, serializedValue);
      updateMinMax((String) rawValue);
    } else {
      // Multi value
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        byte[] serializedValue =  ((String) value).getBytes(UTF_8);
        indexValue(value, serializedValue);
        updateMinMax((String) value);
      }
    }
  }

  private String getInternal(int dictId) {
    return new String(_byteStore.get(dictId));
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    String valueToCompare = getInternal(dictIdToCompare);

    if (includeLower) {
      if (valueToCompare.compareTo(lower) < 0) {
        return false;
      }
    } else {
      if (valueToCompare.compareTo(lower) <= 0) {
        return false;
      }
    }

    if (includeUpper) {
      if (valueToCompare.compareTo(upper) > 0) {
        return false;
      }
    } else {
      if (valueToCompare.compareTo(upper) >= 0) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int indexOf(Object rawValue) {
    byte[] serializedValue = ((String) rawValue).getBytes(UTF_8);
    return getDictId(rawValue, serializedValue);
  }

  @Nonnull
  @Override
  public Object getMinVal() {
    return _min;
  }

  @Nonnull
  @Override
  public Object getMaxVal() {
    return _max;
  }

  @Nonnull
  @Override
  public Object getSortedValues() {
    int numValues = length();
    String[] sortedValues = new String[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = new String(getInternal(i));
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return _byteStore.equalsValueAt(serializedValue, dictId);
  }

  private void updateMinMax(String value) {
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
  public long getTotalOffHeapMemUsed() {
    return super.getTotalOffHeapMemUsed() + _byteStore.getTotalOffHeapMemUsed();
  }

  @Override
  public int getAvgValueSize() {
    return (int) _byteStore.getAvgValueSize();
  }
}
