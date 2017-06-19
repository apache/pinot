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

import java.io.IOException;
import java.util.Arrays;
import com.linkedin.pinot.core.io.writer.impl.OffHeapStringStore;
import javax.annotation.Nonnull;


public class StringOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {

  private final OffHeapStringStore _stringStore;
  private String _min = null;
  private String _max = null;

  public StringOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowHashSize) {
    super(estimatedCardinality, maxOverflowHashSize);
    _stringStore = new OffHeapStringStore();
  }

  @Override
  public void doClose() throws IOException {
    _stringStore.close();
  }

  @Override
  protected void setRawValueAt(int dictId, Object rawValue) {
    _stringStore.add(rawValue.toString());
  }

  @Override
  public Object get(int dictionaryId) {
    return _stringStore.get(dictionaryId);
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    if (rawValue instanceof String) {
      // Single value
      indexValue(rawValue);
      updateMinMax((String) rawValue);
    } else {
      // Multi value
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        indexValue(value);
        updateMinMax((String) value);
      }
    }
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    String valueToCompare = (String) get(dictIdToCompare);

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
    return getDictId(rawValue);
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
      sortedValues[i] = (String) get(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  protected boolean equalsValueAt(int dictId, Object value) {
    if (_stringStore.equalsStringAt(value.toString(), dictId)) {
      return true;
    }
    return false;
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
}
