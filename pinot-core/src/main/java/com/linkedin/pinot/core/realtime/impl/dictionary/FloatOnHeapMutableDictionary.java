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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import java.util.Arrays;
import javax.annotation.Nonnull;


public class FloatOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {
  private float _min = Float.MAX_VALUE;
  private float _max = Float.MIN_VALUE;

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return getDictId(Float.valueOf((String) rawValue));
    } else {
      return getDictId(rawValue);
    }
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    if (rawValue instanceof Float) {
      // Single value
      indexValue(rawValue);
      updateMinMax((Float) rawValue);
    } else {
      // Multi value
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        indexValue(value);
        updateMinMax((Float) value);
      }
    }
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    float lowerFloat = Float.parseFloat(lower);
    float upperFloat = Float.parseFloat(upper);
    float valueToCompare = (Float) get(dictIdToCompare);

    if (includeLower) {
      if (valueToCompare < lowerFloat) {
        return false;
      }
    } else {
      if (valueToCompare <= lowerFloat) {
        return false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperFloat) {
        return false;
      }
    } else {
      if (valueToCompare >= upperFloat) {
        return false;
      }
    }

    return true;
  }

  @Nonnull
  @Override
  public Float getMinVal() {
    return _min;
  }

  @Nonnull
  @Override
  public Float getMaxVal() {
    return _max;
  }

  @Nonnull
  @Override
  public float[] getSortedValues() {
    int numValues = length();
    float[] sortedValues = new float[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = (Float) get(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public int getIntValue(int dictId) {
    return ((Float) get(dictId)).intValue();
  }

  @Override
  public long getLongValue(int dictId) {
    return ((Float) get(dictId)).longValue();
  }

  @Override
  public float getFloatValue(int dictId) {
    return (Float) get(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return (Float) get(dictId);
  }

  private void updateMinMax(float value) {
    if (value < _min) {
      _min = value;
    }
    if (value > _max) {
      _max = value;
    }
  }
}
