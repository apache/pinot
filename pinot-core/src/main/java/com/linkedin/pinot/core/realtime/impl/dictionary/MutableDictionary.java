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

import java.io.Closeable;
import java.io.IOException;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import javax.annotation.Nonnull;


public abstract class MutableDictionary implements Dictionary, Closeable {

 @Override
  public String getStringValue(int dictId) {
    return get(dictId).toString();
  }

  @Override
  public void readIntValues(int[] dictIds, int startPos, int limit, int[] outValues, int outStartPos) {
    int endPos = startPos + limit;
    for (int i = startPos; i < endPos; i++) {
      outValues[outStartPos++] = getIntValue(dictIds[i]);
    }
  }

  @Override
  public void readLongValues(int[] dictIds, int startPos, int limit, long[] outValues, int outStartPos) {
    int endPos = startPos + limit;
    for (int i = startPos; i < endPos; i++) {
      outValues[outStartPos++] = getLongValue(dictIds[i]);
    }
  }

  @Override
  public void readFloatValues(int[] dictIds, int startPos, int limit, float[] outValues, int outStartPos) {
    int endPos = startPos + limit;
    for (int i = startPos; i < endPos; i++) {
      outValues[outStartPos++] = getFloatValue(dictIds[i]);
    }
  }

  @Override
  public void readDoubleValues(int[] dictIds, int startPos, int limit, double[] outValues, int outStartPos) {
    int endPos = startPos + limit;
    for (int i = startPos; i < endPos; i++) {
      outValues[outStartPos++] = getDoubleValue(dictIds[i]);
    }
  }

  @Override
  public void readStringValues(int[] dictIds, int startPos, int limit, String[] outValues, int outStartPos) {
    int endPos = startPos + limit;
    for (int i = startPos; i < endPos; i++) {
      outValues[outStartPos++] = getStringValue(dictIds[i]);
    }
  }

  public abstract void index(@Nonnull Object rawValue);

  public abstract boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare,
      boolean includeLower, boolean includeUpper);

  @Nonnull
  public abstract Object getMinVal();

  @Nonnull
  public abstract Object getMaxVal();

  @Nonnull
  public abstract Object getSortedValues();

  public abstract boolean isEmpty();

  public abstract void close() throws IOException;
}
