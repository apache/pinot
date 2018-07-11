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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;


/**
 * The class <code>BaseOnHeapMutableDictionary</code> is the implementation of the mutable dictionary required by
 * REALTIME consuming segments.
 * <p>The implementation needs to be thread safe for single writer multiple readers scenario.
 * <p>We can assume the readers always first get the dictionary id for a value, then use the dictionary id to fetch the
 * value later, but not reversely. So whenever we return a valid dictionary id for a value, we need to ensure the value
 * can be fetched by the dictionary id returned.
 */
public abstract class BaseOnHeapMutableDictionary extends MutableDictionary {
  private static final int SHIFT_OFFSET = 13;  // INITIAL_DICTIONARY_SIZE = 8192
  private static final int INITIAL_DICTIONARY_SIZE = 1 << SHIFT_OFFSET;
  private static final int MASK = 0xFFFFFFFF >>> (Integer.SIZE - SHIFT_OFFSET);

  private final Map<Object, Integer> _valueToDictId = new ConcurrentHashMap<>(INITIAL_DICTIONARY_SIZE);
  private final Object[][] _dictIdToValue = new Object[INITIAL_DICTIONARY_SIZE][];
  private int _entriesIndexed = 0;

  /**
   * For performance, we don't validate the dictId passed in. It should be returned by index() or indexOf().
   */
  @Nonnull
  @Override
  public Object get(int dictId) {
    return _dictIdToValue[dictId >>> SHIFT_OFFSET][dictId & MASK];
  }

  @Override
  public int length() {
    return _entriesIndexed;
  }

  @Override
  public int getAvgValueSize() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public boolean isEmpty() {
    return _entriesIndexed == 0;
  }

  public void close() throws IOException {
  }

  /**
   * Index a single value.
   * <p>This method will only be called by a single writer thread.
   *
   * @param value single value already converted to correct type.
   */
  protected void indexValue(@Nonnull Object value) {
    if (!_valueToDictId.containsKey(value)) {
      int arrayIndex = _entriesIndexed >>> SHIFT_OFFSET;
      int arrayOffset = _entriesIndexed & MASK;

      // Create a new array if necessary
      if (arrayOffset == 0) {
        _dictIdToValue[arrayIndex] = new Object[INITIAL_DICTIONARY_SIZE];
      }

      // First update dictId to value map then value to dictId map
      // Ensure we can always fetch value by dictId returned by index() or indexOf()
      _dictIdToValue[arrayIndex][arrayOffset] = value;
      _valueToDictId.put(value, _entriesIndexed);
      _entriesIndexed++;
    }
  }

  /**
   * Get the dictId of a single value.
   * <p>This method will only be called by a single writer thread.
   *
   * @param value single value already converted to correct type.
   * @return dictId of the value.
   */
  protected int getDictId(@Nonnull Object value) {
    Integer dictId = _valueToDictId.get(value);
    if (dictId == null) {
      return NULL_VALUE_INDEX;
    } else {
      return dictId;
    }
  }
}
