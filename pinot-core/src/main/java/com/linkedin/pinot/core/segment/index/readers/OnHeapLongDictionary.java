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
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import java.util.Arrays;


/**
 * Implementation of long dictionary that cache all values on-heap.
 * <p>This is useful for Long columns that:
 * <ul>
 *   <li>Have low cardinality long dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 * <p>This helps avoid creation of Long from byte[].
 */
public class OnHeapLongDictionary extends OnHeapDictionary {
  private final Long2IntOpenHashMap _valToDictId;
  private final long[] _dictIdToVal;

  /**
   * Constructor for the class.
   * Populates the value <-> mappings.
   *
   * @param dataBuffer Pinot data buffer
   * @param length Length of the dictionary
   */
  public OnHeapLongDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Long.BYTES, (byte) 0);

    _valToDictId = new Long2IntOpenHashMap(length);
    _valToDictId.defaultReturnValue(-1);
    _dictIdToVal = new long[length];

    for (int dictId = 0; dictId < length; dictId++) {
      long value = getLong(dictId);
      _dictIdToVal[dictId] = value;
      _valToDictId.put(value, dictId);
    }
  }

  @Override
  public int indexOf(Object rawValue) {
    long value = getValue(rawValue);
    return _valToDictId.get(value);
  }

  private long getValue(Object rawValue) {
    long value;
    if (rawValue instanceof String) {
      value = Long.parseLong((String) rawValue);
    } else if (rawValue instanceof Long) {
      value = (Long) rawValue;
    } else {
      throw new IllegalArgumentException(
          "Illegal data type for argument, actual: " + rawValue.getClass().getName() + " expected: "
              + Long.class.getName());
    }
    return value;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    int index = indexOf(rawValue);
    return (index != -1) ? index : Arrays.binarySearch(_dictIdToVal, getValue(rawValue));
  }

  @Override
  public Long get(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public long getLongValue(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public String getStringValue(int dictId) {
    return Long.toString(_dictIdToVal[dictId]);
  }
}
