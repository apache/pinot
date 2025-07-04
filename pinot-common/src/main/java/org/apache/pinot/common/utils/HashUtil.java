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
package org.apache.pinot.common.utils;

import com.google.common.primitives.Ints;


public class HashUtil {
  private HashUtil() {
  }

  /** Tests show that even for smaller set sizes, setting the hash size to this min value
   * improves performance at an insignificant increase of memory footprint.
   */
  public static final int MIN_FASTUTIL_HASHSET_SIZE = 25;

  /**
   * Returns the min size for the fast-util hash-set given the expected size of
   * values stored in the hash-set.
   * @param expected the expected/actual number of values to be stored
   * @return the optimal min value
   */
  public static int getMinHashSetSize(int expected) {
    return Math.max(MIN_FASTUTIL_HASHSET_SIZE, expected);
  }

  /**
   * Returns a capacity that is sufficient to keep the map from being resized as long as it grows no larger than
   * expectedSize and the load factor is >= its default (0.75).
   * NOTE: Borrowed from Guava's Maps library {@code int capacity(int expectedSize)}.
   */
  public static int getHashMapCapacity(int expectedSize) {
    if (expectedSize < 3) {
      return expectedSize + 1;
    }
    if (expectedSize < Ints.MAX_POWER_OF_TWO) {
      return (int) Math.ceil(expectedSize / 0.75);
    }
    return Integer.MAX_VALUE;
  }
}
