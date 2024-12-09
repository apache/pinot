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
import java.nio.ByteBuffer;
import java.nio.IntBuffer;


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

  public static long compute(IntBuffer buff) {
    buff.rewind();
    ByteBuffer bBuff = ByteBuffer.allocate(buff.array().length * 4);
    for (int i : buff.array()) {
      bBuff.putInt(i);
    }
    return compute(bBuff);
  }

  public static long compute(ByteBuffer buff) {
    return hash64(buff.array(), buff.array().length);
  }

  public static long hash64(final byte[] data, int length) {
    // Default seed is 0xe17a1465.
    return hash64(data, length, 0xe17a1465);
  }

  // Implement 64-bit Murmur2 hash.
  public static long hash64(final byte[] data, int length, int seed) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;

    long h = (seed & 0xffffffffL) ^ (length * m);

    int length8 = length / 8;

    for (int i = 0; i < length8; i++) {
      final int i8 = i * 8;
      long k =
          ((long) data[i8 + 0] & 0xff) + (((long) data[i8 + 1] & 0xff) << 8) + (((long) data[i8 + 2] & 0xff) << 16) + (
              ((long) data[i8 + 3] & 0xff) << 24) + (((long) data[i8 + 4] & 0xff) << 32) + (((long) data[i8 + 5] & 0xff)
              << 40) + (((long) data[i8 + 6] & 0xff) << 48) + (((long) data[i8 + 7] & 0xff) << 56);

      k *= m;
      k ^= k >>> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    // CHECKSTYLE:OFF: checkstyle:coding
    switch (length % 8) {
      case 7:
        h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;
      case 6:
        h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;
      case 5:
        h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;
      case 4:
        h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;
      case 3:
        h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;
      case 2:
        h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;
      case 1:
        h ^= data[length & ~7] & 0xff;
        h *= m;
    }
    // CHECKSTYLE:ON: checkstyle:coding

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;
    return h;
  }

  /**
   * Generates 32 bit murmur2 hash from byte array
   * @param data byte array to hash
   * @return 32 bit hash of the given array
   */
  @SuppressWarnings("checkstyle")
  public static int murmur2(final byte[] data) {
    int length = data.length;
    int seed = 0x9747b28c;
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    final int m = 0x5bd1e995;
    final int r = 24;

    // Initialize the hash to a random value
    int h = seed ^ length;
    int length4 = length / 4;

    for (int i = 0; i < length4; i++) {
      final int i4 = i * 4;
      int k =
          (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff)
              << 24);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // CHECKSTYLE:OFF: checkstyle:coding
    // Handle the last few bytes of the input array
    switch (length % 4) {
      case 3:
        h ^= (data[(length & ~3) + 2] & 0xff) << 16;
      case 2:
        h ^= (data[(length & ~3) + 1] & 0xff) << 8;
      case 1:
        h ^= data[length & ~3] & 0xff;
        h *= m;
    }
    // CHECKSTYLE:ON: checkstyle:coding

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }
}
