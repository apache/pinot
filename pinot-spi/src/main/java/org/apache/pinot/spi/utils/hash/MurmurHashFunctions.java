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
package org.apache.pinot.spi.utils.hash;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;


public class MurmurHashFunctions {
  public static final byte INVALID_CHAR = (byte) '?';

  private MurmurHashFunctions() {
  }

  /**
   * NOTE: This code has been copied over from org.apache.kafka.common.utils.Utils::murmurHash2
   *
   * Generates 32 bit murmurHash2 hash from byte array
   * @param data byte array to hash
   * @return 32 bit hash of the given array
   */
  public static int murmurHash2(final byte[] data) {
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

    // Handle the last few bytes of the input array
    // CHECKSTYLE:OFF
    switch (length % 4) {
      case 3:
        h ^= (data[(length & ~3) + 2] & 0xff) << 16;
      case 2:
        h ^= (data[(length & ~3) + 1] & 0xff) << 8;
      case 1:
        h ^= data[length & ~3] & 0xff;
        h *= m;
    }
    // CHECKSTYLE:ON

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }

  /**
   * Implement 64-bit Murmur2 hash.
   * @param data byte array to hash
   * @return 64-bit hash
   */
  public static long murmurHash2Bit64(final byte[] data) {
    return murmurHash2Bit64(data, data.length, 0xe17a1465);
  }

  /**
   * Implement 64-bit Murmur2 hash.
   * @param data byte array to hash
   * @param length byte array length
   * @param seed hash seed
   * @return 64-bit hash
   */
  public static long murmurHash2Bit64(final byte[] data, int length, int seed) {
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

  public static int murmurHash3X86Bit32(byte[] data, int seed) {
    return Hashing.murmur3_32_fixed(seed).hashBytes(data).asInt();
  }

  /**
   * Taken from <a href=
   * "https://github.com/infinispan/infinispan/blob/main/commons/all/src/main/java/org/infinispan/commons/hash
   * /MurmurHash3.java"
   * >Infinispan code base</a>.
   *
   * MurmurHash3 implementation in Java, based on Austin Appleby's <a href=
   * "https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp"
   * >original in C</a>s
   *
   * This is an implementation of MurmurHash3 to generate 32 bit hash for x64 architecture (not part of the original
   * Murmur3 implementations) used by Infinispan and Debezium, Removed the parts that we don't need and formatted
   * the code to Apache Pinot's Checkstyle.
   *
   * @author Patrick McFarland
   * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
   * @see <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash entry on Wikipedia</a>
   */
  public static int murmurHash3X64Bit32(String s, int seed) {
    State state = new State();

    state._h1 = 0x9368e53c2f6af274L ^ seed;
    state._h2 = 0x586dcd208f7cd3fdL ^ seed;

    state._c1 = 0x87c37b91114253d5L;
    state._c2 = 0x4cf5ad432745937fL;

    int byteLen = 0;
    int stringLen = s.length();

    // CHECKSTYLE:OFF: checkstyle:coding
    for (int i = 0; i < stringLen; i++) {
      char c1 = s.charAt(i);
      int cp;
      if (!Character.isSurrogate(c1)) {
        cp = c1;
      } else if (Character.isHighSurrogate(c1)) {
        if (i + 1 < stringLen) {
          char c2 = s.charAt(i + 1);
          if (Character.isLowSurrogate(c2)) {
            i++;
            cp = Character.toCodePoint(c1, c2);
          } else {
            cp = INVALID_CHAR;
          }
        } else {
          cp = INVALID_CHAR;
        }
      } else {
        cp = INVALID_CHAR;
      }

      if (cp <= 0x7f) {
        addByte(state, (byte) cp, byteLen++);
      } else if (cp <= 0x07ff) {
        byte b1 = (byte) (0xc0 | (0x1f & (cp >> 6)));
        byte b2 = (byte) (0x80 | (0x3f & cp));
        addByte(state, b1, byteLen++);
        addByte(state, b2, byteLen++);
      } else if (cp <= 0xffff) {
        byte b1 = (byte) (0xe0 | (0x0f & (cp >> 12)));
        byte b2 = (byte) (0x80 | (0x3f & (cp >> 6)));
        byte b3 = (byte) (0x80 | (0x3f & cp));
        addByte(state, b1, byteLen++);
        addByte(state, b2, byteLen++);
        addByte(state, b3, byteLen++);
      } else {
        byte b1 = (byte) (0xf0 | (0x07 & (cp >> 18)));
        byte b2 = (byte) (0x80 | (0x3f & (cp >> 12)));
        byte b3 = (byte) (0x80 | (0x3f & (cp >> 6)));
        byte b4 = (byte) (0x80 | (0x3f & cp));
        addByte(state, b1, byteLen++);
        addByte(state, b2, byteLen++);
        addByte(state, b3, byteLen++);
        addByte(state, b4, byteLen++);
      }
    }

    long savedK1 = state._k1;
    long savedK2 = state._k2;
    state._k1 = 0;
    state._k2 = 0;
    switch (byteLen & 15) {
      case 15:
        state._k2 ^= (long) ((byte) (savedK2 >> 48)) << 48;
      case 14:
        state._k2 ^= (long) ((byte) (savedK2 >> 40)) << 40;
      case 13:
        state._k2 ^= (long) ((byte) (savedK2 >> 32)) << 32;
      case 12:
        state._k2 ^= (long) ((byte) (savedK2 >> 24)) << 24;
      case 11:
        state._k2 ^= (long) ((byte) (savedK2 >> 16)) << 16;
      case 10:
        state._k2 ^= (long) ((byte) (savedK2 >> 8)) << 8;
      case 9:
        state._k2 ^= ((byte) savedK2);
      case 8:
        state._k1 ^= (long) ((byte) (savedK1 >> 56)) << 56;
      case 7:
        state._k1 ^= (long) ((byte) (savedK1 >> 48)) << 48;
      case 6:
        state._k1 ^= (long) ((byte) (savedK1 >> 40)) << 40;
      case 5:
        state._k1 ^= (long) ((byte) (savedK1 >> 32)) << 32;
      case 4:
        state._k1 ^= (long) ((byte) (savedK1 >> 24)) << 24;
      case 3:
        state._k1 ^= (long) ((byte) (savedK1 >> 16)) << 16;
      case 2:
        state._k1 ^= (long) ((byte) (savedK1 >> 8)) << 8;
      case 1:
        state._k1 ^= ((byte) savedK1);
        bmix(state);
    }
    // CHECKSTYLE:ON: checkstyle:coding
    state._h2 ^= byteLen;

    state._h1 += state._h2;
    state._h2 += state._h1;

    state._h1 = fmix(state._h1);
    state._h2 = fmix(state._h2);

    state._h1 += state._h2;
    state._h2 += state._h1;

    return (int) (state._h1 >> 32);
  }

  /**
   * Hash a value using the x64 128 bit variant of MurmurHash3.
   */
  public static byte[] murmurHash3X64Bit128(byte[] key, int length, int seed) {
    State state = murmurHash3X64(key, length, seed);
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(state._h1);
    buffer.putLong(state._h2);
    return buffer.array();
  }

  public static byte[] murmurHash3X64Bit128(byte[] key, int seed) {
    return murmurHash3X64Bit128(key, key.length, seed);
  }

  /**
   * Hash a value using the x64 128 bit variant of MurmurHash3.
   */
  public static long[] murmurHash3X64Bit128AsLongs(byte[] key, int length, int seed) {
    State state = murmurHash3X64(key, length, seed);
    return new long[]{state._h1, state._h2};
  }

  public static long[] murmurHash3X64Bit128AsLongs(byte[] key, int seed) {
    return murmurHash3X64Bit128AsLongs(key, key.length, seed);
  }

  /**
   * Hash a value using the x64 64 bit variant of murmurHash3.
   */
  public static long murmurHash3X64Bit64(byte[] key, int length, int seed) {
    return murmurHash3X64(key, length, seed)._h1;
  }

  public static long murmurHash3X64Bit64(byte[] key, int seed) {
    return murmurHash3X64Bit64(key, key.length, seed);
  }

  /**
   * Hash a value using the x64 64 bit variant of murmurHash3.
   */
  public static int murmurHash3X64Bit32(byte[] key, int length, int seed) {
    return (int) (murmurHash3X64(key, length, seed)._h1 >>> 32);
  }

  public static int murmurHash3X64Bit32(byte[] key, int seed) {
    return murmurHash3X64Bit32(key, key.length, seed);
  }

  /**
   * Taken and modified from
   * <a href="https://github.com/infinispan/infinispan/blob/main/commons/all/src/main/java/org/infinispan/commons/hash/
   * MurmurHash3.java">Infinispan code base</a>.
   */
  private static State murmurHash3X64(byte[] key, int length, int seed) {
    State state = new State();

    state._h1 = 0x9368e53c2f6af274L ^ seed;
    state._h2 = 0x586dcd208f7cd3fdL ^ seed;

    state._c1 = 0x87c37b91114253d5L;
    state._c2 = 0x4cf5ad432745937fL;

    int end = length - 15;
    for (int i = 0; i < end; i += 16) {
      state._k1 = getblock(key, i);
      state._k2 = getblock(key, i + 8);

      bmix(state);
    }

    state._k1 = 0;
    state._k2 = 0;

    int tail = length & 0xFFFFFFF0;
    // CHECKSTYLE:OFF: checkstyle:coding
    switch (length & 15) {
      case 15:
        state._k2 ^= (long) key[tail + 14] << 48;
      case 14:
        state._k2 ^= (long) key[tail + 13] << 40;
      case 13:
        state._k2 ^= (long) key[tail + 12] << 32;
      case 12:
        state._k2 ^= (long) key[tail + 11] << 24;
      case 11:
        state._k2 ^= (long) key[tail + 10] << 16;
      case 10:
        state._k2 ^= (long) key[tail + 9] << 8;
      case 9:
        state._k2 ^= key[tail + 8];
      case 8:
        state._k1 ^= (long) key[tail + 7] << 56;
      case 7:
        state._k1 ^= (long) key[tail + 6] << 48;
      case 6:
        state._k1 ^= (long) key[tail + 5] << 40;
      case 5:
        state._k1 ^= (long) key[tail + 4] << 32;
      case 4:
        state._k1 ^= (long) key[tail + 3] << 24;
      case 3:
        state._k1 ^= (long) key[tail + 2] << 16;
      case 2:
        state._k1 ^= (long) key[tail + 1] << 8;
      case 1:
        state._k1 ^= key[tail];
        bmix(state);
    }
    // CHECKSTYLE:ON: checkstyle:coding

    state._h2 ^= length;

    state._h1 += state._h2;
    state._h2 += state._h1;

    state._h1 = fmix(state._h1);
    state._h2 = fmix(state._h2);

    state._h1 += state._h2;
    state._h2 += state._h1;

    return state;
  }

  private static void addByte(State state, byte b, int len) {
    int shift = (len & 0x7) * 8;
    long bb = (b & 0xffL) << shift;
    if ((len & 0x8) == 0) {
      state._k1 |= bb;
    } else {
      state._k2 |= bb;
      if ((len & 0xf) == 0xf) {
        bmix(state);
        state._k1 = 0;
        state._k2 = 0;
      }
    }
  }

  private static long getblock(byte[] key, int i) {
    return (key[i] & 0x00000000000000FFL) | ((key[i + 1] & 0x00000000000000FFL) << 8) | (
        (key[i + 2] & 0x00000000000000FFL) << 16) | ((key[i + 3] & 0x00000000000000FFL) << 24) | (
        (key[i + 4] & 0x00000000000000FFL) << 32) | ((key[i + 5] & 0x00000000000000FFL) << 40) | (
        (key[i + 6] & 0x00000000000000FFL) << 48) | ((key[i + 7] & 0x00000000000000FFL) << 56);
  }

  private static void bmix(State state) {
    state._k1 *= state._c1;
    state._k1 = (state._k1 << 23) | (state._k1 >>> 64 - 23);
    state._k1 *= state._c2;
    state._h1 ^= state._k1;
    state._h1 += state._h2;

    state._h2 = (state._h2 << 41) | (state._h2 >>> 64 - 41);

    state._k2 *= state._c2;
    state._k2 = (state._k2 << 23) | (state._k2 >>> 64 - 23);
    state._k2 *= state._c1;
    state._h2 ^= state._k2;
    state._h2 += state._h1;

    state._h1 = state._h1 * 3 + 0x52dce729;
    state._h2 = state._h2 * 3 + 0x38495ab5;

    state._c1 = state._c1 * 5 + 0x7b7d159c;
    state._c2 = state._c2 * 5 + 0x6bce6396;
  }

  private static long fmix(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }

  private static class State {
    long _h1;
    long _h2;

    long _k1;
    long _k2;

    long _c1;
    long _c2;
  }
}
