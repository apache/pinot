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
package org.apache.pinot.segment.spi.partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Implementation of {@link PartitionFunction} which partitions based on 32 bit murmur3 hash
 */
public class Murmur3PartitionFunction implements PartitionFunction {
  public static final byte INVALID_CHAR = (byte) '?';
  private static final String NAME = "Murmur3";
  private static final String SEED_KEY = "seed";
  private static final String MURMUR3_VARIANT = "variant";
  private final int _numPartitions;
  private final int _hashSeed;
  private final String _variant;

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions.
   * @param functionConfig to extract configurations for the partition function.
   */
  public Murmur3PartitionFunction(int numPartitions, Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    Preconditions.checkArgument(
        functionConfig == null || functionConfig.get(MURMUR3_VARIANT) == null || functionConfig.get(MURMUR3_VARIANT)
            .isEmpty() || functionConfig.get(MURMUR3_VARIANT).equals("x86_32") || functionConfig.get(MURMUR3_VARIANT)
            .equals("x64_32"), "Murmur3 variant must be either x86_32 or x64_32");
    _numPartitions = numPartitions;

    // default value of the hash seed is 0.
    _hashSeed =
        (functionConfig == null || functionConfig.get(SEED_KEY) == null || functionConfig.get(SEED_KEY).isEmpty()) ? 0
            : Integer.parseInt(functionConfig.get(SEED_KEY));

    // default value of the murmur3 variant is x86_32.
    _variant =
        (functionConfig == null || functionConfig.get(MURMUR3_VARIANT) == null || functionConfig.get(MURMUR3_VARIANT)
            .isEmpty()) ? "x86_32" : functionConfig.get(MURMUR3_VARIANT);
  }

  @Override
  public int getPartition(Object value) {
    if (_variant.equals("x86_32")) {
      return (murmurHash332BitsX86(value.toString().getBytes(UTF_8), _hashSeed) & Integer.MAX_VALUE) % _numPartitions;
    }
    return (murmurHash332BitsX64(value, _hashSeed) & Integer.MAX_VALUE) % _numPartitions;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  // Keep it for backward-compatibility, use getName() instead
  @Override
  public String toString() {
    return NAME;
  }

  @VisibleForTesting
  int murmurHash332BitsX86(byte[] data, int hashSeed) {
    return Hashing.murmur3_32_fixed(hashSeed).hashBytes(data).asInt();
  }

  /**
   * Taken from <a href=
   * "https://github.com/infinispan/infinispan/blob/main/commons/all/src/main/java/org/infinispan/commons/hash
   * /MurmurHash3.java"
   * >Infinispan code base</a>.
   *
   * MurmurHash3 implementation in Java, based on Austin Appleby's <a href=
   * "https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp"
   * >original in C</a>
   *
   * This is an implementation of MurmurHash3 to generate 32 bit hash for x64 architecture (not part of the original
   * Murmur3 implementations) used by Infinispan and Debezium, Removed the parts that we don't need and formatted
   * the code to Apache Pinot's Checkstyle.
   *
   * @author Patrick McFarland
   * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
   * @see <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash entry on Wikipedia</a>
   */

  private long getblock(byte[] key, int i) {
    return ((key[i + 0] & 0x00000000000000FFL)) | ((key[i + 1] & 0x00000000000000FFL) << 8) | (
        (key[i + 2] & 0x00000000000000FFL) << 16) | ((key[i + 3] & 0x00000000000000FFL) << 24) | (
        (key[i + 4] & 0x00000000000000FFL) << 32) | ((key[i + 5] & 0x00000000000000FFL) << 40) | (
        (key[i + 6] & 0x00000000000000FFL) << 48) | ((key[i + 7] & 0x00000000000000FFL) << 56);
  }

  private void bmix(State state) {
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

  private long fmix(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }

  /**
   * Hash a value using the x64 64 bit variant of MurmurHash3
   *
   * @param key value to hash
   * @param seed random value
   * @return 64 bit hashed key
   */
  private long murmurHash364BitsX64(final byte[] key, final int seed) {
    State state = new State();

    state._h1 = 0x9368e53c2f6af274L ^ seed;
    state._h2 = 0x586dcd208f7cd3fdL ^ seed;

    state._c1 = 0x87c37b91114253d5L;
    state._c2 = 0x4cf5ad432745937fL;

    for (int i = 0; i < key.length / 16; i++) {
      state._k1 = getblock(key, i * 2 * 8);
      state._k2 = getblock(key, (i * 2 + 1) * 8);

      bmix(state);
    }

    state._k1 = 0;
    state._k2 = 0;

    int tail = (key.length >>> 4) << 4;

    // CHECKSTYLE:OFF
    switch (key.length & 15) {
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
        state._k1 ^= key[tail + 0];
        bmix(state);
    }

    // CHECKSTYLE:ON
    state._h2 ^= key.length;

    state._h1 += state._h2;
    state._h2 += state._h1;

    state._h1 = fmix(state._h1);
    state._h2 = fmix(state._h2);

    state._h1 += state._h2;
    state._h2 += state._h1;

    return state._h1;
  }

  /**
   * Hash a value using the x64 32 bit variant of MurmurHash3
   *
   * @param key value to hash
   * @param seed random value
   * @return 32 bit hashed key
   */
  private int murmurHash332BitsX64(final byte[] key, final int seed) {
    return (int) (murmurHash364BitsX64(key, seed) >>> 32);
  }

  private long murmurHash364BitsX64(final long[] key, final int seed) {
    // Exactly the same as MurmurHash3_x64_128, except it only returns state.h1
    State state = new State();

    state._h1 = 0x9368e53c2f6af274L ^ seed;
    state._h2 = 0x586dcd208f7cd3fdL ^ seed;

    state._c1 = 0x87c37b91114253d5L;
    state._c2 = 0x4cf5ad432745937fL;

    for (int i = 0; i < key.length / 2; i++) {
      state._k1 = key[i * 2];
      state._k2 = key[i * 2 + 1];

      bmix(state);
    }

    long tail = key[key.length - 1];

    if (key.length % 2 != 0) {
      state._k1 ^= tail;
      bmix(state);
    }

    state._h2 ^= key.length * 8;

    state._h1 += state._h2;
    state._h2 += state._h1;

    state._h1 = fmix(state._h1);
    state._h2 = fmix(state._h2);

    state._h1 += state._h2;
    state._h2 += state._h1;

    return state._h1;
  }

  /**
   * Hash a value using the x64 32 bit variant of MurmurHash3
   *
   * @param key value to hash
   * @param seed random value
   * @return 32 bit hashed key
   */
  private int murmurHash332BitsX64(final long[] key, final int seed) {
    return (int) (murmurHash364BitsX64(key, seed) >>> 32);
  }

  @VisibleForTesting
  int murmurHash332BitsX64(Object o, int seed) {
    if (o instanceof byte[]) {
      return murmurHash332BitsX64((byte[]) o, seed);
    } else if (o instanceof long[]) {
      return murmurHash332BitsX64((long[]) o, seed);
    } else if (o instanceof String) {
      return murmurHash332BitsX64((String) o, seed);
    } else {
      // Differing from the source implementation here. The default case in the source implementation is to apply the
      // hash on the hashcode of the object. The hashcode of an object is not guaranteed to be consistent across JVMs
      // (except for String values), so we cannot guarantee the same value as the data source. Instead, we will apply
      // the hash on the string representation of the object, which aligns with rest of our codebase.
      return murmurHash332BitsX64(o.toString().getBytes(UTF_8), seed);
    }
  }

  private int murmurHash332BitsX64(String s, int seed) {
    return (int) (murmurHash364BitsX64String(s, seed) >> 32);
  }

  private long murmurHash364BitsX64String(String s, long seed) {
    // Exactly the same as MurmurHash3_x64_64, except it works directly on a String's chars
    State state = new State();

    state._h1 = 0x9368e53c2f6af274L ^ seed;
    state._h2 = 0x586dcd208f7cd3fdL ^ seed;

    state._c1 = 0x87c37b91114253d5L;
    state._c2 = 0x4cf5ad432745937fL;

    int byteLen = 0;
    int stringLen = s.length();

    // CHECKSTYLE:OFF
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

    // CHECKSTYLE:ON
    long savedK1 = state._k1;
    long savedK2 = state._k2;
    state._k1 = 0;
    state._k2 = 0;

    // CHECKSTYLE:OFF
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
    // CHECKSTYLE:ON
    state._h2 ^= byteLen;

    state._h1 += state._h2;
    state._h2 += state._h1;

    state._h1 = fmix(state._h1);
    state._h2 = fmix(state._h2);

    state._h1 += state._h2;
    state._h2 += state._h1;

    return state._h1;
  }

  private void addByte(State state, byte b, int len) {
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

  static class State {
    long _h1;
    long _h2;

    long _k1;
    long _k2;

    long _c1;
    long _c2;
  }
}
