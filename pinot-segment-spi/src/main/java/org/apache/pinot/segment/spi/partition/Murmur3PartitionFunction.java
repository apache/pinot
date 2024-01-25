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
import org.apache.commons.lang.StringUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Implementation of {@link PartitionFunction} which partitions based on 32 bit murmur3 hash
 */
public class Murmur3PartitionFunction implements PartitionFunction {
  public static final byte INVALID_CHAR = (byte) '?';
  private static final String NAME = "Murmur3";
  private static final String SEED_KEY = "seed";
  private static final String VARIANT_KEY = "variant";
  private final int _numPartitions;
  private final int _seed;
  private final boolean _useX64;

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions.
   * @param functionConfig to extract configurations for the partition function.
   */
  public Murmur3PartitionFunction(int numPartitions, Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    _numPartitions = numPartitions;

    int seed = 0;
    boolean useX64 = false;
    if (functionConfig != null) {
      String seedString = functionConfig.get(SEED_KEY);
      if (StringUtils.isNotEmpty(seedString)) {
        seed = Integer.parseInt(seedString);
      }
      String variantString = functionConfig.get(VARIANT_KEY);
      if (StringUtils.isNotEmpty(variantString)) {
        if (variantString.equals("x64_32")) {
          useX64 = true;
        } else {
          Preconditions.checkArgument(variantString.equals("x86_32"),
              "Murmur3 variant must be either x86_32 or x64_32");
        }
      }
    }
    _seed = seed;
    _useX64 = useX64;
  }

  @Override
  public int getPartition(String value) {
    int hash = _useX64 ? murmur3Hash32BitsX64(value, _seed) : murmur3Hash32BitsX86(value.getBytes(UTF_8), _seed);
    return (hash & Integer.MAX_VALUE) % _numPartitions;
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
  static int murmur3Hash32BitsX86(byte[] data, int seed) {
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

  @VisibleForTesting
  static int murmur3Hash32BitsX64(String s, int seed) {
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
    // CHECKSTYLE:ON

    state._h2 ^= byteLen;

    state._h1 += state._h2;
    state._h2 += state._h1;

    state._h1 = fmix(state._h1);
    state._h2 = fmix(state._h2);

    state._h1 += state._h2;
    state._h2 += state._h1;

    return (int) (state._h1 >> 32);
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

  private static class State {
    long _h1;
    long _h2;

    long _k1;
    long _k2;

    long _c1;
    long _c2;
  }
}
