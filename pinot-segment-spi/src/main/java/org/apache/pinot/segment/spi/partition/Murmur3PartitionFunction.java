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
    Preconditions.checkArgument(functionConfig == null || functionConfig.get(MURMUR3_VARIANT) == null || (
            functionConfig.get(MURMUR3_VARIANT).equals("x86_32") || functionConfig.get(MURMUR3_VARIANT).equals(
                "x64_32")),
        "Murmur3 variant must be either x86_32 or x64_32");
    _numPartitions = numPartitions;

    // default value of the hash seed is 0.
    _hashSeed = (functionConfig == null || functionConfig.get(SEED_KEY) == null) ? 0
        : Integer.parseInt(functionConfig.get(SEED_KEY));

    // default value of the murmur3 variant is x86_32.
    _variant = (functionConfig == null || functionConfig.get(MURMUR3_VARIANT) == null) ? "x86_32"
        : functionConfig.get(MURMUR3_VARIANT);
  }

  @Override
  public int getPartition(Object value) {
    if (_variant.equals("x86_32")) {
      return (murmurHash332bitsX86(value.toString().getBytes(UTF_8), _hashSeed) & Integer.MAX_VALUE) % _numPartitions;
    }
    return (murmurHash332bitsX64(value.toString().getBytes(UTF_8), _hashSeed) & Integer.MAX_VALUE) % _numPartitions;
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
  int murmurHash332bitsX86(byte[] data, int hashSeed) {
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
  private long murmurHash364bitsX64(final byte[] key, final int seed) {
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
  @VisibleForTesting
  int murmurHash332bitsX64(final byte[] key, final int seed) {
    return (int) (murmurHash364bitsX64(key, seed) >>> 32);
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
