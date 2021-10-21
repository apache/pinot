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
package org.apache.pinot.segment.local.segment.index.readers.bloom;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import static java.nio.charset.StandardCharsets.UTF_8;


@SuppressWarnings("UnstableApiUsage")
public class GuavaBloomFilterReaderUtils {
  private GuavaBloomFilterReaderUtils() {
  }

  // DO NOT change the hash function. It has to be aligned with the bloom filter creator.
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

  /**
   * Returns the hash of the given value as a byte array.
   */
  public static byte[] hash(String value) {
    return HASH_FUNCTION.hashBytes(value.getBytes(UTF_8)).asBytes();
  }

  /* Cheat sheet:

     m: total bits
     n: expected insertions
     b: m/n, bits per insertion
     p: expected false positive probability
     k: number of hash functions

     1) Optimal k = b * ln2
     2) p = (1 - e ^ (-kn/m)) ^ k
     3) For optimal k: p = 2 ^ (-k) ~= 0.6185^b
     4) For optimal k: m = -nlnp / ((ln2) ^ 2)

     See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
   */

  /**
   * Calculates the fpp (false positive probability) based on the given bloom filter size and number of insertions.
   */
  public static double computeFPP(int sizeInBytes, int numInsertions) {
    double b = (double) sizeInBytes * Byte.SIZE / numInsertions;
    double k = b * Math.log(2);
    return Math.pow(2, -k);
  }
}
