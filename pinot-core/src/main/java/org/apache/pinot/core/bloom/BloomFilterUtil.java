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
package org.apache.pinot.core.bloom;

/**
 * Util class for bloom filter
 */
public class BloomFilterUtil {

  private BloomFilterUtil() {
  }

  public static long computeNumBits(long cardinality, double maxFalsePosProbability) {
    return (long) (Math
        .ceil((cardinality * Math.log(maxFalsePosProbability)) / Math.log(1.0 / Math.pow(2.0, Math.log(2.0)))));
  }

  public static int computeNumberOfHashFunctions(long cardinality, long numBits) {
    return (int) Math.max(1.0, Math.round(((double) numBits / cardinality) * Math.log(2.0)));
  }

  public static double computeMaxFalsePosProbability(long cardinality, int numHashFunction, long numBits) {
    return Math.pow(1.0 - Math.exp(-1.0 * numHashFunction / ((double) numBits / cardinality)), numHashFunction);
  }

  public static double computeMaxFalsePositiveProbabilityForNumBits(long cardinality, long maxNumBits,
      double defaultMaxFalsePosProbability) {
    // Get the number of bits required for achieving default false positive probability
    long numBitsRequired = BloomFilterUtil.computeNumBits(cardinality, defaultMaxFalsePosProbability);

    // If the size of bloom filter is smaller than 1MB, use default max false positive probability
    if (numBitsRequired <= maxNumBits) {
      return defaultMaxFalsePosProbability;
    }

    // If the size of bloom filter is larger than 1MB, compute the maximum false positive probability within
    // storage limit
    int numHashFunction = BloomFilterUtil.computeNumberOfHashFunctions(cardinality, maxNumBits);
    return BloomFilterUtil.computeMaxFalsePosProbability(cardinality, numHashFunction, maxNumBits);
  }
}
