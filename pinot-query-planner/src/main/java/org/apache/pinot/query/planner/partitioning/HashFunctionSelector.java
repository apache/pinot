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
package org.apache.pinot.query.planner.partitioning;

import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.utils.hash.CityHashFunctions;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;


/**
 * Utility class to compute hash values using different hash functions.
 * This class provides consistent hash computation for KeySelector implementations.
 */
public class HashFunctionSelector {
  private HashFunctionSelector() {
  }

  /**
   * Computes hash value using the specified hash function.
   *
   * @param value the value to hash
   * @param hashFunction the hash function to use
   * @return hash value as a positive integer
   */
  public static int computeHash(Object value, String hashFunction) {
    if (value == null) {
      return 0;
    }

    switch (hashFunction.toLowerCase()) {
      case "abshashcode":
        return value.hashCode() & Integer.MAX_VALUE;
      case "murmur2":
        return computeMurmur2Hash(value);
      case "murmur3":
        return computeMurmur3Hash(value);
      case "cityhash":
        return computeCityHash(value);
      default:
        // Default to absHashCode for unknown hash functions
        return Math.abs(value.hashCode());
    }
  }

  private static int computeMurmur2Hash(Object value) {
    byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
    return MurmurHashFunctions.murmurHash2(bytes) & Integer.MAX_VALUE;
  }

  private static int computeMurmur3Hash(Object value) {
    byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
    return MurmurHashFunctions.murmurHash3X64Bit32(bytes, 0) & Integer.MAX_VALUE;
  }

  private static int computeCityHash(Object value) {
    byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
    long hash = CityHashFunctions.cityHash64(bytes);
    return (int) (hash & Integer.MAX_VALUE);
  }
}
