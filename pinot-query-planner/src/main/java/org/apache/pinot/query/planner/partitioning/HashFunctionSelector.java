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
  public static final String MURMUR2 = "murmur2";
  public static final String MURMUR3 = "murmur3";
  public static final String CITY_HASH = "cityhash";
  public static final String ABS_HASH_CODE = "absHashCode";

  private HashFunctionSelector() {
  }

  /**
   * Computes a hash code for a single value using the specified hash function.
   * @param value The value to hash.
   * @param hashFunction The hash function to use (e.g., "murmur2", "murmur3", "cityhash", "absHashCode").
   * @return The computed hash code.
   */
  public static int computeHash(Object value, String hashFunction) {
    if (value == null) {
      return 0;
    }

    switch (hashFunction.toLowerCase()) {
      case MURMUR2: return murmur2(value);
      case MURMUR3: return murmur3(value);
      case CITY_HASH: return cityHash(value);
      // Default hash is absHashCode.
      default: return absHashCode(value);
    }
  }

  /**
   * Computes a hash code for multiple values using the specified hash function.
   * @param values The array of values to hash.
   * @param hashFunction The hash function to use (e.g., "murmur2", "murmur3", "cityhash", "absHashCode").
   * @return The computed hash code.
   */
  public static int computeMultiHash(Object[] values, String hashFunction) {
    if (values == null || values.length == 0) {
      return 0;
    }

    switch (hashFunction.toLowerCase()) {
      case MURMUR2: return multiHash(values, HashFunctionSelector::murmur2);
      case MURMUR3: return multiHash(values, HashFunctionSelector::murmur3);
      case CITY_HASH: return multiHash(values, HashFunctionSelector::cityHash);
      // Default hash is absHashCode.
      default: return multiAbsHashCode(values);
    }
  }

  private static int absHashCode(Object value) {
    return value.hashCode() & Integer.MAX_VALUE;
  }

  // multiAbsHashCode is different from absHashCode due to legacy implementation.
  private static int multiAbsHashCode(Object[] values) {
    int hash = 0;
    for (Object value : values) {
      if (value != null) {
        hash += value.hashCode();
      }
    }
    return hash & Integer.MAX_VALUE;
  }

  private static int murmur2(Object value) {
    return MurmurHashFunctions.murmurHash2(toBytes(value)) & Integer.MAX_VALUE;
  }

  private static int murmur3(Object value) {
    return MurmurHashFunctions.murmurHash3X64Bit32(toBytes(value), 0) & Integer.MAX_VALUE;
  }

  private static int cityHash(Object value) {
    return (int) (CityHashFunctions.cityHash64(toBytes(value)) & Integer.MAX_VALUE);
  }

  private static int multiHash(Object[] values, java.util.function.ToIntFunction<Object> hashFunction) {
    int hash = 0;
    for (Object value : values) {
      if (value != null) {
        hash += hashFunction.applyAsInt(value);
      }
    }
    return hash & Integer.MAX_VALUE;
  }

  private static byte[] toBytes(Object value) {
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }
}
