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
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;


/**
 * Utility class to compute hash values using different hash functions.
 * This class provides consistent hash computation for KeySelector implementations.
 */
public class HashFunctionSelector {
  public static final String MURMUR2 = "murmur";
  public static final String MURMUR3 = "murmur3";
  public static final String HASH_CODE = "hashcode";

  private HashFunctionSelector() {
  }

  /**
   * Computes a hash code for a single value using the specified hash function.
   * @param value The value to hash.
   * @param hashFunction The hash function to use (e.g., "murmur", "murmur3", "cityhash", "absHashCode").
   * @return The computed hash code.
   */
  public static int computeHash(Object value, String hashFunction) {
    if (value == null) {
      return 0;
    }

    switch (hashFunction.toLowerCase()) {
      case MURMUR2: return murmur2(value);
      case MURMUR3: return murmur3(value);
      // hashCode and absHashCode are treated the same for single hash.
      case HASH_CODE:
      // Default hash is absHashCode.
      default: return absHashCode(value);
    }
  }

  /**
   * Computes a hash code for multiple values based on specified key IDs using the specified hash function.
   * This is useful for partitioning where only certain keys are relevant.
   * @param values The array of values to hash.
   * @param keyIds The array of key IDs indicating which values to include in the hash computation.
   * @param hashFunction The hash function to use (e.g., "murmur2", "murmur3", "cityhash", "absHashCode").
   * @return The computed hash code.
   */
  public static int computeMultiHash(Object[] values, int[] keyIds, String hashFunction) {
    if (values == null || values.length == 0) {
      return 0;
    }

    switch (hashFunction.toLowerCase()) {
      case MURMUR2: return murmur2(values, keyIds);
      case MURMUR3: return murmur3(values, keyIds);
      // hashCode and absHashCode are treated the same for multi hash.
      case HASH_CODE:
        // We should hashCode instead of absHashCode for multi hash to maintain consistency with legacy behavior.
      default: return hashCode(values, keyIds);
    }
  }

  private static int absHashCode(Object value) {
    return value.hashCode() & Integer.MAX_VALUE;
  }

  private static int hashCode(Object value) {
    return value.hashCode();
  }

  private static int murmur2(Object value) {
    return MurmurHashFunctions.murmurHash2(toBytes(value)) & Integer.MAX_VALUE;
  }

  private static int murmur3(Object value) {
    return MurmurHashFunctions.murmurHash3X64Bit32(toBytes(value), 0) & Integer.MAX_VALUE;
  }

  private static int murmur2(Object[] values, int[] keyIds) {
    int hash = 0;
    for (int keyId : keyIds) {
      if (keyId < values.length && values[keyId] != null) {
        hash += murmur2(values[keyId]);
      }
    }
    return hash & Integer.MAX_VALUE;
  }

  private static int murmur3(Object[] values, int[] keyIds) {
    int hash = 0;
    for (int keyId : keyIds) {
      if (keyId < values.length && values[keyId] != null) {
        hash += murmur3(values[keyId]);
      }
    }
    return hash & Integer.MAX_VALUE;
  }

  private static int hashCode(Object[] values, int[] keyIds) {
    int hash = 0;
    for (int keyId : keyIds) {
      if (keyId < values.length && values[keyId] != null) {
        hash += hashCode(values[keyId]);
      }
    }
    return hash & Integer.MAX_VALUE;
  }

  private static byte[] toBytes(Object value) {
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }
}
