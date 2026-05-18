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

import java.util.Locale;
import javax.annotation.Nullable;


/**
 * Stateless Fowler-Noll-Vo hash helpers for stable 32-bit and 64-bit hashing.
 */
public final class FnvHashFunctions {
  private static final String ALLOWED_VARIANTS = "fnv1_32, fnv1a_32, fnv1_64 or fnv1a_64";
  private static final int FNV_32_OFFSET_BASIS = 0x811c9dc5;
  private static final int FNV_32_PRIME = 0x01000193;
  private static final long FNV_64_OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long FNV_64_PRIME = 0x100000001b3L;

  private FnvHashFunctions() {
  }

  public enum Variant {
    FNV1_32,
    FNV1A_32,
    FNV1_64,
    FNV1A_64;

    public static Variant fromString(String value) {
      if (value == null) {
        throw invalidVariantException(null);
      }
      try {
        return valueOf(value.trim().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw invalidVariantException(value);
      }
    }

    public boolean is64Bit() {
      return this == FNV1_64 || this == FNV1A_64;
    }
  }

  private static IllegalArgumentException invalidVariantException(@Nullable String value) {
    return new IllegalArgumentException(
        "FNV variant must be one of " + ALLOWED_VARIANTS + ", but was: " + formatValue(value));
  }

  private static String formatValue(@Nullable String value) {
    return value == null ? "null" : "'" + value + "'";
  }

  /**
   * Computes the 32-bit FNV-1 hash for the given bytes.
   */
  public static int fnv1Hash32(byte[] bytes) {
    int hash = FNV_32_OFFSET_BASIS;
    for (byte value : bytes) {
      hash *= FNV_32_PRIME;
      hash ^= value & 0xFF;
    }
    return hash;
  }

  /**
   * Computes the 32-bit FNV-1a hash for the given bytes.
   */
  public static int fnv1aHash32(byte[] bytes) {
    int hash = FNV_32_OFFSET_BASIS;
    for (byte value : bytes) {
      hash ^= value & 0xFF;
      hash *= FNV_32_PRIME;
    }
    return hash;
  }

  /**
   * Computes the 64-bit FNV-1 hash for the given bytes.
   */
  public static long fnv1Hash64(byte[] bytes) {
    long hash = FNV_64_OFFSET_BASIS;
    for (byte value : bytes) {
      hash *= FNV_64_PRIME;
      hash ^= value & 0xFFL;
    }
    return hash;
  }

  /**
   * Computes the 64-bit FNV-1a hash for the given bytes.
   */
  public static long fnv1aHash64(byte[] bytes) {
    long hash = FNV_64_OFFSET_BASIS;
    for (byte value : bytes) {
      hash ^= value & 0xFFL;
      hash *= FNV_64_PRIME;
    }
    return hash;
  }
}
