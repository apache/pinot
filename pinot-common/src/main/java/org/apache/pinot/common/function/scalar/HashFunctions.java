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
package org.apache.pinot.common.function.scalar;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.hash.CityHashFunctions;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;


/**
 * Inbuilt Hash Transformation Functions
 * The functions can be used as UDFs in Query when added in the FunctionRegistry.
 * @ScalarFunction annotation is used with each method for the registration
 *
 * Example usage:
 * <code> SELECT SHA(bytesColumn) FROM baseballStats LIMIT 10 </code>
 * <code> SELECT SHA256(bytesColumn) FROM baseballStats LIMIT 10 </code>
 * <code> SELECT SHA512(bytesColumn) FROM baseballStats LIMIT 10 </code>
 * <code> SELECT MD5(bytesColumn) FROM baseballStats LIMIT 10 </code>
 */
public class HashFunctions {
  private HashFunctions() {
  }

  /**
   * Return SHA-1 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static String sha(byte[] input) {
    return DigestUtils.sha1Hex(input);
  }

  /**
   * Return SHA-224 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static String sha224(byte[] input) {
    return DigestUtils.sha3_224Hex(input);
  }

  /**
   * Return SHA-256 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static String sha256(byte[] input) {
    return DigestUtils.sha256Hex(input);
  }

  /**
   * Return SHA-512 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static String sha512(byte[] input) {
    return DigestUtils.sha512Hex(input);
  }

  /**
   * Return MD2 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static String md2(byte[] input) {
    return DigestUtils.md2Hex(input);
  }

  /**
   * Return MD5 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static String md5(byte[] input) {
    return DigestUtils.md5Hex(input);
  }

  /**
   * Computes 32-bit MurmurHash2 of the given byte array.
   *
   * @param input the byte array to hash
   * @return 32-bit hash
   */
  @ScalarFunction
  public static int murmurHash2(byte[] input) {
    return MurmurHashFunctions.murmurHash2(input);
  }

  /**
   * Computes 32-bit MurmurHash2 of the given string.
   *
   * @param input the byte array to hash
   * @return 32-bit hash
   */
  @ScalarFunction
  public static int murmurHash2UTF8(String input) {
    return MurmurHashFunctions.murmurHash2(input.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Computes 64-bit MurmurHash2 of the given byte array.
   *
   * @param input the byte array to hash
   * @return 64-bit hash
   */
  @ScalarFunction
  public static long murmurHash2Bit64(byte[] input) {
    return MurmurHashFunctions.murmurHash2Bit64(input);
  }

  /**
   * Computes 64-bit MurmurHash2 of the given byte array and seed.
   *
   * @param input the byte array to hash
   * @return 64-bit hash
   */
  @ScalarFunction
  public static long murmurHash2Bit64(byte[] input, int seed) {
    return MurmurHashFunctions.murmurHash2Bit64(input, input.length, seed);
  }

  /**
   * Computes 32-bit Murmur3 Hash of the given byte array and seed.
   *
   * @param input the byte array to hash
   * @return 32-bit hash
   */
  @ScalarFunction
  public static int murmurHash3Bit32(byte[] input, int seed) {
    return Hashing.murmur3_32_fixed(seed).hashBytes(input).asInt();
  }

  /**
   * Computes 64-bit Murmur3 Hash of the given byte array and seed.
   *
   * @param input the byte array to hash
   * @return 64-bit hash
   */
  @ScalarFunction
  public static long murmurHash3Bit64(byte[] input, int seed) {
    return Hashing.murmur3_128(seed).hashBytes(input).asLong();
  }

  /**
   * Computes 128-bit Murmur3 Hash of the given byte array and seed.
   *
   * @param input the byte array to hash
   * @return 128-bit hash represented in a 16-byte array
   */
  @ScalarFunction
  public static byte[] murmurHash3Bit128(byte[] input, int seed) {
    return Hashing.murmur3_128(seed).hashBytes(input).asBytes();
  }

  /**
   * Computes 32-bit Murmur3 Hash of the given byte array and seed for x64 platform.
   *
   * @param input the byte array to hash
   * @return 32-bit hash
   */
  @ScalarFunction
  public static int murmurHash3X64Bit32(byte[] input, int seed) {
    return MurmurHashFunctions.murmurHash3X64Bit32(input, seed);
  }

  /**
   * Computes 64-bit Murmur3 Hash of the given byte array and seed for x64 platform.
   *
   * @param input the byte array to hash
   * @return 64-bit hash
   */
  @ScalarFunction
  public static long murmurHash3X64Bit64(byte[] input, int seed) {
    return MurmurHashFunctions.murmurHash3X64Bit32(input, seed);
  }

  /**
   * Computes 128-bit Murmur3 Hash of the given byte array and seed for x64 platform.
   *
   * @param input the byte array to hash
   * @return 128-bit hash represented in a 16-byte array
   */
  @ScalarFunction
  public static byte[] murmurHash3X64Bit128(byte[] input, int seed) {
    return MurmurHashFunctions.murmurHash3X64Bit128(input, seed);
  }

  /**
   * Computes 32-bit Adler Hash of the given byte array
   * @param input the byte array to hash
   * @return 32-bit hash
   */
  @ScalarFunction
  public static int adler32(byte[] input) {
    return Hashing.adler32().hashBytes(input).asInt();
  }

  /**
   * Computes 32-bit CRC (Cyclic Redundancy Check)  of the given byte array
   * @param input the byte array to hash
   * @return 32-bit CRC32 hash
   */
  @ScalarFunction
  public static int crc32(byte[] input) {
    return Hashing.crc32().hashBytes(input).asInt();
  }

  /**
   * Computes 32-bit CRC32C (Cyclic Redundancy Check 32C) of the given byte array.
   *
   * @param input the byte array to hash
   * @return 32-bit CRC32C hash
   */
  @ScalarFunction
  public static int crc32c(byte[] input) {
    return Hashing.crc32c().hashBytes(input).asInt();
  }

  /**
   * Computes 32-bit CityHash of the given byte array
   *
   * @param input the byte array to hash
   * @return 32-bit CityHash
   */
  @ScalarFunction
  public static long cityHash32(byte[] input) {
    return CityHashFunctions.cityHash32(input);
  }

  /**
   * Computes 64-bit CityHash of the given byte array
   *
   * @param input the byte array to hash
   * @return 64-bit CityHash
   */
  @ScalarFunction
  public static long cityHash64(byte[] input) {
    return CityHashFunctions.cityHash64(input);
  }

  /**
   * Computes 64-bit CityHash of the given byte array and the seed
   *
   * @param input the byte array to hash
   * @param seed the seed
   * @return 64-bit CityHash
   */
  @ScalarFunction
  public static long cityHash64(byte[] input, long seed) {
    return CityHashFunctions.cityHash64WithSeed(input, seed);
  }

  /**
   * Computes 64-bit CityHash of the given byte array and the two seeds
   *
   * @param input the byte array to hash
   * @param seed1 the first seed value
   * @param seed2 the second seed value
   * @return 64-bit CityHash
   */
  @ScalarFunction
  public static long cityHash64(byte[] input, long seed1, long seed2) {
    return CityHashFunctions.cityHash64WithSeeds(input, seed1, seed2);
  }
  /**
   * Computes 128-bit CityHash of the given byte array
   *
   * @param input the byte array to hash
   * @return 128-bit CityHash represented in a 16-byte array
   */
  @ScalarFunction
  public static byte[] cityHash128(byte[] input) {
    return CityHashFunctions.cityHash128(input);
  }
}
