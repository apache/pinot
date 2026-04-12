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
package org.apache.pinot.segment.spi.function.scalar;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.hash.FnvHashFunctions;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;


/**
 * Test-only scalar functions used to exercise the partition-expression compiler inside the pinot-segment-spi module.
 */
public final class PartitionFunctionExprTestFunctions {
  private PartitionFunctionExprTestFunctions() {
  }

  @ScalarFunction
  public static String lower(String input) {
    return input.toLowerCase(Locale.ROOT);
  }

  @ScalarFunction
  public static String md5(byte[] input) {
    return BytesUtils.toHexString(md5Digest(input));
  }

  @ScalarFunction(names = {"md5_raw"})
  public static byte[] md5Raw(byte[] input) {
    return md5Digest(input);
  }

  @ScalarFunction(names = {"fnv1a_32"})
  public static int fnv1a32(byte[] input) {
    return FnvHashFunctions.fnv1aHash32(input);
  }

  @ScalarFunction
  public static int murmur2(byte[] input) {
    return MurmurHashFunctions.murmurHash2(input);
  }

  @ScalarFunction
  public static int identity(int value) {
    return value;
  }

  @ScalarFunction
  public static long bucket(long value, long divisor) {
    return value / divisor;
  }

  @ScalarFunction(names = {"cid"}, isDeterministic = false)
  public static String nonDeterministicCid(String input) {
    return input;
  }

  @ScalarFunction(isDeterministic = false)
  public static long randomBucket(long value) {
    return value + ThreadLocalRandom.current().nextLong();
  }

  private static byte[] md5Digest(byte[] input) {
    try {
      return MessageDigest.getInstance("MD5").digest(input);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 digest is not available", e);
    }
  }
}
