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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;

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
    return DigestUtils.shaHex(input);
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
   * Return MD5 digest as hex string.
   *
   * @param input the byte array representing the data
   * @return hash string in hex format
   */
  @ScalarFunction
  public static String md5(byte[] input) {
    return DigestUtils.md5Hex(input);
  }
}
