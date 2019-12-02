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
package org.apache.pinot.spi.utils;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;


public class BytesUtils {
  private BytesUtils() {
  }

  /**
   * Converts a Hex encoded string to a byte array.
   *
   * @param stringValue Hex encoded string
   * @return Decoded byte array
   */
  public static byte[] toBytes(String stringValue) {
    try {
      return Hex.decodeHex(stringValue.toCharArray());
    } catch (DecoderException e) {
      throw new IllegalArgumentException("Value: " + stringValue + " is not Hex encoded", e);
    }
  }

  /**
   * Converts a Hex encoded string to a {@link ByteArray}.
   *
   * @param stringValue Hex encoded string
   * @return Decoded {@link ByteArray}
   */
  public static ByteArray toByteArray(String stringValue) {
    return new ByteArray(toBytes(stringValue));
  }

  /**
   * Converts the byte array to a Hex encoded string.
   *
   * @param bytes byte array
   * @return Hex encoded string
   */
  public static String toHexString(byte[] bytes) {
    return Hex.encodeHexString(bytes);
  }
}
