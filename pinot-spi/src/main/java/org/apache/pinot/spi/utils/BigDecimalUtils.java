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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;


public class BigDecimalUtils {
  private BigDecimalUtils() {
  }

  /**
   * Returns the number of bytes in the serialized big decimal.
   */
  public static int byteSize(BigDecimal value) {
    BigInteger unscaledValue = value.unscaledValue();
    return (unscaledValue.bitLength() >>> 3) + 3;
  }

  /**
   * This gets the expected byte size of a big decimal with a specific precision.
   * It is equal to (ceil(log2(10^precision) - 1)
   */
  public static int byteSizeForFixedPrecision(int precision) {
    BigDecimal bd = generateMaximumNumberWithPrecision(precision);
    return byteSize(bd);
  }

  /**
   * Serializes a big decimal to a byte array.
   */
  public static byte[] serialize(BigDecimal value) {
    int scale = value.scale();
    BigInteger unscaledValue = value.unscaledValue();
    byte[] unscaledValueBytes = unscaledValue.toByteArray();
    byte[] valueBytes = new byte[unscaledValueBytes.length + 2];
    valueBytes[0] = (byte) (scale >> 8);
    valueBytes[1] = (byte) scale;
    System.arraycopy(unscaledValueBytes, 0, valueBytes, 2, unscaledValueBytes.length);
    return valueBytes;
  }

  public static byte[] serializeWithSize(BigDecimal value, int fixedSize) {
    int scale = value.scale();
    BigInteger unscaledValue = value.unscaledValue();
    byte[] unscaledValueBytes = unscaledValue.toByteArray();

    int unscaledBytesStartingIndex = fixedSize - unscaledValueBytes.length;
    if (unscaledValueBytes.length > (fixedSize - 2)) {
      throw new IllegalArgumentException("Big decimal of size " + (unscaledValueBytes.length + 2)
          + " is too big to serialize into a fixed size of " + fixedSize + " bytes");
    }

    byte[] valueBytes = new byte[fixedSize];
    valueBytes[0] = (byte) (scale >> 8);
    valueBytes[1] = (byte) scale;

    byte paddingByte = 0;
    if (value.signum() < 0) {
      paddingByte = -1;
    }

    for (int i = 2; i < unscaledBytesStartingIndex; i++) {
      valueBytes[i] = paddingByte;
    }

    System.arraycopy(unscaledValueBytes, 0, valueBytes, unscaledBytesStartingIndex, unscaledValueBytes.length);
    return valueBytes;
  }

  /**
   * Deserializes a big decimal from a byte array.
   */
  public static BigDecimal deserialize(byte[] bytes) {
    int scale = (short) ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
    byte[] unscaledValueBytes = new byte[bytes.length - 2];
    System.arraycopy(bytes, 2, unscaledValueBytes, 0, unscaledValueBytes.length);
    BigInteger unscaledValue = new BigInteger(unscaledValueBytes);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Deserializes a big decimal from ByteArray.
   */
  public static BigDecimal deserialize(ByteArray byteArray) {
    return deserialize(byteArray.getBytes());
  }

  /**
   * Deserializes a big decimal from ByteBuffer.
   */
  public static BigDecimal deserialize(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return deserialize(bytes);
  }

  public static BigDecimal generateMaximumNumberWithPrecision(int precision) {
    return (new BigDecimal("10")).pow(precision).subtract(new BigDecimal("1"));
  }
}
