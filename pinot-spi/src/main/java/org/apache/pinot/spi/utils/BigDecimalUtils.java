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
import java.math.MathContext;


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
   * Serializes a big decimal to a byte array.
   */
  public static byte[] serialize(BigDecimal value) {
    int scale = value.scale();
    BigInteger unscaledValue = value.unscaledValue();
    byte[] unscaledValueBytes = unscaledValue.toByteArray();
    byte[] valueBytes = new byte[unscaledValueBytes.length + 2];
    valueBytes[0] = (byte) (scale >>> 8);
    valueBytes[1] = (byte) scale;
    System.arraycopy(unscaledValueBytes, 0, valueBytes, 2, unscaledValueBytes.length);
    return valueBytes;
  }

  /**
   * Deserializes a big decimal from a byte array.
   */
  public static BigDecimal deserialize(byte[] bytes) {
    int scale = ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
    byte[] unscaledValueBytes = new byte[bytes.length - 2];
    System.arraycopy(bytes, 2, unscaledValueBytes, 0, unscaledValueBytes.length);
    BigInteger unscaledValue = new BigInteger(unscaledValueBytes);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Converts a numeric string to a big decimal.
   *
   * @param stringValue Numeric string
   * @return {@link BigDecimal}
   */
  public static BigDecimal toBigDecimal(String stringValue) {
    return new BigDecimal(stringValue);
  }

  /**
   * Converts a numeric string to a big decimal.
   *
   * @param stringValue Numeric string
   * @param precision Total number of digits of the big decimal
   * @param scale Number of decimal digits
   * @return {@link BigDecimal}
   */
  public static BigDecimal createBigDecimal(String stringValue, int precision, int scale) {
    MathContext mathContext = new MathContext(precision);
    return new BigDecimal(stringValue, mathContext).setScale(scale, mathContext.getRoundingMode());
  }

  /**
   * A ballpark min value for Dimensions.
   *
   * @param scale Number of decimal digits
   * @return {@link BigDecimal}
   */
  public static BigDecimal referenceMinValue(int scale) {
    return createBigDecimal(Double.toString(-Double.MAX_VALUE), Integer.MAX_VALUE, scale);
  }
}
