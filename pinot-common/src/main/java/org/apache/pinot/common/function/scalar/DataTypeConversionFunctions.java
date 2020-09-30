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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class DataTypeConversionFunctions {
  private DataTypeConversionFunctions() {

  }

  @ScalarFunction
  public static byte[] bigDecimalToBytes(String number) {
    BigDecimal bigDecimal = new BigDecimal(number);
    int scale = bigDecimal.scale();
    BigInteger unscaled = bigDecimal.unscaledValue();
    byte[] value = unscaled.toByteArray();
    byte[] bigDecimalBytesArray = new byte[value.length + 2];

    bigDecimalBytesArray[0] = (byte) (scale >>> 8);
    bigDecimalBytesArray[1] = (byte) (scale);

    System.arraycopy(value, 0, bigDecimalBytesArray, 2, value.length);
    return bigDecimalBytesArray;
  }

  @ScalarFunction
  public static String bytesToBigDecimal(byte[] bytes) {
    int scale = 0;
    scale += (((int) bytes[0]) << (8));
    scale += (((int) bytes[1]));
    byte[] vals = new byte[bytes.length - 2];
    System.arraycopy(bytes, 2, vals, 0, vals.length);
    BigInteger unscaled = new BigInteger(vals);
    BigDecimal number = new BigDecimal(unscaled, scale);
    return number.toString();
  }

  @ScalarFunction
  public static byte[] bigDecimalFromString(String bigDecimal) {
    return bigDecimalToBytes(bigDecimal);
  }

  @ScalarFunction
  public static byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }

  @ScalarFunction
  public static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X ", b));
    }

    return sb.toString();
  }

  @ScalarFunction
  public static byte[] base64Encode(byte[] input) {
    return Base64.getEncoder().encodeToString(input).getBytes();
  }

  @ScalarFunction
  public static byte[] base64Decode(String input) {
    return Base64.getDecoder().decode(input.getBytes());
  }
}
