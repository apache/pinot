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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Base64;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BytesUtils;

import static org.apache.pinot.common.utils.PinotDataType.DOUBLE;
import static org.apache.pinot.common.utils.PinotDataType.INTEGER;
import static org.apache.pinot.common.utils.PinotDataType.LONG;
import static org.apache.pinot.common.utils.PinotDataType.STRING;


/**
 * Contains function to convert a datatype to another datatype.
 */
public class DataTypeConversionFunctions {
  private DataTypeConversionFunctions() {
  }

  @ScalarFunction
  public static Object cast(Object value, String targetTypeLiteral) {
    if (value == null) {
      return null;
    }
    try {
      Class<?> clazz = value.getClass();
      // TODO: Support cast for MV
      Preconditions.checkArgument(!clazz.isArray() | clazz == byte[].class, "%s must not be an array type", clazz);
      PinotDataType sourceType = PinotDataType.getSingleValueType(clazz);
      String transformed = targetTypeLiteral.toUpperCase();
      PinotDataType targetDataType;
      if ("INT".equals(transformed)) {
        targetDataType = INTEGER;
      } else if ("VARCHAR".equals(transformed)) {
        targetDataType = STRING;
      } else {
        targetDataType = PinotDataType.valueOf(transformed);
      }
      if (sourceType == STRING && (targetDataType == INTEGER || targetDataType == LONG)) {
        if (String.valueOf(value).contains(".")) {
          // convert integers via double to avoid parse errors
          return targetDataType.convert(DOUBLE.convert(value, sourceType), DOUBLE);
        }
      }
      return targetDataType.convert(value, sourceType);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown data type: " + targetTypeLiteral);
    }
  }

  /**
   * Converts {@link BigDecimal} to bytes.
   * Only scale of upto 2 bytes is supported by the function
   * @param bigDecimal big decimal number
   * @return The result byte array contains the bytes of the unscaled value appended to bytes of the scale in BIG
   * ENDIAN order.
   */
  @ScalarFunction
  public static byte[] bigDecimalToBytes(BigDecimal bigDecimal) {
    return BigDecimalUtils.serialize(bigDecimal);
  }

  /**
   * Converts serialized {@link BigDecimal} bytes to value.
   * @param bytes array that contains the bytes of the unscaled value appended to 2 bytes of the scale in BIG ENDIAN
   *              order.
   * @return big decimal number
   */
  @ScalarFunction
  public static BigDecimal bytesToBigDecimal(byte[] bytes) {
    return BigDecimalUtils.deserialize(bytes);
  }

  /**
   * convert simple hex string to byte array
   * @param hex a plain hex string e.g. 'f0e1a3b2'
   * @return byte array representation of hex string
   */
  @ScalarFunction
  public static byte[] hexToBytes(String hex) {
    return BytesUtils.toBytes(hex);
  }

  /**
   * convert simple bytes array to hex string
   * @param bytes any byte array
   * @return plain hex string e.g. 'f012be3c'
   */
  @ScalarFunction
  public static String bytesToHex(byte[] bytes) {
    return BytesUtils.toHexString(bytes);
  }

  /**
   * Encodes bytes using {@link Base64}.
   * @param input original bytes
   * @return base64 encoded bytes
   */
  @ScalarFunction
  public static byte[] base64Encode(byte[] input) {
    return Base64.getEncoder().encode(input);
  }

  /**
   * Decodes {@link Base64} encoded bytes.
   * @param input base64 encoded bytes
   * @return decoded bytes
   */
  @ScalarFunction
  public static byte[] base64Decode(byte[] input) {
    return Base64.getDecoder().decode(input);
  }
}
