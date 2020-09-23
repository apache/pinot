package org.apache.pinot.common.function.scalar;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.pinot.common.function.annotations.ScalarFunction;


public class DataTypeConversionFunctions {
  private DataTypeConversionFunctions() {

  }

  @ScalarFunction
  public static byte[] bigDecimalToBytes(BigDecimal number) {
    int scale = number.scale();
    BigInteger unscaled = number.unscaledValue();
    byte[] value = unscaled.toByteArray();
    byte[] bigDecimalBytesArray = new byte[value.length + 4];
    for (int i = 0; i < 4; i++) {
      bigDecimalBytesArray[i] = (byte) (scale >>> (8 * (3 - i)));
    }
    System.arraycopy(value, 0, bigDecimalBytesArray, 4, value.length);
    return bigDecimalBytesArray;
  }

  @ScalarFunction
  public static String bytesToBigDecimal(byte[] bytes) {
    int scale = 0;
    for (int i = 0; i < 4; i++) {
      scale += (((int) bytes[i]) << (8 * (3 - i)));
    }
    byte[] vals = new byte[bytes.length - 4];
    System.arraycopy(bytes, 4, vals, 0, vals.length);
    BigInteger unscaled = new BigInteger(vals);
    BigDecimal number = new BigDecimal(unscaled, scale);
    return number.toString();
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
}
