/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree.hll;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


/**
 * Utility functions for manipulation of hll field.
 */
public class HllUtil {
  /**
   * To display a row with hll fields properly,
   * instead of directly invoking {@link GenericRow#toString()},
   * hll fields should be inspected and transformed.
   *
   * @param row GenericRow
   * @param hllDeriveColumnSuffix column with this suffix will be treated as hll column
   * @return string representation of row
   */
  public static String inspectGenericRow(GenericRow row, String hllDeriveColumnSuffix) {
    StringBuilder b = new StringBuilder();
    for (String name : row.getFieldNames()) {
      b.append(name);
      b.append(" : ");
      Object value = row.getValue(name);
      if (value instanceof String && name.endsWith(hllDeriveColumnSuffix)) {
        // hll field
        b.append(convertStringToHll((String) value).cardinality());
      } else if (value instanceof Object[]) {
        b.append(Arrays.toString((Object[]) value));
      } else {
        b.append(value);
      }
      b.append(", ");
    }
    return b.toString();
  }

  public static byte[] toBytes(HyperLogLog hll) {
    try {
      return hll.getBytes();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String convertHllToString(HyperLogLog hll) {
    return new String(SerializationConverter.byteArrayToChars(toBytes(hll)));
  }

  public static HyperLogLog convertStringToHll(String s) {
    return buildHllFromBytes(SerializationConverter.charsToByteArray(s.toCharArray()));
  }

  /**
   * Generate a hll from a single value, and convert it to string type.
   * It is used for default derived field value.
   * @param log2m
   * @param value
   * @return
   */
  public static String singleValueHllAsString(int log2m, Object value) {
    HyperLogLog hll = new HyperLogLog(log2m);
    hll.offer(value);
    return convertHllToString(hll);
  }

  public static HyperLogLog buildHllFromBytes(byte[] bytes) {
    try {
      return HyperLogLog.Builder.build(bytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static HyperLogLog clone(HyperLogLog hll, int log2m) {
    try {
      HyperLogLog ret = new HyperLogLog(log2m);
      ret.addAll(hll);
      return ret;
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Merge all HLLs in list to the first HLL in the list, the list must contain at least one element
   * @param resultList
   * @return
   */
  public static HyperLogLog mergeHLLResultsToFirstInList(List<HyperLogLog> resultList) {
    HyperLogLog hllResult = resultList.get(0);
    for (int i = 1; i < resultList.size(); ++i) {
      try {
        hllResult.addAll(resultList.get(i));
      } catch (CardinalityMergeException e) {
        Utils.rethrowException(e);
      }
    }
    return hllResult;
  }

  /**
   * Convert between byte array and char array, one byte is mapped to one char and vice versa.
   * This is due to UTF-8 encoding for String type serialization used all over the system.
   */
  public static class SerializationConverter {
    private static final int BYTE_TO_CHAR_OFFSET = 129; // we choose 129 since normally we leave \0 for padding.

    public static char[] byteArrayToChars(byte[] byteArray) {
      char[] charArrayBuffer = new char[byteArray.length];
      for (int i = 0; i < byteArray.length; i++) {
        charArrayBuffer[i] = byteToChar(byteArray[i]);
      }
      return charArrayBuffer;
    }

    public static byte[] charsToByteArray(char[] chars) {
      byte[] ret = new byte[chars.length];
      for (int i = 0; i < ret.length; i++) {
        ret[i] = charToByte(chars[i]);
      }
      return ret;
    }

    public static char byteToChar(byte b) {
      return (char)(((int)b) + BYTE_TO_CHAR_OFFSET);
    }

    public static byte charToByte(char c) {
      return (byte)(((int)c) - BYTE_TO_CHAR_OFFSET);
    }
  }
}
