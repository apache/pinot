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

import java.io.Serializable;
import java.util.Arrays;
import javax.annotation.Nonnull;


/**
 * Wrapper around byte[] that provides additional features such as:
 * <ul>
 *   <li> Implements comparable interface, so comparison and sorting can be performed. </li>
 *   <li> Implements equals() and hashCode(), so it can be used as key for HashMap/Set. </li>
 * </ul>
 */
public class ByteArray implements Comparable<ByteArray>, Serializable {
  private final byte[] _bytes;

  public ByteArray(byte[] bytes) {
    _bytes = bytes;
  }

  public byte[] getBytes() {
    return _bytes;
  }

  public int length() {
    return _bytes.length;
  }

  public String toHexString() {
    return BytesUtils.toHexString(_bytes);
  }

  @Override
  public String toString() {
    return toHexString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ByteArray bytes = (ByteArray) o;

    return Arrays.equals(_bytes, bytes._bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_bytes);
  }

  @Override
  public int compareTo(@Nonnull ByteArray that) {
    if (this == that) {
      return 0;
    }

    return compare(_bytes, that._bytes);
  }

  /**
   * Compares two byte[] values. The comparison performed is on unsigned value for each byte.
   * Returns:
   * <ul>
   *   <li> 0 if both values are identical. </li>
   *   <li> -ve integer if first value is smaller than the second. </li>
   *   <li> +ve integer if first value is larger than the second. </li>
   * </ul>
   *
   * @param bytes1 First byte[] to compare.
   * @param bytes2 Second byte[] to compare.
   * @return Result of comparison as stated above.
   */
  public static int compare(byte[] bytes1, byte[] bytes2) {
    int len1 = bytes1.length;
    int len2 = bytes2.length;
    int lim = Math.min(len1, len2);

    for (int k = 0; k < lim; k++) {
      // Java byte is always signed, but we need to perform unsigned comparison.
      int ai = Byte.toUnsignedInt(bytes1[k]);
      int bi = Byte.toUnsignedInt(bytes2[k]);
      if (ai != bi) {
        return ai - bi;
      }
    }
    return len1 - len2;
  }
}
