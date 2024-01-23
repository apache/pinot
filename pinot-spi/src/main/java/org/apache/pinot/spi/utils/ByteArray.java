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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper around byte[] that provides additional features such as:
 * <ul>
 *   <li>Implements comparable interface, so comparison and sorting can be performed</li>
 *   <li>Implements equals() and hashCode(), so it can be used as key for HashMap/Set</li>
 *   <li>Caches the hash code of the byte[]</li>
 * </ul>
 */
public class ByteArray implements Comparable<ByteArray>, Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ByteArray.class);

  private static final MethodHandle COMPARE_UNSIGNED;

  static {
    MethodHandle compareUnsigned = null;
    try {
      compareUnsigned = MethodHandles.publicLookup().findStatic(Arrays.class, "compareUnsigned",
          MethodType.methodType(int.class, byte[].class, int.class, int.class, byte[].class, int.class, int.class));
    } catch (Exception ignored) {
      LOGGER.warn("Arrays.compareUnsigned unavailable - this may have a performance impact (are you using JDK8?)");
    }
    COMPARE_UNSIGNED = compareUnsigned;
  }

  private final byte[] _bytes;

  // Hash for empty ByteArray is 1
  private int _hash = 1;

  public ByteArray(byte[] bytes) {
    _bytes = bytes;
  }

  public ByteArray(byte[] bytes, @Nullable FALFInterner<byte[]> byteInterner) {
    if (byteInterner == null) {
      _bytes = bytes;
    } else {
      _bytes = byteInterner.intern(bytes);
    }
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
    int hash = _hash;
    if (hash == 1 && _bytes.length > 0) {
      int i = 0;
      for (; i + 7 < _bytes.length; i += 8) {
        hash = -1807454463 * hash
            + 1742810335 * _bytes[i]
            + 887503681 * _bytes[i + 1]
            + 28629151 * _bytes[i + 2]
            + 923521 * _bytes[i + 3]
            + 29791 * _bytes[i + 4]
            + 961 * _bytes[i + 5]
            + 31 * _bytes[i + 6]
            + _bytes[i + 7];
      }
      for (; i < _bytes.length; i++) {
        hash = 31 * hash + _bytes[i];
      }
      _hash = hash;
    }
    return hash;
  }

  @Override
  public int compareTo(ByteArray that) {
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
   * @param left First byte[] to compare.
   * @param right Second byte[] to compare.
   * @return Result of comparison as stated above.
   */
  public static int compare(byte[] left, byte[] right) {
    return compare(left, 0, left.length, right, 0, right.length);
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
   * @param left First byte[] to compare.
   * @param leftFromIndex inclusive index of first byte to compare in left
   * @param leftToIndex exclusive index of last byte to compare in left
   * @param right Second byte[] to compare.
   * @param rightFromIndex inclusive index of first byte to compare in right
   * @param rightToIndex exclusive index of last byte to compare in right
   * @return Result of comparison as stated above.
   */
  public static int compare(byte[] left, int leftFromIndex, int leftToIndex, byte[] right, int rightFromIndex,
      int rightToIndex) {
    if (COMPARE_UNSIGNED != null) {
      try {
        return (int) COMPARE_UNSIGNED.invokeExact(left, leftFromIndex, leftToIndex, right, rightFromIndex,
            rightToIndex);
      } catch (ArrayIndexOutOfBoundsException outOfBounds) {
        throw outOfBounds;
      } catch (Throwable ignore) {
      }
    }
    return compareFallback(left, leftFromIndex, leftToIndex, right, rightFromIndex, rightToIndex);
  }

  private static int compareFallback(byte[] left, int leftFromIndex, int leftToIndex, byte[] right, int rightFromIndex,
      int rightToIndex) {
    int len1 = leftToIndex - leftFromIndex;
    int len2 = rightToIndex - rightFromIndex;
    int lim = Math.min(len1, len2);

    for (int k = 0; k < lim; k++) {
      // Java byte is always signed, but we need to perform unsigned comparison.
      int ai = Byte.toUnsignedInt(left[k + leftFromIndex]);
      int bi = Byte.toUnsignedInt(right[k + rightFromIndex]);
      if (ai != bi) {
        return ai - bi;
      }
    }
    return len1 - len2;
  }
}
