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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Various utilities in implementing {@link Object#equals(Object)} and {@link Object#hashCode()}.
 *
 * For primitive float and double, {@code isEqual()} is not the same as Java == operator, {@code isEqual(NaN, NaN)}
 * returns true instead of false.
 */
public class EqualityUtils {
  private EqualityUtils() {
  }

  public static boolean isEqual(int left, int right) {
    return left == right;
  }

  public static boolean isEqual(long left, long right) {
    return left == right;
  }

  /**
   * Compare both arguments for equality, and consider {@code Float.NaN} to be equal to {@code Float.NaN} (unlike the
   * Java == operator).
   */
  public static boolean isEqual(float left, float right) {
    return Float.floatToIntBits(left) == Float.floatToIntBits(right);
  }

  /**
   * Compare both arguments for equality, and consider {@code Double.NaN} to be equal to {@code Double.NaN} (unlike the
   * Java == operator).
   */
  public static boolean isEqual(double left, double right) {
    return Double.doubleToLongBits(left) == Double.doubleToLongBits(right);
  }

  public static boolean isEqual(short left, short right) {
    return left == right;
  }

  public static boolean isEqual(char left, char right) {
    return left == right;
  }

  public static boolean isEqual(byte left, byte right) {
    return left == right;
  }

  public static boolean isEqual(@Nullable Object left, @Nullable Object right) {
    if (left != null && right != null) {
      if ((left instanceof Map) && (right instanceof Map)) {
        return EqualityUtils.isEqualMap((Map)left, (Map)right);
      }
      return left.equals(right);
    } else {
      return left == right;
    }
  }

  public static boolean isEqual(@Nullable Object[] left, @Nullable Object[] right) {
    return Arrays.deepEquals(left, right);
  }

  public static boolean isEqualMap(@Nullable Map left, @Nullable Map right) {
    if (left != null && right != null) {
      if (left.size() != right.size()) {
        return false;
      }
      for (Object key : left.keySet()) {
        if ((!right.containsKey(key)) || (!EqualityUtils.isEqual(left.get(key), right.get(key)))) {
          return false;
        }
      }
      return true;
    }
    return left == right;
  }

  @SuppressWarnings("unchecked")
  public static boolean isEqualIgnoreOrder(@Nullable List left, @Nullable List right) {
    if (left != null && right != null) {
      List sortedLeft = new ArrayList(left);
      List sortedRight = new ArrayList(right);
      Collections.sort(sortedLeft);
      Collections.sort(sortedRight);
      return sortedLeft.equals(sortedRight);
    } else {
      return left == right;
    }
  }

  public static boolean isEqualSet(@Nullable Set left, @Nullable Set right) {
    if (left != null && right != null) {
      return isEqualIgnoreOrder(Arrays.asList(left.toArray()), Arrays.asList(right.toArray()));
    }
    return left == right;
  }

  public static boolean isNullOrNotSameClass(@Nonnull Object left, @Nullable Object right) {
    return right == null || left.getClass() != right.getClass();
  }

  public static boolean isSameReference(@Nullable Object left, @Nullable Object right) {
    return left == right;
  }

  /**
   * Given an object, return the hashcode of it. For {@code null}, return 0 instead.
   */
  public static int hashCodeOf(@Nullable Object o) {
    if (o != null) {
      return o.hashCode();
    } else {
      return 0;
    }
  }

  public static int hashCodeOf(int previousHashCode, @Nullable Object o) {
    return 37 * previousHashCode + hashCodeOf(o);
  }

  public static int hashCodeOf(int previousHashCode, int value) {
    return 37 * previousHashCode + value;
  }

  public static int hashCodeOf(int previousHashCode, long value) {
    return 37 * previousHashCode + (int) (value ^ (value >>> 32));
  }

  public static int hashCodeOf(int previousHashCode, boolean value) {
    return 37 * previousHashCode + (value ? 1 : 0);
  }
}
