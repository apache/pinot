/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Various utilities to be used in implementing equals and hashCode.
 *
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

  public static boolean isEqual(float left, float right) {
    return Float.compare(left, right) == 0;
  }

  public static boolean isEqual(double left, double right) {
    return Double.compare(left, right) == 0;
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

  public static boolean isEqual(Object[] left, Object[] right) {
    return Arrays.deepEquals(left, right);
  }

  public static boolean isEqual(Collection left, Collection right) {
    if (left != null && right != null) {
      return left.toString().equals(right.toString());
    } else {
      return left == right;
    }
  }

  public static boolean isEqualIgnoringOrder(List left, List right) {
    if (left != null && right != null) {
      ArrayList sortedLeft = new ArrayList(left);
      ArrayList sortedRight = new ArrayList(right);
      Collections.sort(sortedLeft);
      Collections.sort(sortedRight);
      return sortedLeft.toString().equals(sortedRight.toString());
    } else {
      return left == right;
    }
  }

  public static boolean isEqual(Map left, Map right) {
    if (left != null && right != null) {
      if (left.size() != right.size()) {
        return false;
      }
      for (Object key : left.keySet()) {
        if (right.containsKey(key)) {
          Object leftValue = left.get(key);
          Object rightValue = right.get(key);
          if (!isEqual(leftValue, rightValue)) {
            return false;
          }
        } else {
          return false;
        }
      }
      return true;
    } else {
      return left == right;
    }
  }

  public static boolean isEqual(Object left, Object right) {
    if (left != null)
      return left.equals(right);
    else
      return null == right;
  }

  public static boolean isNullOrNotSameClass(Object left, Object right) {
    return right == null || left.getClass() != right.getClass();
  }

  public static boolean isSameReference(Object left, Object right) {
    return left == right;
  }

  public static int hashCodeOf(Object o) {
    if (o != null)
      return o.hashCode();
    else
      return 0;
  }

  public static int hashCodeOf(int previousHashCode, Object o) {
    return 31 * previousHashCode + hashCodeOf(o);
  }

  public static int hashCodeOf(int previousHashCode, int value) {
    return 31 * previousHashCode + value;
  }

  public static int hashCodeOf(int previousHashCode, long value) {
    return 31 * previousHashCode + (int) (value ^ (value >>> 32));
  }
}
