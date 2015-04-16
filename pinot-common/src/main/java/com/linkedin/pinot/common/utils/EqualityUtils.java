package com.linkedin.pinot.common.utils;

import java.util.Arrays;


/**
 * Various utilities to be used in implementing equals and hashCode.
 *
 * @author jfim
 */
public class EqualityUtils {
  private EqualityUtils(){}

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
