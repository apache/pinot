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
package com.linkedin.pinot.common.utils;

import java.util.Comparator;

public class Pairs {

  public static IntPair intPair(int a, int b) {
    return new IntPair(a, b);
  }

  public static Comparator<IntPair> intPairComparator() {
    return new AscendingIntPairComparator();
  }

  public static class IntPair {
    int a;

    int b;

    public IntPair(int a, int b) {
      this.a = a;
      this.b = b;
    }

    public int getLeft() {
      return a;
    }

    public int getRight() {
      return b;
    }

    public void setLeft(int a) {
      this.a = a;
    }

    public void setRight(int b) {
      this.b = b;
    }

    @Override
    public String toString() {
      return "[" + a + "," + b + "]";
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IntPair) {
        IntPair that = (IntPair) obj;
        return obj != null && a == (that.a) && b == that.b;
      }
      return false;
    }
  }

  public static class AscendingIntPairComparator implements Comparator<IntPair> {

    @Override
    public int compare(IntPair o1, IntPair o2) {
      return Integer.compare(o1.a, o2.a);
    }
  }

  public static Comparator<Number2ObjectPair> getAscendingnumber2ObjectPairComparator() {
    return new AscendingNumber2ObjectPairComparator();
  }

  public static Comparator<Number2ObjectPair> getDescendingnumber2ObjectPairComparator() {
    return new DescendingNumber2ObjectPairComparator();
  }

  public static class Number2ObjectPair<T> {
    Number a;

    T b;

    public Number2ObjectPair(Number a, T b) {
      this.a = a;
      this.b = b;
    }

    public Number getA() {
      return a;
    }

    public T getB() {
      return b;
    }
  }

  public static class AscendingNumber2ObjectPairComparator
      implements Comparator<Number2ObjectPair> {
    @Override
    public int compare(Number2ObjectPair o1, Number2ObjectPair o2) {
      return new Double(o1.a.doubleValue()).compareTo(new Double(o2.a.doubleValue()));
    }
  }

  public static class DescendingNumber2ObjectPairComparator
      implements Comparator<Number2ObjectPair> {
    @Override
    public int compare(Number2ObjectPair o1, Number2ObjectPair o2) {
      return new Double(o2.a.doubleValue()).compareTo(new Double(o1.a.doubleValue()));
    }
  }

  /**
   * Utility class to store a primitive 'int' and 'double' pair.
   */
  public static class IntDoublePair {
    int _intValue;
    double _doubleValue;

    /**
     * Constructor for the class
     *
     * @param intVal 'int' value
     * @param doubleVal 'double' value
     */
    public IntDoublePair(int intVal, double doubleVal) {
      _intValue = intVal;
      _doubleValue = doubleVal;
    }

    /**
     * Sets the provided value into the 'int' field.
     * @param intVal Value to set
     */
    public void setIntValue(int intVal) {
      _intValue = intVal;
    }

    /**
     * Returns the int value of the pair
     * @return 'int' value
     */
    public int getIntValue() {
      return _intValue;
    }

    /**
     * Sets the provided value into the 'double' field.
     * @param doubleVal Value to set
     */
    public void setDoubleValue(double doubleVal) {
      _doubleValue = doubleVal;
    }

    /**
     * Returns the double value of the pair
     * @return 'double' value
     */
    public double getDoubleValue() {
      return _doubleValue;
    }
  }

  /**
   * Comparator class for comparing {@link IntDoublePair}.
   */
  public static class IntDoubleComparator implements Comparator<IntDoublePair> {
    private final boolean _descending;

    public IntDoubleComparator(boolean descending) {
      _descending = descending;
    }

    @Override
    public int compare(IntDoublePair o1, IntDoublePair o2) {
      double v1 = o1.getDoubleValue();
      double v2 = o2.getDoubleValue();

      if (v1 < v2) {
        return  (_descending) ? 1 : -1;
      } else if (v1 > v2) {
        return (_descending) ? -1 : 1;
      } else {
        return 0;
      }
    }
  }

  /**
   * Utility class to store a primitive 'int' and 'Object' pair.
   */
  public static class IntObjectPair<T extends Comparable> {
    int _intValue;
    T _objectValue;

    /**
     * Constructor for the class
     *
     * @param intVal 'int' value
     * @param objectVal 'Object' value
     */
    public IntObjectPair(int intVal, T objectVal) {
      _intValue = intVal;
      _objectValue = objectVal;
    }

    /**
     * Sets the provided value into the 'int' field.
     */
    public void setIntValue(int intValue) {
      _intValue = intValue;
    }

    /**
     * Returns the int value of the pair
     * @return 'int' value
     */
    public int getIntValue() {
      return _intValue;
    }

    /**
     * Sets the specified object value into the 'object' field.
     */
    public void setObjectValue(T objectValue) {
      _objectValue = objectValue;
    }

    /**
     * Returns the object value of the pair
     * @return 'Object' value
     */
    public T getObjectValue() {
      return _objectValue;
    }
  }

  /**
   * Comparator for {@link IntObjectComparator} class
   */
  public static class IntObjectComparator implements Comparator<IntObjectPair> {
    private final boolean _descending;

    public IntObjectComparator(boolean descending) {
      _descending = descending;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(IntObjectPair pair1, IntObjectPair pair2) {

      Comparable c1 = (Comparable) pair1.getObjectValue();
      Comparable c2 = (Comparable) pair2.getObjectValue();

      int cmpValue = c1.compareTo(c2);
      if (cmpValue == -1) {
        return (_descending) ? 1 : -1;
      } else if (cmpValue == 1) {
        return (_descending) ? -1 : 1;
      } else {
        return 0;
      }
    }
  }

}
