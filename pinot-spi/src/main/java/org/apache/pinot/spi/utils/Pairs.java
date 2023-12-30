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

import java.util.Comparator;


public class Pairs {
  private Pairs() {
  }

  public static IntPair intPair(int a, int b) {
    return new IntPair(a, b);
  }

  public static Comparator<IntPair> intPairComparator() {
    return new AscendingIntPairComparator(true);
  }

  public static Comparator<IntPair> intPairComparator(boolean ascending) {
    return new AscendingIntPairComparator(ascending);
  }

  public static class IntPair {
    private int _left;
    private int _right;

    public IntPair(int left, int right) {
      _left = left;
      _right = right;
    }

    public int getLeft() {
      return _left;
    }

    public int getRight() {
      return _right;
    }

    public void setLeft(int left) {
      _left = left;
    }

    public void setRight(int right) {
      _right = right;
    }

    @Override
    public String toString() {
      return "[" + _left + "," + _right + "]";
    }

    @Override
    public int hashCode() {
      return 37 * _left + _right;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof IntPair) {
        IntPair that = (IntPair) obj;
        return _left == that._left && _right == that._right;
      }
      return false;
    }
  }

  public static class AscendingIntPairComparator implements Comparator<IntPair> {
    private boolean _ascending;

    public AscendingIntPairComparator(boolean ascending) {
      _ascending = ascending;
    }

    @Override
    public int compare(IntPair pair1, IntPair pair2) {
      if (pair1._left != pair2._left) {
        if (_ascending) {
          return Integer.compare(pair1._left, pair2._left);
        } else {
          return Integer.compare(pair2._left, pair1._left);
        }
      } else {
        if (_ascending) {
          return Integer.compare(pair1._right, pair2._right);
        } else {
          return Integer.compare(pair2._right, pair1._right);
        }
      }
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
        return (_descending) ? 1 : -1;
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
      if (cmpValue < 0) {
        return (_descending) ? 1 : -1;
      } else if (cmpValue > 0) {
        return (_descending) ? -1 : 1;
      } else {
        return 0;
      }
    }
  }
}
