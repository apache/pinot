/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
}
