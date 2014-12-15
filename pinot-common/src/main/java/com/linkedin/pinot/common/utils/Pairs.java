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

    public int getA() {
      return a;
    }

    public int getB() {
      return b;
    }
  }

  public static class AscendingIntPairComparator implements Comparator<IntPair> {

    @Override
    public int compare(IntPair o1, IntPair o2) {
      return new Integer(o1.a).compareTo(new Integer(o2.a));
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

  public static class AscendingNumber2ObjectPairComparator implements Comparator<Number2ObjectPair> {
    @Override
    public int compare(Number2ObjectPair o1, Number2ObjectPair o2) {
      return new Double(o1.a.doubleValue()).compareTo(new Double(o2.a.doubleValue()));
    }
  }

  public static class DescendingNumber2ObjectPairComparator implements Comparator<Number2ObjectPair> {
    @Override
    public int compare(Number2ObjectPair o1, Number2ObjectPair o2) {
      return new Double(o2.a.doubleValue()).compareTo(new Double(o1.a.doubleValue()));
    }
  }
}
