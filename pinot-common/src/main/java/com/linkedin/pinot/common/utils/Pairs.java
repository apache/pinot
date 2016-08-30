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
