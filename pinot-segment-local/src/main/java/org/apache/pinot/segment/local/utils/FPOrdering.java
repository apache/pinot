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
package org.apache.pinot.segment.local.utils;

public class FPOrdering {

  private FPOrdering() {
  }

  /**
   * Maps doubles to longs with the same total (unsigned) order
   * NEGATIVE_INFINITY is considered less than or equal to all values.
   * NaN and POSITIVE_INFINITY are considered greater than or equal to all values.
   *
   * @param value a double value
   * @return an ordinal
   */
  public static long ordinalOf(double value) {
    if (value == Double.NEGATIVE_INFINITY) {
      return 0;
    }
    if (value == Double.POSITIVE_INFINITY || Double.isNaN(value)) {
      return 0xFFFFFFFFFFFFFFFFL;
    }
    long bits = Double.doubleToLongBits(value);
    // need negatives to come before positives
    if ((bits & Long.MIN_VALUE) == Long.MIN_VALUE) {
      // conflate 0/-0, or reverse order of negatives
      bits = bits == Long.MIN_VALUE ? Long.MIN_VALUE : ~bits;
    } else { // positives after negatives
      bits ^= Long.MIN_VALUE;
    }
    return bits;
  }

  /**
   * Maps doubles to ints with the same total (unsigned) order
   * NEGATIVE_INFINITY is considered less than or equal to all values.
   * NaN and POSITIVE_INFINITY are considered greater than or equal to all values.
   *
   * @param value a double value
   * @return an ordinal
   */
  public static long ordinalOf(float value) {
    if (value == Double.NEGATIVE_INFINITY) {
      return 0;
    }
    if (value == Double.POSITIVE_INFINITY || Double.isNaN(value)) {
      return 0xFFFFFFFF;
    }
    int bits = Float.floatToIntBits(value);
    // need negatives to come before positives
    if ((bits & Integer.MIN_VALUE) == Integer.MIN_VALUE) {
      // conflate 0/-0, or reverse order of negatives
      bits = bits == Integer.MIN_VALUE ? Integer.MIN_VALUE : ~bits;
    } else { // positives after negatives
      bits ^= Integer.MIN_VALUE;
    }
    return bits & 0xFFFFFFFFL;
  }
}
