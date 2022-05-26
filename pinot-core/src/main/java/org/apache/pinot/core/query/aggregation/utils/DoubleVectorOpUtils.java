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
package org.apache.pinot.core.query.aggregation.utils;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;


public class DoubleVectorOpUtils {
  private DoubleVectorOpUtils() {
  }

  public static DoubleArrayList vectorAdd(DoubleArrayList a, DoubleArrayList b) {
    return vectorAdd(a, b.elements());
  }

  public static DoubleArrayList vectorAdd(DoubleArrayList a, double[] b) {
    double[] elements = a.elements();
    int length = elements.length;
    Preconditions.checkState(length == b.length, "The two operand arrays are not of the same size! provided %s, %s",
        length, b.length);
    for (int i = 0; i < length; i++) {
      elements[i] += b[i];
    }
    return a;
  }

  /**
   * return a new DoubleArrayList initialized with a
   * @param a array to initialize
   * @return a new DoubleArrayList initialized with a
   */
  public static DoubleArrayList createAndInitialize(double[] a) {
    return new DoubleArrayList(a);
  }

  /**
   * return a new DoubleArrayList with all zeros
   * @param len length of array
   * @return a new DoubleArrayList with all zeros
   */
  public static DoubleArrayList createAndInitialize(int len) {
    Preconditions.checkState(len > 0, "Asking for an array of length %s", len);
    return new DoubleArrayList(new double[len]);
  }

  public static DoubleArrayList incrementElement(DoubleArrayList a, int offset, double val) {
    Preconditions.checkState(a.size() > offset, "The offset %s exceeds the array size!", offset);
    double[] elements = a.elements();
    elements[offset] += val;
    return a;
  }

  public static DoubleArrayList incrementElementByOne(DoubleArrayList a, int offset) {
    return incrementElement(a, offset, 1d);
  }
}
