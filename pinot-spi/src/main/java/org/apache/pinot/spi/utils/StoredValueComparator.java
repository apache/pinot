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

import java.util.Arrays;
import java.util.Comparator;


/// Compares two non-null values of the same type in their stored form (e.g. `int` for `BOOLEAN`, [ByteArray] for
/// `BYTES`, `int[]` for `INT_ARRAY`), in the order of that type:
/// - A single value is ordered by its natural order.
/// - An array is ordered element by element, and a proper prefix comes first.
///
/// The ordering depends only on the type, not on what produced the value, so every operator that sorts on a column
/// agrees on it. The comparator is stateless and thread-safe.
public final class StoredValueComparator implements Comparator<Object> {
  public static final StoredValueComparator INSTANCE = new StoredValueComparator();

  private StoredValueComparator() {
  }

  @Override
  public int compare(Object value1, Object value2) {
    if (value1 instanceof Comparable) {
      //noinspection rawtypes,unchecked
      return ((Comparable) value1).compareTo(value2);
    }
    if (value1 instanceof int[]) {
      return Arrays.compare((int[]) value1, (int[]) value2);
    }
    if (value1 instanceof long[]) {
      return Arrays.compare((long[]) value1, (long[]) value2);
    }
    if (value1 instanceof float[]) {
      return Arrays.compare((float[]) value1, (float[]) value2);
    }
    if (value1 instanceof double[]) {
      return Arrays.compare((double[]) value1, (double[]) value2);
    }
    //noinspection rawtypes,unchecked
    return Arrays.compare((Comparable[]) value1, (Comparable[]) value2);
  }
}
