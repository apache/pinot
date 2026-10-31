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
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.ByteArray;


/// Expands the value of a multi-value (MV) group key into the scalar elements that form its groups.
///
/// The value is what an MV column holds in a multi-stage block: a primitive array (`int[]`, `long[]`, `float[]`,
/// `double[]`), a `String[]`, a `byte[][]`, any other `Object[]` (for example `BigDecimal[]`), or a [List] (scalar
/// functions return these). The rules follow the single-stage engine semantics of a group by on an MV column:
/// - A `null` value expands to one `null` element, so the row forms the `null` group.
/// - An empty array expands to no element, so the row contributes to no group.
/// - A repeated element stays repeated, so the row counts once per copy.
///
/// A `byte[]` element is wrapped in a [ByteArray], the representation of a `BYTES` key in the multi-stage engine,
/// so that equal byte sequences map to the same group.
///
/// The class is stateless and thread-safe.
final class MvKeyExpander {
  private static final Object[] NULL_ELEMENT = new Object[]{null};
  private static final Object[] NO_ELEMENT = new Object[0];

  private MvKeyExpander() {
  }

  /// Returns the elements of `value`. The caller must not modify the returned array.
  ///
  /// @throws IllegalArgumentException when `value` is not an array or a [List]
  static Object[] elements(@Nullable Object value) {
    if (value == null) {
      return NULL_ELEMENT;
    }
    if (value instanceof int[]) {
      int[] values = (int[]) value;
      int length = values.length;
      if (length == 0) {
        return NO_ELEMENT;
      }
      Object[] elements = new Object[length];
      for (int i = 0; i < length; i++) {
        elements[i] = values[i];
      }
      return elements;
    }
    if (value instanceof long[]) {
      long[] values = (long[]) value;
      int length = values.length;
      if (length == 0) {
        return NO_ELEMENT;
      }
      Object[] elements = new Object[length];
      for (int i = 0; i < length; i++) {
        elements[i] = values[i];
      }
      return elements;
    }
    if (value instanceof float[]) {
      float[] values = (float[]) value;
      int length = values.length;
      if (length == 0) {
        return NO_ELEMENT;
      }
      Object[] elements = new Object[length];
      for (int i = 0; i < length; i++) {
        elements[i] = values[i];
      }
      return elements;
    }
    if (value instanceof double[]) {
      double[] values = (double[]) value;
      int length = values.length;
      if (length == 0) {
        return NO_ELEMENT;
      }
      Object[] elements = new Object[length];
      for (int i = 0; i < length; i++) {
        elements[i] = values[i];
      }
      return elements;
    }
    if (value instanceof byte[][]) {
      byte[][] values = (byte[][]) value;
      int length = values.length;
      if (length == 0) {
        return NO_ELEMENT;
      }
      Object[] elements = new Object[length];
      for (int i = 0; i < length; i++) {
        elements[i] = toElement(values[i]);
      }
      return elements;
    }
    if (value instanceof Object[]) {
      Object[] values = (Object[]) value;
      int length = values.length;
      if (length == 0) {
        return NO_ELEMENT;
      }
      // String[] and other arrays of immutable values need no copy unless an element must be wrapped.
      for (Object element : values) {
        if (element instanceof byte[]) {
          Object[] elements = new Object[length];
          for (int i = 0; i < length; i++) {
            elements[i] = toElement(values[i]);
          }
          return elements;
        }
      }
      return values;
    }
    if (value instanceof List) {
      List<?> values = (List<?>) value;
      int length = values.size();
      if (length == 0) {
        return NO_ELEMENT;
      }
      Object[] elements = new Object[length];
      for (int i = 0; i < length; i++) {
        elements[i] = toElement(values.get(i));
      }
      return elements;
    }
    throw new IllegalArgumentException(
        "Unsupported value for a multi-value group key: " + value.getClass().getName());
  }

  @Nullable
  private static Object toElement(@Nullable Object element) {
    return element instanceof byte[] ? new ByteArray((byte[]) element) : element;
  }
}
