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
package org.apache.pinot.spi.data.readers;

import java.util.BitSet;
import javax.annotation.Nullable;


/**
 * Result wrapper for multi-value column reads that tracks element-level nulls.
 *
 * <p>This class addresses a limitation where bulk reads from columnar formats (like Arrow)
 * don't check the validity bitmap for null elements. By returning both the values array
 * and a nulls BitSet, callers can properly handle null elements within multi-value columns.
 *
 * <p><b>BitSet Semantics:</b>
 * <ul>
 *   <li>Set bit (1) = null element</li>
 *   <li>Unset bit (0) = valid/non-null element</li>
 *   <li>Null nulls BitSet = no nulls in the range (fast path)</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * MultiValueResult<int[]> result = columnReader.nextIntMV();
 * int[] values = result.getValues();
 *
 * if (result.hasNulls()) {
 *   for (int i = 0; i < values.length; i++) {
 *     if (result.isNull(i)) {
 *       // Handle null element - values[i] contains default value (0 for int)
 *     } else {
 *       // Use values[i]
 *     }
 *   }
 * } else {
 *   // Fast path: no nulls, use values array directly
 * }
 * }</pre>
 *
 * @param <T> The array type (int[], long[], float[], double[])
 */
public class MultiValueResult<T> {
  private final T _values;
  @Nullable
  private final BitSet _nulls;

  private MultiValueResult(T values, @Nullable BitSet nulls) {
    _values = values;
    _nulls = nulls;
  }

  /**
   * Create a MultiValueResult with optional nulls information.
   *
   * @param values The values array (int[], long[], float[], double[])
   * @param nulls BitSet where set bits indicate null elements,
   *              or null if no nulls exist in the range
   * @param <T> The array type
   * @return A new MultiValueResult instance
   */
  public static <T> MultiValueResult<T> of(T values, @Nullable BitSet nulls) {
    return new MultiValueResult<>(values, nulls);
  }

  /**
   * Check if any elements in this result are null.
   *
   * @return true if there are null elements, false otherwise
   */
  public boolean hasNulls() {
    return _nulls != null;
  }

  /**
   * Check if a specific element is null.
   *
   * @param index The index of the element to check
   * @return true if the element is null, false if it's valid
   */
  public boolean isNull(int index) {
    return _nulls != null && _nulls.get(index);
  }

  /**
   * Get the values array.
   *
   * <p>Note: If {@link #hasNulls()} returns true, some elements in this array
   * may contain default values (0 for numeric types) for null positions.
   * Use {@link #isNull(int)} to check individual elements.
   *
   * @return The values array
   */
  public T getValues() {
    return _values;
  }

  /**
   * Get the nulls BitSet.
   *
   * @return The nulls BitSet where set bits indicate null elements, or null if no nulls exist
   */
  @Nullable
  public BitSet getNulls() {
    return _nulls;
  }
}
