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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Utility class for special value transformation logic shared between
 * {@link org.apache.pinot.segment.local.recordtransformer.SpecialValueTransformer} and
 * {@link org.apache.pinot.segment.local.columntransformer.SpecialValueColumnTransformer}.
 *
 * <p>This transformer handles special values according to the following rules:
 * <ul>
 *   <li>For FLOAT and DOUBLE:
 *     <ul>
 *       <li>Negative zero (-0.0) should be converted to 0.0</li>
 *       <li>NaN should be converted to null</li>
 *     </ul>
 *   </li>
 *   <li>For BIG_DECIMAL:
 *     <ul>
 *       <li>Strip trailing zeros (unless allowTrailingZeros is set)</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>This transformation is required to ensure that the value is equal to itself, and the ordering
 * of the values is consistent with equals. This is required for certain data structures (e.g., sorted map)
 * and algorithms (e.g., binary search) to work correctly.
 */
public class SpecialValueTransformerUtils {

  private static final int NEGATIVE_ZERO_FLOAT_BITS = Float.floatToRawIntBits(-0.0f);
  private static final long NEGATIVE_ZERO_DOUBLE_BITS = Double.doubleToLongBits(-0.0d);

  private SpecialValueTransformerUtils() {
  }

  /**
   * Check if a column needs special value transformation based on its data type.
   *
   * @param fieldSpec The field specification for the column
   * @return true if the column needs transformation
   */
  public static boolean needsTransformation(FieldSpec fieldSpec) {
    DataType dataType = fieldSpec.getDataType();
    return dataType == DataType.FLOAT || dataType == DataType.DOUBLE
        || (dataType == DataType.BIG_DECIMAL && !fieldSpec.isAllowTrailingZeros());
  }

  /**
   * Transform a single value to handle special float/double/BigDecimal values.
   * <ul>
   *   <li>Float/Double NaN → null</li>
   *   <li>Float/Double -0.0 → 0.0</li>
   *   <li>BigDecimal → strip trailing zeros</li>
   * </ul>
   *
   * <p>Note: Only call this for columns that need transformation. For BigDecimal, only call
   * if the column does not allow trailing zeros.
   *
   * @param value The value to transform
   * @return Transformed value, or null for NaN values
   */
  @Nullable
  public static Object transformValue(@Nullable Object value) {
    if (value instanceof Float) {
      Float floatValue = (Float) value;
      if (floatValue.isNaN()) {
        return null;
      }
      if (Float.floatToRawIntBits(floatValue) == NEGATIVE_ZERO_FLOAT_BITS) {
        return 0.0f;
      }
    } else if (value instanceof Double) {
      Double doubleValue = (Double) value;
      if (doubleValue.isNaN()) {
        return null;
      }
      if (Double.doubleToRawLongBits(doubleValue) == NEGATIVE_ZERO_DOUBLE_BITS) {
        return 0.0d;
      }
    } else if (value instanceof BigDecimal) {
      BigDecimal bigDecimalValue = (BigDecimal) value;
      BigDecimal stripped = bigDecimalValue.stripTrailingZeros();
      if (!stripped.equals(bigDecimalValue)) {
        return stripped;
      }
    }
    return value;
  }

  /**
   * Transform an array of values (for multi-value columns).
   * Returns a new array if any values were transformed, otherwise returns the original array.
   * Null values from NaN transformation are filtered out.
   *
   * <p>Note: Only call this for columns that need transformation. For BigDecimal, only call
   * if the column does not allow trailing zeros.
   *
   * @param values The array of values to transform
   * @return Transformed array, or null if all values became null
   */
  @Nullable
  public static Object[] transformValues(@Nullable Object[] values) {
    if (values == null || values.length == 0) {
      return values;
    }

    List<Object> transformedValues = new ArrayList<>(values.length);
    boolean transformed = false;

    for (Object value : values) {
      Object transformedValue = transformValue(value);
      if (transformedValue != value) {
        transformed = true;
      }
      if (transformedValue != null) {
        transformedValues.add(transformedValue);
      }
    }

    if (!transformed) {
      return values;
    }

    return !transformedValues.isEmpty() ? transformedValues.toArray() : null;
  }
}
