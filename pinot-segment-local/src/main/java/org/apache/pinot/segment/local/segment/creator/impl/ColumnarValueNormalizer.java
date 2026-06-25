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
package org.apache.pinot.segment.local.segment.creator.impl;

import org.apache.pinot.segment.local.utils.DataTypeTransformerUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.PinotDataType;


/**
 * Normalizes a single column value read from a {@code ColumnReader} into the canonical form the
 * per-column stats collectors and index creators expect, for the column-major
 * ({@code buildColumnar}) path.
 *
 * <p>The row-major build runs each record through a {@code TransformPipeline} whose
 * {@code NullValueTransformer} substitutes {@link FieldSpec#getDefaultNullValue()} for {@code null}
 * and whose {@code DataTypeTransformer} coerces every value to the column's stored type (e.g.
 * {@code Boolean} → {@code Integer} for a {@code BOOLEAN} column stored as {@code INT},
 * {@code Timestamp} → {@code Long} for {@code TIMESTAMP}). The column-major driver deliberately runs
 * with no transform pipeline, so a non-segment source (e.g. Arrow) delivers values in the source's
 * logical type with raw {@code null}s — which the typed collectors / index creators do not accept.
 *
 * <p>This helper applies the equivalent of those two transformers to one value, in the same order:
 * <ol>
 *   <li>{@code NullValueTransformer}: a {@code null} value becomes the column default — the scalar
 *       default for single-value columns, or a one-element {@code Object[]} of that scalar for
 *       multi-value columns (matching {@code NullValueTransformerUtils.getDefaultNullValue}).</li>
 *   <li>{@code DataTypeTransformer}: {@link DataTypeTransformerUtils#transformValue} standardizes the
 *       value (collapsing single-element collections, dropping {@code null} elements from multi-value
 *       arrays) and converts it to {@code destDataType} via {@link PinotDataType}.</li>
 * </ol>
 *
 * <p>A whole-value {@code null} (or a value that standardizes to {@code null}, e.g. an empty array)
 * therefore resolves to the column default rather than reaching the collector as {@code null}. The
 * one intentional divergence from the row-major path is the configured time column: this helper uses
 * the raw {@link FieldSpec#getDefaultNullValue()} rather than the row-major time-range-validated /
 * current-time override; the stats and index paths stay mutually consistent (segment metadata agrees
 * with the forward index).
 */
public final class ColumnarValueNormalizer {

  private ColumnarValueNormalizer() {
  }

  /**
   * @param column the column name (used only for error messages)
   * @param fieldSpec the column's field spec (supplies the default null value and SV/MV-ness)
   * @param destDataType the column's destination {@link PinotDataType}, i.e.
   *        {@code PinotDataType.getPinotDataTypeForIngestion(fieldSpec)} — pass it in pre-computed so
   *        it is resolved once per column rather than once per value
   * @param value the raw value read from the {@code ColumnReader} (may be {@code null})
   * @return the normalized, never-{@code null} value to feed to the stats collector / index creator
   */
  public static Object normalize(String column, FieldSpec fieldSpec, PinotDataType destDataType, Object value) {
    if (value == null) {
      value = defaultNullValue(fieldSpec);
    }
    value = DataTypeTransformerUtils.transformValue(column, value, destDataType);
    if (value == null) {
      // The value standardized to null (e.g. an empty multi-value array). Substitute the default so a
      // null never reaches the typed collectors / index creators.
      value = defaultNullValue(fieldSpec);
    }
    return value;
  }

  private static Object defaultNullValue(FieldSpec fieldSpec) {
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    return fieldSpec.isSingleValueField() ? defaultNullValue : new Object[]{defaultNullValue};
  }
}
