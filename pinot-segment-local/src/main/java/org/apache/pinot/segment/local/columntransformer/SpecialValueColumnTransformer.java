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
package org.apache.pinot.segment.local.columntransformer;

import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.SpecialValueTransformerUtils;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Column transformer that handles special values for FLOAT, DOUBLE, and BIG_DECIMAL columns.
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
 *
 * <p>NOTE: should put this after {@link DataTypeColumnTransformer} so that we already have the values
 * complying with the schema before handling special values, and before {@link NullValueColumnTransformer}
 * so that it transforms all the null values properly.
 */
public class SpecialValueColumnTransformer implements ColumnTransformer {

  private final boolean _isNoOp;

  /**
   * Create a SpecialValueColumnTransformer.
   *
   * @param fieldSpec The field specification for the column
   */
  public SpecialValueColumnTransformer(FieldSpec fieldSpec) {
    _isNoOp = !SpecialValueTransformerUtils.needsTransformation(fieldSpec);
  }

  @Override
  public boolean isNoOp() {
    return _isNoOp;
  }

  @Override
  public Object transform(@Nullable Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Object[]) {
      // Multi-valued column
      return SpecialValueTransformerUtils.transformValues((Object[]) value);
    } else {
      // Single-valued column
      return SpecialValueTransformerUtils.transformValue(value);
    }
  }
}
