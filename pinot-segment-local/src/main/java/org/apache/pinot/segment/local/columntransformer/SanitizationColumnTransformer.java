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
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils.SanitizationResult;
import org.apache.pinot.segment.local.utils.SanitizationTransformerUtils.SanitizedColumnInfo;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Column transformer that sanitizes STRING, JSON, and BYTES column values.
 *
 * <p>The sanitization rules include:
 * <ul>
 *   <li>No {@code null} characters in string values</li>
 *   <li>String values are within the length limit</li>
 * </ul>
 *
 * <p>Uses the MaxLengthExceedStrategy in the FieldSpec to decide what to do when the value exceeds the max:
 * <ul>
 *   <li>TRIM_LENGTH: The value is trimmed to the max length</li>
 *   <li>SUBSTITUTE_DEFAULT_VALUE: The value is replaced with the default null value string</li>
 *   <li>ERROR: An exception is thrown</li>
 *   <li>NO_ACTION: The value is kept as is if no NULL_CHARACTER present, else trimmed till NULL</li>
 * </ul>
 *
 * <p>NOTE: should put this after {@link DataTypeColumnTransformer} so that all values follow the data types in
 * FieldSpec, and after {@link NullValueColumnTransformer} so that before sanitization, all values are non-null
 * and follow the data types defined in the schema.
 */
public class SanitizationColumnTransformer implements ColumnTransformer {

  @Nullable
  private final SanitizedColumnInfo _columnInfo;
  /**
   * Create a SanitizationColumnTransformer.
   *
   * @param fieldSpec The field specification for the column
   */
  public SanitizationColumnTransformer(FieldSpec fieldSpec) {
    _columnInfo = SanitizationTransformerUtils.getSanitizedColumnInfo(fieldSpec);
  }

  @Override
  public boolean isNoOp() {
    return _columnInfo == null;
  }

  @Override
  public Object transform(@Nullable Object value) {
    SanitizationResult result = SanitizationTransformerUtils.sanitizeValue(_columnInfo, value);
    if (result != null) {
      return result.getValue();
    }
    return value;
  }
}
