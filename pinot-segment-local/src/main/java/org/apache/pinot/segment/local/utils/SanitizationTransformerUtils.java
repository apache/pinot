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

import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.utils.StringUtil;


/**
 * Utility class for sanitization transformation logic shared between
 * {@link org.apache.pinot.segment.local.recordtransformer.SanitizationTransformer} and
 * {@link org.apache.pinot.segment.local.columntransformer.SanitizationColumnTransformer}.
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
 *   <li>ERROR: An exception is thrown and the record is skipped</li>
 *   <li>NO_ACTION: The value is kept as is if no NULL_CHARACTER present, else trimmed till NULL</li>
 * </ul>
 */
public class SanitizationTransformerUtils {

  private static final String NULL_CHARACTER = "\0";

  private SanitizationTransformerUtils() {
  }

  @Nullable
  public static SanitizedColumnInfo getSanitizedColumnInfo(FieldSpec fieldSpec) {
    FieldSpec.DataType dataType = fieldSpec.getDataType();

    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.JSON
        || dataType == FieldSpec.DataType.BYTES) {
      MaxLengthExceedStrategy strategy = fieldSpec.getEffectiveMaxLengthExceedStrategy();
      // For STRING, always apply (to handle null characters even with NO_ACTION)
      // For JSON/BYTES, only apply if strategy is not NO_ACTION
      if (dataType == FieldSpec.DataType.STRING || strategy != MaxLengthExceedStrategy.NO_ACTION) {
        return new SanitizedColumnInfo(fieldSpec.getName(), fieldSpec.getEffectiveMaxLength(),
            strategy, fieldSpec.getDefaultNullValue());
      }
    }
    return null;
  }

  /**
   * Configuration for a column that needs sanitization.
   */
  public static class SanitizedColumnInfo {
    private final String _columnName;
    private final int _maxLength;
    private final MaxLengthExceedStrategy _maxLengthExceedStrategy;
    private final Object _defaultNullValue;
    private final SanitizationResult _result = new SanitizationResult();

    SanitizedColumnInfo(String columnName, int maxLength, MaxLengthExceedStrategy maxLengthExceedStrategy,
        Object defaultNullValue) {
      _columnName = columnName;
      _maxLength = maxLength;
      _maxLengthExceedStrategy = maxLengthExceedStrategy;
      _defaultNullValue = defaultNullValue;
    }

    String getColumnName() {
      return _columnName;
    }

    int getMaxLength() {
      return _maxLength;
    }

    MaxLengthExceedStrategy getMaxLengthExceedStrategy() {
      return _maxLengthExceedStrategy;
    }

    Object getDefaultNullValue() {
      return _defaultNullValue;
    }

    SanitizationResult getResult() {
      return _result;
    }
  }

  /**
   * Mutable result of sanitization containing the sanitized value and whether it was modified.
   * Reused across calls to avoid repeated object allocation.
   */
  public static class SanitizationResult {
    private Object _value;
    private boolean _sanitized;

    SanitizationResult() {
    }

    void set(Object value, boolean sanitized) {
      _value = value;
      _sanitized = sanitized;
    }

    public Object getValue() {
      return _value;
    }

    public boolean isSanitized() {
      return _sanitized;
    }
  }

  @Nullable
  public static SanitizationResult sanitizeValue(SanitizedColumnInfo columnInfo, Object value) {
    if (value == null) {
      return null;
    }
    SanitizationResult result = columnInfo.getResult();
    if (value instanceof byte[]) {
      // Single-value BYTES column
      sanitizeBytesValue(columnInfo, (byte[]) value, result);
      return result;
    } else if (value instanceof String) {
      // Single-valued String column
      sanitizeStringValue(columnInfo, (String) value, result);
      return result;
    } else if (value instanceof Object[]) {
      // Multi-valued String / BYTES column
      Object[] values = (Object[]) value;
      boolean isSanitized = false;
      for (int i = 0; i < values.length; i++) {
        if (values[i] instanceof byte[]) {
          sanitizeBytesValue(columnInfo, (byte[]) values[i], result);
        } else {
          sanitizeStringValue(columnInfo, values[i].toString(), result);
        }
        isSanitized |= result.isSanitized();
        values[i] = result.getValue();
      }
      result.set(values, isSanitized);
      return result;
    }
    return null;
  }

  /**
   * Sanitize a string value based on column info configuration.
   * Updates the provided result in place.
   */
  private static void sanitizeStringValue(SanitizedColumnInfo columnInfo, String value, SanitizationResult result) {
    sanitizeStringValue(columnInfo.getColumnName(), value, columnInfo.getMaxLength(),
        columnInfo.getMaxLengthExceedStrategy(), columnInfo.getDefaultNullValue(), result);
  }

  /**
   * Sanitize a string value based on max length and strategy.
   * Updates the provided result in place.
   *
   * @throws IllegalStateException If strategy is ERROR and value exceeds max length or contains null character
   */
  private static void sanitizeStringValue(String columnName, String value, int maxLength,
      MaxLengthExceedStrategy strategy, Object defaultNullValue, SanitizationResult result) {
    String sanitizedValue = StringUtil.sanitizeStringValue(value, maxLength);
    int index;

    // NOTE: reference comparison to check if the string was modified
    // noinspection StringEquality
    if (sanitizedValue != value) {
      switch (strategy) {
        case TRIM_LENGTH:
          result.set(sanitizedValue, true);
          return;
        case SUBSTITUTE_DEFAULT_VALUE:
          result.set(FieldSpec.getStringValue(defaultNullValue), true);
          return;
        case ERROR:
          index = value.indexOf(NULL_CHARACTER);
          if (index < 0) {
            throw new IllegalStateException(
                String.format("Throwing exception as value: %s for column %s exceeds configured max length %d.", value,
                    columnName, maxLength));
          } else {
            throw new IllegalStateException(
                String.format("Throwing exception as value: %s for column %s contains null character.", value,
                    columnName));
          }
        case NO_ACTION:
          index = value.indexOf(NULL_CHARACTER);
          if (index < 0) {
            result.set(value, false);
          } else {
            result.set(sanitizedValue, true);
          }
          return;
        default:
          throw new IllegalStateException("Unsupported max length exceed strategy: " + strategy);
      }
    }
    result.set(value, false);
  }

  /**
   * Sanitize a bytes value based on column info configuration.
   * Updates the provided result in place.
   */
  private static void sanitizeBytesValue(SanitizedColumnInfo columnInfo, byte[] value, SanitizationResult result) {
    sanitizeBytesValue(columnInfo.getColumnName(), value, columnInfo.getMaxLength(),
        columnInfo.getMaxLengthExceedStrategy(), columnInfo.getDefaultNullValue(), result);
  }

  /**
   * Sanitize a bytes value based on max length and strategy.
   * Updates the provided result in place.
   *
   * @throws IllegalStateException If strategy is ERROR and value exceeds max length
   */
  private static void sanitizeBytesValue(String columnName, byte[] value, int maxLength,
      MaxLengthExceedStrategy strategy, Object defaultNullValue, SanitizationResult result) {
    if (value.length > maxLength) {
      switch (strategy) {
        case TRIM_LENGTH:
          result.set(Arrays.copyOf(value, maxLength), true);
          return;
        case SUBSTITUTE_DEFAULT_VALUE:
          result.set(defaultNullValue, true);
          return;
        case ERROR:
          throw new IllegalStateException(
              String.format("Throwing exception as value for column %s exceeds configured max length %d.", columnName,
                  maxLength));
        case NO_ACTION:
          result.set(value, false);
          return;
        default:
          throw new IllegalStateException("Unsupported max length exceed strategy: " + strategy);
      }
    }
    result.set(value, false);
  }
}
