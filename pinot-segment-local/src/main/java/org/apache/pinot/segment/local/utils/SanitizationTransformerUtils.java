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

    if (dataType == FieldSpec.DataType.STRING || dataType == FieldSpec.DataType.JSON || dataType == FieldSpec.DataType.BYTES) {
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
  }

  /**
   * Result of sanitization containing the sanitized value and whether it was modified.
   */
  public static class SanitizationResult {
    private final Object _value;
    private final boolean _sanitized;

    SanitizationResult(Object value, boolean sanitized) {
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
    if (value instanceof byte[]) {
      // Single-value BYTES column
      return SanitizationTransformerUtils.sanitizeBytesValue(columnInfo, (byte[]) value);
    } else if (value instanceof String) {
      // Single-valued String column
      return SanitizationTransformerUtils.sanitizeStringValue(columnInfo, (String) value);
    } else if (value instanceof Object[]) {
      // Multi-valued String / BYTES column
      Object[] values = (Object[]) value;
      boolean isSanitized = false;
      for (int i = 0; i < values.length; i++) {
        SanitizationResult result;
        if (values[i] instanceof byte[]) {
          result = SanitizationTransformerUtils.sanitizeBytesValue(columnInfo, (byte[]) values[i]);
        } else {
          result = SanitizationTransformerUtils.sanitizeStringValue(columnInfo, values[i].toString());
        }
        if (result != null && result.isSanitized()) {
          isSanitized = true;
          values[i] = result.getValue();
        }
      }
      return new SanitizationResult(values, isSanitized);
    }
    return null;
  }

  /**
   * Sanitize a string value based on column info configuration.
   *
   * @param columnInfo The sanitization configuration for the column
   * @param value The string value to sanitize
   * @return The sanitization result containing the sanitized value and whether it was modified
   * @throws IllegalStateException If strategy is ERROR and value exceeds max length or contains null character
   */
  @Nullable
  private static SanitizationResult sanitizeStringValue(SanitizedColumnInfo columnInfo, String value) {
    String columnName = columnInfo.getColumnName();
    int maxLength = columnInfo.getMaxLength();
    MaxLengthExceedStrategy strategy = columnInfo.getMaxLengthExceedStrategy();
    Object defaultNullValue = columnInfo.getDefaultNullValue();
    return sanitizeStringValue(columnName, value, maxLength, strategy, defaultNullValue);
  }

  /**
   * Sanitize a string value based on max length and strategy.
   *
   * @param columnName The column name (for error messages)
   * @param value The string value to sanitize
   * @param maxLength The maximum allowed length
   * @param strategy The strategy to use when max length is exceeded
   * @param defaultNullValue The default null value to use for SUBSTITUTE_DEFAULT_VALUE strategy
   * @return The sanitization result containing the sanitized value and whether it was modified
   * @throws IllegalStateException If strategy is ERROR and value exceeds max length or contains null character
   */
  private static SanitizationResult sanitizeStringValue(String columnName, String value, int maxLength,
      MaxLengthExceedStrategy strategy, Object defaultNullValue) {
    String sanitizedValue = StringUtil.sanitizeStringValue(value, maxLength);
    int index;

    // NOTE: reference comparison to check if the string was modified
    // noinspection StringEquality
    if (sanitizedValue != value) {
      switch (strategy) {
        case TRIM_LENGTH:
          return new SanitizationResult(sanitizedValue, true);
        case SUBSTITUTE_DEFAULT_VALUE:
          return new SanitizationResult(FieldSpec.getStringValue(defaultNullValue), true);
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
            return null;
          } else {
            return new SanitizationResult(sanitizedValue, true);
          }
        default:
          throw new IllegalStateException("Unsupported max length exceed strategy: " + strategy);
      }
    }
    return null;
  }

  /**
   * Sanitize a bytes value based on column info configuration.
   *
   * @param columnInfo The sanitization configuration for the column
   * @param value The bytes value to sanitize
   * @return The sanitization result containing the sanitized value and whether it was modified
   * @throws IllegalStateException If strategy is ERROR and value exceeds max length
   */
  @Nullable
  private static SanitizationResult sanitizeBytesValue(SanitizedColumnInfo columnInfo, byte[] value) {
    return sanitizeBytesValue(columnInfo.getColumnName(), value, columnInfo.getMaxLength(),
        columnInfo.getMaxLengthExceedStrategy(), columnInfo.getDefaultNullValue());
  }

  /**
   * Sanitize a bytes value based on max length and strategy.
   *
   * @param columnName The column name (for error messages)
   * @param value The bytes value to sanitize
   * @param maxLength The maximum allowed length
   * @param strategy The strategy to use when max length is exceeded
   * @param defaultNullValue The default null value to use for SUBSTITUTE_DEFAULT_VALUE strategy
   * @return The sanitization result containing the sanitized value and whether it was modified
   * @throws IllegalStateException If strategy is ERROR and value exceeds max length
   */
  private static SanitizationResult sanitizeBytesValue(String columnName, byte[] value, int maxLength,
      MaxLengthExceedStrategy strategy, Object defaultNullValue) {
    if (value.length > maxLength) {
      switch (strategy) {
        case TRIM_LENGTH:
          return new SanitizationResult(Arrays.copyOf(value, maxLength), true);
        case SUBSTITUTE_DEFAULT_VALUE:
          return new SanitizationResult(defaultNullValue, true);
        case ERROR:
          throw new IllegalStateException(
              String.format("Throwing exception as value for column %s exceeds configured max length %d.", columnName,
                  maxLength));
        case NO_ACTION:
          return null;
        default:
          throw new IllegalStateException("Unsupported max length exceed strategy: " + strategy);
      }
    }
    return null;
  }
}
