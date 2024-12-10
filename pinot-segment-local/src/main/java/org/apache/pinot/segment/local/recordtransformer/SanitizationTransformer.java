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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.StringUtil;


/**
 * The {@code SanitizationTransformer} class will sanitize the values to follow certain rules including:
 * <ul>
 *   <li>No {@code null} characters in string values</li>
 *   <li>String values are within the length limit</li>
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}.
 * This uses the MaxLengthExceedStrategy in the {@link FieldSpec} to decide what to do when the value exceeds the max.
 * For TRIM_LENGTH, the value is trimmed to the max length.
 * For SUBSTITUTE_DEFAULT_VALUE, the value is replaced with the default null value string.
 * For ERROR, an exception is thrown and the record is skipped.
 * For NO_ACTION, the value is kept as is if no NULL_CHARACTER present else trimmed till NULL.
 * In the first 2 scenarios, this metric REALTIME_ROWS_SANITIZED can be tracked to know if a trimmed /
 * default record was persisted.
 * In the third scenario, this metric ROWS_WITH_ERRORS can be tracked  to know if a record was skipped.
 * In the last scenario, this metric REALTIME_ROWS_SANITIZED can be tracked to know if a record was trimmed
 * due to having a null character.
 */
public class SanitizationTransformer implements RecordTransformer {
  private static final String NULL_CHARACTER = "\0";
  private final Map<String, SanitizedColumnInfo> _columnToColumnInfoMap = new HashMap<>();

  public SanitizationTransformer(Schema schema) {
    FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy;
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        if (fieldSpec.getDataType().equals(FieldSpec.DataType.STRING)) {
          maxLengthExceedStrategy =
              fieldSpec.getMaxLengthExceedStrategy() == null ? FieldSpec.MaxLengthExceedStrategy.TRIM_LENGTH
                  : fieldSpec.getMaxLengthExceedStrategy();
          _columnToColumnInfoMap.put(fieldSpec.getName(), new SanitizedColumnInfo(fieldSpec.getName(),
              fieldSpec.getMaxLength(), maxLengthExceedStrategy, fieldSpec.getDefaultNullValue()));
        } else if (fieldSpec.getDataType().equals(FieldSpec.DataType.JSON) || fieldSpec.getDataType()
            .equals(FieldSpec.DataType.BYTES)) {
          maxLengthExceedStrategy = fieldSpec.getMaxLengthExceedStrategy() == null
              ? FieldSpec.MaxLengthExceedStrategy.NO_ACTION : fieldSpec.getMaxLengthExceedStrategy();
          if (maxLengthExceedStrategy.equals(FieldSpec.MaxLengthExceedStrategy.NO_ACTION)) {
            continue;
          }
          _columnToColumnInfoMap.put(fieldSpec.getName(), new SanitizedColumnInfo(fieldSpec.getName(),
              fieldSpec.getMaxLength(), fieldSpec.getMaxLengthExceedStrategy(), fieldSpec.getDefaultNullValue()));
        }
      }
    }
  }

  @Override
  public boolean isNoOp() {
    return _columnToColumnInfoMap.isEmpty();
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, SanitizedColumnInfo> entry : _columnToColumnInfoMap.entrySet()) {
      String columnName = entry.getKey();
      Object value = record.getValue(columnName);
      Pair<?, Boolean> result;
      if (value instanceof byte[]) {
        // Single-values BYTES column
        result = sanitizeBytesValue(columnName, (byte[]) value, entry.getValue());
        record.putValue(columnName, result.getLeft());
        if (result.getRight()) {
          record.putValue(GenericRow.SANITIZED_RECORD_KEY, true);
        }
      } else if (value instanceof String) {
        // Single-valued String column
        result = sanitizeValue(columnName, (String) value, entry.getValue());
        record.putValue(columnName, result.getLeft());
        if (result.getRight()) {
          record.putValue(GenericRow.SANITIZED_RECORD_KEY, true);
        }
      } else {
        // Multi-valued String / BYTES column
        Object[] values = (Object[]) value;
        for (int i = 0; i < values.length; i++) {
          if (values[i] instanceof byte[]) {
            result = sanitizeBytesValue(columnName, (byte[]) values[i], entry.getValue());
          } else {
            result = sanitizeValue(columnName, values[i].toString(), entry.getValue());
          }
          values[i] = result.getLeft();
          if (result.getRight()) {
            record.putValue(GenericRow.SANITIZED_RECORD_KEY, true);
          }
        }
      }
    }
    return record;
  }

  /**
   * Sanitize the value for the given column.
   * @param columnName column name
   * @param value value of the column
   * @param sanitizedColumnInfo metadata from field spec of the column defined in schema
   * @return the sanitized value and a boolean indicating if the value was sanitized
   */
  private Pair<String, Boolean> sanitizeValue(String columnName, String value,
      SanitizedColumnInfo sanitizedColumnInfo) {
    String sanitizedValue = StringUtil.sanitizeStringValue(value, sanitizedColumnInfo.getMaxLength());
    FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy = sanitizedColumnInfo.getMaxLengthExceedStrategy();
    int index;
    // NOTE: reference comparison
    // noinspection StringEquality
    if (sanitizedValue != value) {
      switch (maxLengthExceedStrategy) {
        case TRIM_LENGTH:
          return Pair.of(sanitizedValue, true);
        case SUBSTITUTE_DEFAULT_VALUE:
          return Pair.of(FieldSpec.getStringValue(sanitizedColumnInfo.getDefaultNullValue()), true);
        case ERROR:
          index = value.indexOf(NULL_CHARACTER);
          if (index < 0) {
            throw new IllegalStateException(
                String.format("Throwing exception as value: %s for column %s exceeds configured max length %d.", value,
                    columnName, sanitizedColumnInfo.getMaxLength()));
          } else {
            throw new IllegalStateException(
                String.format("Throwing exception as value: %s for column %s contains null character.", value,
                    columnName));
          }
        case NO_ACTION:
          index = value.indexOf(NULL_CHARACTER);
          if (index < 0) {
            return Pair.of(value, false);
          } else {
            return Pair.of(sanitizedValue, true);
          }
        default:
          throw new IllegalStateException(
              "Unsupported max length exceed strategy: " + sanitizedColumnInfo.getMaxLengthExceedStrategy());
      }
    }
    return Pair.of(sanitizedValue, false);
  }

  /**
   * Sanitize the value for the given column.
   * @param columnName column name
   * @param value value of the column
   * @param sanitizedColumnInfo metadata from field spec of the column defined in schema
   * @return the sanitized value and a boolean indicating if the value was sanitized
   */
  private Pair<byte[], Boolean> sanitizeBytesValue(String columnName, byte[] value,
      SanitizedColumnInfo sanitizedColumnInfo) {
    if (value.length > sanitizedColumnInfo.getMaxLength()) {
      FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy = sanitizedColumnInfo.getMaxLengthExceedStrategy();
      switch (maxLengthExceedStrategy) {
        case TRIM_LENGTH:
          return Pair.of(Arrays.copyOf(value, sanitizedColumnInfo.getMaxLength()), true);
        case SUBSTITUTE_DEFAULT_VALUE:
          return Pair.of((byte[]) sanitizedColumnInfo.getDefaultNullValue(), true);
        case ERROR:
          throw new IllegalStateException(
              String.format("Throwing exception as value for column %s exceeds configured max length %d.", columnName,
                  sanitizedColumnInfo.getMaxLength()));
        case NO_ACTION:
          return Pair.of(value, false);
        default:
          throw new IllegalStateException(
              "Unsupported max length exceed strategy: " + sanitizedColumnInfo.getMaxLengthExceedStrategy());
      }
    }
    return Pair.of(value, false);
  }

  private static class SanitizedColumnInfo {
    private final String _columnName;
    private final int _maxLength;
    private final FieldSpec.MaxLengthExceedStrategy _maxLengthExceedStrategy;
    private final Object _defaultNullValue;

    private SanitizedColumnInfo(String columnName, int maxLength,
        FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy, Object defaultNullValue) {
      _columnName = columnName;
      _maxLength = maxLength;
      _maxLengthExceedStrategy = maxLengthExceedStrategy;
      _defaultNullValue = defaultNullValue;
    }

    public String getColumnName() {
      return _columnName;
    }

    public int getMaxLength() {
      return _maxLength;
    }

    public FieldSpec.MaxLengthExceedStrategy getMaxLengthExceedStrategy() {
      return _maxLengthExceedStrategy;
    }

    public Object getDefaultNullValue() {
      return _defaultNullValue;
    }
  }
}
