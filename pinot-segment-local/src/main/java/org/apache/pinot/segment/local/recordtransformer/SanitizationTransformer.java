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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.StringUtil;


/**
 * The {@code SanitizationTransformer} class will sanitize the values to follow certain rules including:
 * <ul>
 *   <li>No {@code null} characters in string values</li>
 *   <li>String values are within the length limit</li>
 *   TODO: add length limit to BYTES values if necessary
 * </ul>
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}.
 * This uses the MaxLengthExceedStrategy in the {@link FieldSpec} to decide what to do when the value exceeds the max.
 * For TRIM_LENGTH, the value is trimmed to the max length.
 * For SUBSTITUTE_DEFAULT_VALUE, the value is replaced with the default null value string.
 * For FAIL_INGESTION, an exception is thrown and the record is skipped.
 * For NO_ACTION, the value is kept as is if no NULL_CHARACTER present else trimmed till NULL.
 * In the first 2 scenarios, this metric INCOMPLETE_REALTIME_ROWS_CONSUMED can be tracked to know if a trimmed /
 * default record was persisted.
 * In the last scenario, this metric ROWS_WITH_ERRORS can be tracked  to know if a record was skipped.
 */
public class SanitizationTransformer implements RecordTransformer {
  private static final String NULL_CHARACTER = "\0";
  private final Map<String, FieldSpec> _stringColumnToFieldSpecMap = new HashMap<>();

  public SanitizationTransformer(Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && fieldSpec.getDataType() == DataType.STRING) {
        _stringColumnToFieldSpecMap.put(fieldSpec.getName(), fieldSpec);
      }
    }
  }

  @Override
  public boolean isNoOp() {
    return _stringColumnToFieldSpecMap.isEmpty();
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, FieldSpec> entry : _stringColumnToFieldSpecMap.entrySet()) {
      String stringColumn = entry.getKey();
      Object value = record.getValue(stringColumn);
      if (value instanceof String) {
        // Single-valued column
        Pair<String, Boolean> result = sanitizeValue(stringColumn, (String) value, entry.getValue());
        record.putValue(stringColumn, result.getLeft());
        if (result.getRight()) {
          record.putValue(GenericRow.INCOMPLETE_RECORD_KEY, true);
        }
      } else {
        // Multi-valued column
        Object[] values = (Object[]) value;
        for (int i = 0; i < values.length; i++) {
          Pair<String, Boolean> result = sanitizeValue(stringColumn, values[i].toString(), entry.getValue());
          values[i] = result.getLeft();
          if (result.getRight()) {
            record.putValue(GenericRow.INCOMPLETE_RECORD_KEY, true);
          }
        }
      }
    }
    return record;
  }

  /**
   * Sanitize the value for the given column. Usually a STRING column can be extended for JSON / BYTES in future.
   * @param stringColumn column name
   * @param value value of the column
   * @param columnFieldSpec field spec of the column defined in schema
   * @return the sanitized value and a boolean indicating if the value was sanitized
   */
  private Pair<String, Boolean> sanitizeValue(String stringColumn, String value, FieldSpec columnFieldSpec) {
    String sanitizedValue = StringUtil.sanitizeStringValue(value, columnFieldSpec.getMaxLength());
    FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy;
    if (columnFieldSpec.getMaxLengthExceedStrategy() == null) {
      maxLengthExceedStrategy = columnFieldSpec.getDataType() == DataType.STRING
          ? FieldSpec.MaxLengthExceedStrategy.TRIM_LENGTH : FieldSpec.MaxLengthExceedStrategy.NO_ACTION;
    } else {
      maxLengthExceedStrategy = columnFieldSpec.getMaxLengthExceedStrategy();
    }
    // NOTE: reference comparison
    // noinspection StringEquality
    if (sanitizedValue != value) {
      switch (maxLengthExceedStrategy) {
        case TRIM_LENGTH:
          return Pair.of(sanitizedValue, true);
        case SUBSTITUTE_DEFAULT_VALUE:
          return Pair.of(columnFieldSpec.getDefaultNullValueString(), true);
        case FAIL_INGESTION:
          int index = value.indexOf(NULL_CHARACTER);
          if (index < 0) {
            throw new IllegalStateException(
                String.format("Throwing exception as value: %s for column %s exceeds configured max length %d.", value,
                    stringColumn, columnFieldSpec.getMaxLength()));
          } else {
            throw new IllegalStateException(
                String.format("Throwing exception as value: %s for column %s contains null character.", value,
                    stringColumn));
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
              "Unsupported max length exceed strategy: " + columnFieldSpec.getMaxLengthExceedStrategy());
      }
    }
    return Pair.of(sanitizedValue, false);
  }
}
