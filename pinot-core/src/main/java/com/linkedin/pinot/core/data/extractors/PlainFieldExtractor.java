/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.extractors;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.time.TimeConverter;
import com.linkedin.pinot.common.utils.time.TimeConverterProvider;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This implementation will only inject columns inside the Schema.
 */
public class PlainFieldExtractor implements FieldExtractor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PlainFieldExtractor.class);

  private static final Map<Class, PinotDataType> SINGLE_VALUE_TYPE_MAP = new HashMap<>();
  private static final Map<Class, PinotDataType> MULTI_VALUE_TYPE_MAP = new HashMap<>();

  static {
    SINGLE_VALUE_TYPE_MAP.put(Boolean.class, PinotDataType.BOOLEAN);
    SINGLE_VALUE_TYPE_MAP.put(Byte.class, PinotDataType.BYTE);
    SINGLE_VALUE_TYPE_MAP.put(Character.class, PinotDataType.CHARACTER);
    SINGLE_VALUE_TYPE_MAP.put(Short.class, PinotDataType.SHORT);
    SINGLE_VALUE_TYPE_MAP.put(Integer.class, PinotDataType.INTEGER);
    SINGLE_VALUE_TYPE_MAP.put(Long.class, PinotDataType.LONG);
    SINGLE_VALUE_TYPE_MAP.put(Float.class, PinotDataType.FLOAT);
    SINGLE_VALUE_TYPE_MAP.put(Double.class, PinotDataType.DOUBLE);
    SINGLE_VALUE_TYPE_MAP.put(String.class, PinotDataType.STRING);

    MULTI_VALUE_TYPE_MAP.put(Byte.class, PinotDataType.BYTE_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Character.class, PinotDataType.CHARACTER_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Short.class, PinotDataType.SHORT_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Integer.class, PinotDataType.INTEGER_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Long.class, PinotDataType.LONG_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Float.class, PinotDataType.FLOAT_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(Double.class, PinotDataType.DOUBLE_ARRAY);
    MULTI_VALUE_TYPE_MAP.put(String.class, PinotDataType.STRING_ARRAY);
  }

  private final Schema _schema;

  private final Map<String, Integer> _errorCount = new HashMap<>();
  private int _totalErrors = 0;
  private int _totalNulls = 0;
  private int _totalConversions = 0;
  private int _totalNullCols = 0;

  private final Map<String, PinotDataType> _columnType = new HashMap<>();

  private String _incomingTimeColumnName;
  private String _outgoingTimeColumnName;
  private TimeConverter _timeConverter;

  public PlainFieldExtractor(Schema schema) {
    _schema = schema;
    initErrorCount();
    initColumnTypes();
    initTimeConverters();
  }

  public void resetCounters() {
    _totalErrors = 0;
    _totalNulls = 0;
    _totalConversions = 0;
    _totalNullCols = 0;
  }

  private void initErrorCount() {
    for (String column : _schema.getColumnNames()) {
      _errorCount.put(column, 0);
    }
  }

  private void initColumnTypes() {
    // Get the map from column name to pinot data type.
    for (String column : _schema.getColumnNames()) {
      FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
      Preconditions.checkNotNull(fieldSpec, "Bad schema: " + _schema.getSchemaName() + ", field: " + column);
      _columnType.put(column, PinotDataType.getPinotDataType(fieldSpec));
    }
  }

  private void initTimeConverters() {
    TimeFieldSpec timeFieldSpec = _schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();
      _outgoingTimeColumnName = outgoingGranularitySpec.getName();
      if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
        _incomingTimeColumnName = incomingGranularitySpec.getName();
        _timeConverter = TimeConverterProvider.getTimeConverter(incomingGranularitySpec, outgoingGranularitySpec);
      }
    }
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public GenericRow transform(GenericRow row) {
    return transform(row, new GenericRow());
  }

  @Override
  public GenericRow transform(GenericRow row, GenericRow destinationRow) {
    boolean hasError = false;
    boolean hasNull = false;
    boolean hasConversion = false;

    for (String column : _schema.getColumnNames()) {
      FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
      // Ignore transform of DerivedMetric
      if (fieldSpec instanceof MetricFieldSpec && ((MetricFieldSpec) fieldSpec).isDerivedMetric()) {
        continue;
      }

      Object value;

      // Fetch value for this column.
      if (column.equals(_outgoingTimeColumnName) && _timeConverter != null) {
        // Convert incoming time to outgoing time.
        value = row.getValue(_incomingTimeColumnName);
        if (value == null) {
          hasNull = true;
          _totalNullCols++;
        } else {
          try {
            value = _timeConverter.convert(value);
          } catch (Exception e) {
            LOGGER.debug("Caught exception while converting incoming time value: {}", value, e);
            value = null;
            hasError = true;
            _errorCount.put(column, _errorCount.get(column) + 1);
          }
        }
      } else {
        value = row.getValue(column);
        if (value == null) {
          hasNull = true;
          _totalNullCols++;
        }
      }

      // Convert value if necessary.
      PinotDataType dest = _columnType.get(column);
      PinotDataType source = null;
      if (value != null) {
        if (value instanceof Object[]) {
          // Multi-value.
          Object[] valueArray = (Object[]) value;
          if (valueArray.length > 0) {
            source = MULTI_VALUE_TYPE_MAP.get(valueArray[0].getClass());
            if (source == null) {
              source = PinotDataType.OBJECT_ARRAY;
            }
          } else {
            LOGGER.debug("Got 0 length array.");
            // Use default value for 0 length array.
            value = null;
            hasError = true;
            _errorCount.put(column, _errorCount.get(column) + 1);
          }
        } else {
          // Single-value.
          source = SINGLE_VALUE_TYPE_MAP.get(value.getClass());
          if (source == null) {
            source = PinotDataType.OBJECT;
          }
        }

        if (value != null && source != dest) {
          Object before = value;
          try {
            value = dest.convert(before, source);
            hasConversion = true;
          } catch (Exception e) {
            LOGGER.debug("Caught exception while converting value: {} from: {} to: {}", before, source, dest);
            value = null;
            hasError = true;
            _errorCount.put(column, _errorCount.get(column) + 1);
          }
        }

        // Null character is used as the padding character, so we do not allow null characters in strings.
        if (dest == PinotDataType.STRING && value != null) {
          if (StringUtil.containsNullCharacter(value.toString())) {
            LOGGER.error("Input value: {} for column: {} contains null character", value, column);
            value = StringUtil.removeNullCharacters(value.toString());
          }
        }
      }

      // Assign default value for null value.
      if (value == null) {
        if (fieldSpec.isSingleValueField()) {
          // Single-value field.
          value = fieldSpec.getDefaultNullValue();
        } else {
          // Multi-value field.
          value = new Object[]{fieldSpec.getDefaultNullValue()};
        }
      }

      destinationRow.putField(column, value);
    }

    if (hasError) {
      _totalErrors++;
    }
    if (hasNull) {
      _totalNulls++;
    }
    if (hasConversion) {
      _totalConversions++;
    }

    return destinationRow;
  }

  public Map<String, Integer> getErrorCount() {
    return _errorCount;
  }

  public int getTotalErrors() {
    return _totalErrors;
  }

  public int getTotalNulls() {
    return _totalNulls;
  }

  public int getTotalConversions() {
    return _totalConversions;
  }

  public int getTotalNullCols() {
    return _totalNullCols;
  }
}
