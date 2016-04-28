/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.PinotDataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.utils.time.TimeConverter;
import com.linkedin.pinot.common.utils.time.TimeConverterProvider;
import com.linkedin.pinot.core.data.GenericRow;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation will only inject columns inside the Schema.
 */
public class PlainFieldExtractor implements FieldExtractor {

  Schema _schema = null;

  private Map<String, Integer> _errorCount;

  private int _totalErrors = 0;
  private int _totalNulls = 0;
  private int _totalConversions = 0;
  private int _totalNullCols = 0;
  private static final Logger LOGGER = LoggerFactory.getLogger(PlainFieldExtractor.class);
  private Map<String, PinotDataType> _columnType;
  private Map<String, PinotDataType> _typeMap;
  private TimeConverter _timeConverter;
  private String _incomingTimeColumnName;
  private String _outGoingTimeColumnName;

  // Made public so it can be used in Pinot Admin code.
  public PlainFieldExtractor(Schema schema) {
    _schema = schema;
    initErrorCount();
    initColumnTypes();
    initTimeConverters();
  }

  private void initTimeConverters() {
    TimeFieldSpec timeFieldSpec = _schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
      _incomingTimeColumnName = timeFieldSpec.getIncomingTimeColumnName();
      _outGoingTimeColumnName = timeFieldSpec.getOutGoingTimeColumnName();
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();
      _timeConverter = TimeConverterProvider.getTimeConverter(incomingGranularitySpec,
          outgoingGranularitySpec);
    }
  }

  private void initErrorCount() {
    _errorCount = new HashMap<String, Integer>();
    for (String column : _schema.getColumnNames()) {
      _errorCount.put(column, 0);
    }
    _totalErrors = 0;
    _totalNulls = 0;
    _totalConversions = 0;
    _totalNullCols = 0;
  }

  private void initColumnTypes() {
    _columnType = new HashMap<String, PinotDataType>();
    for (String column : _schema.getColumnNames()) {
      FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
      PinotDataType dest = PinotDataType.OBJECT;
      if (fieldSpec != null) {
        // ChaosMonkey generates schemas with null fieldspecs
        dest = PinotDataType.getPinotDataType(fieldSpec);
      } else {
        LOGGER.warn("Bad schema: {}, Field: {}", _schema.getSchemaName(), column);
      }
      _columnType.put(column, dest);
    }
    _typeMap = new HashMap<String, PinotDataType>();
    for (PinotDataType p : PinotDataType.values()) {
      for (Method m : p.getClass().getMethods()) {
        if (m.getName() == "convert") {
          _typeMap.put(m.getReturnType().getCanonicalName(), p);
        }
      }
    }
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  public int getTotalConversions() {
    return _totalConversions;
  }

  public int getTotalNulls() {
    return _totalNulls;
  }

  public int getTotalNullCols() {
    return _totalNullCols;
  }

  @Override
  public GenericRow transform(GenericRow row) {
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    if (_schema.size() > 0) {
      boolean hasError = false;
      boolean hasNull = false;
      boolean hasConversion = false;
      for (String column : _schema.getColumnNames()) {
        // Adding outgoing time column to fieldMap;
        Object value;
        // _schema.getTimeColumnName() will give outgoing time column name.
        if (column.equals(_schema.getTimeColumnName())) {
          value = row.getValue(_incomingTimeColumnName);
          // For null time value, will let the rest code to handle
          if (value != null) {
            try {
              value = _timeConverter.convert(value);
            } catch (Exception e) {
              LOGGER.error("Got exception during converter incoming time value: " + value, e);
              value = null;
            }
          }
        } else {
          value = row.getValue(column);
        }
        FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
        PinotDataType dest = _columnType.get(column);
        PinotDataType source;
        if (value == null) {
          source = PinotDataType.OBJECT;
          hasNull = true;
          _totalNullCols++;
        } else {
          String typeName = (value.getClass().getCanonicalName());
          if ((typeName.equals("java.lang.Object[]")) && ((Object[]) value).length != 0) {
            typeName = ((Object[]) value)[0].getClass().getCanonicalName();
            typeName = typeName + "[]";
          }
          source = _typeMap.get(typeName);
          if (source == null) {
            source = PinotDataType.OBJECT;
          }
        }
        // PinotDataType source = PinotDataType.getPinotDataType(value);

        if ((source != dest) && (value != null)) {
          try {
            hasConversion = true;
            value = dest.convert(value, source);
            if (value == null) {
              hasError = true;
            }
          } catch (Exception e) {
            value = null;
            hasError = true;
          }
        }
        if (value == null) {
          // value was null
          // either because there was no field for column in the input row
          // or because there was an error in the conversion.
          // Count an error for column and row
          _errorCount.put(column, _errorCount.get(column) + 1);
          LOGGER.debug("Invalid value {} in column {} in schema {}", row.getValue(column), column,
              _schema.getSchemaName());
          try {
            if (fieldSpec.isSingleValueField()) {
              value = fieldSpec.getDefaultNullValue();
            } else {
              // A multi-value field was null.
              value = new Object[] {
                  fieldSpec.getDefaultNullValue()
              };
            }
          } catch (UnsupportedOperationException e) {
            // Already has value as null
          }
        }
        fieldMap.put(column, value);
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
      row.init(fieldMap);
    }
    return row;
  }

  public Map<String, Integer> getError_count() {
    return _errorCount;
  }

  public int getTotalErrors() {
    return _totalErrors;
  }
}
