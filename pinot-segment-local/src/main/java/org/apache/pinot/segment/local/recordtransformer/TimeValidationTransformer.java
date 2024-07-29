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

import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TimeValidationTransformer} class will validate the time column value.
 * <p>NOTE: should put this after the {@link DataTypeTransformer} so that all values follow the data types in
 * {@link FieldSpec}, and before the {@link NullValueTransformer} so that the invalidated value can be filled.
 */
public class TimeValidationTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeValidationTransformer.class);

  private final String _timeColumnName;
  private final DateTimeFormatSpec _timeFormatSpec;
  private final boolean _enableTimeValueCheck;
  private final boolean _continueOnError;

  public TimeValidationTransformer(TableConfig tableConfig, Schema schema) {
    _timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    if (_timeColumnName != null) {
      DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(_timeColumnName);
      Preconditions.checkState(dateTimeFieldSpec != null, "Failed to find spec for time column: %s from schema: %s",
          _timeColumnName, schema.getSchemaName());
      _timeFormatSpec = dateTimeFieldSpec.getFormatSpec();
      IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
      if (ingestionConfig != null) {
        _enableTimeValueCheck = ingestionConfig.isRowTimeValueCheck();
        _continueOnError = ingestionConfig.isContinueOnError();
      } else {
        _enableTimeValueCheck = false;
        _continueOnError = false;
      }
    } else {
      _timeFormatSpec = null;
      _enableTimeValueCheck = false;
      _continueOnError = false;
    }
  }

  @Override
  public boolean isNoOp() {
    return !_enableTimeValueCheck;
  }

  @Override
  public GenericRow transform(GenericRow record) {
    if (!_enableTimeValueCheck) {
      return record;
    }
    Object timeValue = record.getValue(_timeColumnName);
    if (timeValue == null) {
      return record;
    }
    long timeValueMs;
    try {
      timeValueMs = _timeFormatSpec.fromFormatToMillis(timeValue.toString());
    } catch (Exception e) {
      String errorMessage =
          String.format("Caught exception while parsing time value: %s with format: %s", timeValue, _timeFormatSpec);
      if (_continueOnError) {
        LOGGER.debug(errorMessage);
        record.putValue(_timeColumnName, null);
        return record;
      } else {
        throw new IllegalStateException(errorMessage);
      }
    }
    if (!TimeUtils.timeValueInValidRange(timeValueMs)) {
      String errorMessage =
          String.format("Time value: %s is not in valid range: %s", new DateTime(timeValueMs, DateTimeZone.UTC),
              TimeUtils.VALID_TIME_INTERVAL);
      if (_continueOnError) {
        LOGGER.debug(errorMessage);
        record.putValue(_timeColumnName, null);
        record.putValue(GenericRow.INCOMPLETE_RECORD_KEY, true);
        return record;
      } else {
        throw new IllegalStateException(errorMessage);
      }
    }
    return record;
  }
}
