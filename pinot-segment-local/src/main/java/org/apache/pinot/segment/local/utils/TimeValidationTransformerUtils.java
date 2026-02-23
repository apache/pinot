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

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.ThrottledLogger;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for time value validation logic shared between
 * {@link org.apache.pinot.segment.local.recordtransformer.TimeValidationTransformer} and
 * {@link org.apache.pinot.segment.local.columntransformer.TimeValidationColumnTransformer}.
 */
public class TimeValidationTransformerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeValidationTransformerUtils.class);

  private TimeValidationTransformerUtils() {
  }

  /**
   * Configuration for time validation, holding all state needed to validate time values.
   * Similar to {@link SanitizationTransformerUtils.SanitizedColumnInfo}.
   */
  public static class TimeValidationConfig {
    private final DateTimeFormatSpec _timeFormatSpec;
    private final boolean _continueOnError;
    private final ThrottledLogger _throttledLogger;

    TimeValidationConfig(DateTimeFormatSpec timeFormatSpec, boolean continueOnError,
        ThrottledLogger throttledLogger) {
      _timeFormatSpec = timeFormatSpec;
      _continueOnError = continueOnError;
      _throttledLogger = throttledLogger;
    }

    public DateTimeFormatSpec getTimeFormatSpec() {
      return _timeFormatSpec;
    }

    public boolean isContinueOnError() {
      return _continueOnError;
    }

    public ThrottledLogger getThrottledLogger() {
      return _throttledLogger;
    }
  }

  /**
   * Create a TimeValidationConfig from table config and schema. Returns null when time validation
   * is disabled (acts as the isNoOp signal).
   *
   * @param tableConfig The table configuration
   * @param schema The schema
   * @return TimeValidationConfig if time validation is enabled, null otherwise
   */
  @Nullable
  public static TimeValidationConfig getConfig(TableConfig tableConfig, Schema schema) {
    String timeColumnName = tableConfig.getValidationConfig() != null
        ? tableConfig.getValidationConfig().getTimeColumnName() : null;
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    boolean enableTimeValueCheck = timeColumnName != null && ingestionConfig != null
        && ingestionConfig.isRowTimeValueCheck();

    if (!enableTimeValueCheck) {
      return null;
    }

    DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
    Preconditions.checkState(dateTimeFieldSpec != null, "Failed to find spec for time column: %s from schema: %s",
        timeColumnName, schema.getSchemaName());
    DateTimeFormatSpec timeFormatSpec = dateTimeFieldSpec.getFormatSpec();
    boolean continueOnError = ingestionConfig.isContinueOnError();
    ThrottledLogger throttledLogger = new ThrottledLogger(LOGGER, ingestionConfig);
    return new TimeValidationConfig(timeFormatSpec, continueOnError, throttledLogger);
  }

  /**
   * Validate that the time value is within the valid range (1971-2071).
   *
   * @param timeValue The time value to validate
   * @param timeFormatSpec The format spec to parse the time value
   * @return The original timeValue if valid, null if invalid or null input
   * @throws Exception if parsing fails
   */
  @Nullable
  public static Object validateTimeValue(@Nullable Object timeValue, DateTimeFormatSpec timeFormatSpec)
      throws Exception {
    if (timeValue == null) {
      return null;
    }
    long timeValueMs = timeFormatSpec.fromFormatToMillis(timeValue.toString());
    if (!TimeUtils.timeValueInValidRange(timeValueMs)) {
      return null;
    }
    return timeValue;
  }

  /**
   * Check if the time value is within the valid range (1971-2071).
   *
   * @param timeValue The time value to check
   * @param timeFormatSpec The format spec to parse the time value
   * @return true if the time value is valid, false otherwise
   * @throws Exception if parsing fails
   */
  public static boolean isTimeValueValid(@Nullable Object timeValue, DateTimeFormatSpec timeFormatSpec)
      throws Exception {
    if (timeValue == null) {
      return true; // null values are considered valid (handled by NullValueTransformer)
    }
    long timeValueMs = timeFormatSpec.fromFormatToMillis(timeValue.toString());
    return TimeUtils.timeValueInValidRange(timeValueMs);
  }

  /**
   * Transform (validate) a time value. Returns the value unchanged if valid, null if invalid.
   * Handles error logging/throwing based on the config's continueOnError setting.
   *
   * @param config The time validation configuration
   * @param value The time value to validate
   * @return The original value if valid, null if invalid or null input
   */
  @Nullable
  public static Object transformTimeValue(TimeValidationConfig config, @Nullable Object value) {
    if (value == null) {
      return null;
    }
    try {
      long timeValueMs = config.getTimeFormatSpec().fromFormatToMillis(value.toString());
      if (!TimeUtils.timeValueInValidRange(timeValueMs)) {
        String errorMessage =
            String.format("Time value: %s is not in valid range: %s", new DateTime(timeValueMs, DateTimeZone.UTC),
                TimeUtils.VALID_TIME_INTERVAL);
        if (!config.isContinueOnError()) {
          throw new IllegalStateException(errorMessage);
        }
        config.getThrottledLogger().warn(errorMessage, new IllegalStateException(errorMessage));
        return null;
      }
      return value;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      String errorMessage =
          String.format("Caught exception while parsing time value: %s with format: %s", value,
              config.getTimeFormatSpec());
      if (!config.isContinueOnError()) {
        throw new IllegalStateException(errorMessage, e);
      }
      config.getThrottledLogger().warn(errorMessage, e);
      return null;
    }
  }
}
