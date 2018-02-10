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
package com.linkedin.pinot.common.data;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.linkedin.pinot.common.utils.EqualityUtils;
import javax.annotation.Nonnull;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class DateTimeFieldSpec extends FieldSpec {
  private String _format;
  private String _granularity;

  public enum TimeFormat {
    EPOCH,
    SIMPLE_DATE_FORMAT
  }

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public DateTimeFieldSpec() {
    super();
  }

  /**
   * @param name
   *
   * @param dataType
   *
   * @param format - defines how to interpret the numeric value in the date time column.
   * Format has to follow the pattern - size:timeunit:timeformat, where
   * size and timeUnit together define the granularity of the time column value.
   * Size is the integer value of the granularity size.
   * TimeFormat tells us whether the time column value is expressed in epoch or is a simple date format pattern
   * Consider 2 date time values for example 2017/07/01 00:00:00 and 2017/08/29 05:20:00:
   * e.g. 1) If the time column value is defined in millisSinceEpoch (1498892400000, 1504009200000)
   *          this configuration will be 1:MILLISECONDS:EPOCH
   *      2) If the time column value is defined in 5 minutes since epoch (4996308, 5013364)
   *         this configuration will be 5:MINUTES:EPOCH
   *      3) If the time column value is defined in a simple date format of a day (e.g. 20170701, 20170829),
   *         this configuration will be 1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd (the pattern can be configured as desired)
   *
   * @param granularity - defines in what granularity the data is bucketed.
   * Granularity has to follow pattern- size:timeunit, where
   * size and timeUnit together define the bucket granularity of the data.
   * This is independent of the format, which is purely defining how to interpret the numeric value in the date time column.
   * E.g.
   *       1) if a time column is defined in millisSinceEpoch (format=1:MILLISECONDS:EPOCH), but the data buckets are 5 minutes,
   *          the granularity will be 5:MINUTES.
   *       2) if a time column is defined in hoursSinceEpoch (format=1:HOURS:EPOCH), and the data buckets are 1 hours,
   *          the granularity will be 1:HOURS
   */
  public DateTimeFieldSpec(@Nonnull String name, @Nonnull DataType dataType, @Nonnull String format,
      @Nonnull String granularity) {
    super(name, dataType, true);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(dataType);
    Preconditions.checkArgument(DateTimeFormatSpec.isValidFormat(format));
    Preconditions.checkArgument(DateTimeGranularitySpec.isValidGranularity(granularity));

    _format = format;
    _granularity = granularity;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public FieldType getFieldType() {
    return FieldType.DATE_TIME;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setSingleValueField(boolean isSingleValueField) {
    Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for date time field.");
  }

  @Nonnull
  public String getFormat() {
    return _format;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setFormat(@Nonnull String format) {
    _format = format;
  }

  @Nonnull
  public String getGranularity() {
    return _granularity;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setGranularity(@Nonnull String granularity) {
    _granularity = granularity;
  }

  @Nonnull
  @Override
  public JsonObject toJsonObject() {
    JsonObject jsonObject = super.toJsonObject();
    jsonObject.addProperty("format", _format);
    jsonObject.addProperty("granularity", _granularity);
    return jsonObject;
  }

  @Override
  public String toString() {
    return "< field type: DATE_TIME, field name: " + _name + ", datatype: " + _dataType + ", time column format: "
        + _format + ", time field granularity: " + _granularity + " >";
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    DateTimeFieldSpec that = (DateTimeFieldSpec) o;
    return EqualityUtils.isEqual(_format, that._format) && EqualityUtils.isEqual(_granularity, that._granularity);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(super.hashCode(), _format);
    result = EqualityUtils.hashCodeOf(result, _granularity);
    return result;
  }
}
