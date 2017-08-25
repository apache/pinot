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

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.EnumUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.EqualityUtils;


@JsonIgnoreProperties(ignoreUnknown = true)
public final class DateTimeFieldSpec extends FieldSpec {

  private static final String NUMBER_REGEX = "[0-9]+";
  private static final String COLON_SEPARATOR = ":";

  private String _format;
  private String _granularity;
  private DateTimeType _dateTimeType;

  public enum DateTimeType {
    PRIMARY,
    SECONDARY,
    DERIVED
  }

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
   * @param dataType
   * @param format - size:timeunit:timeformat eg: 1:MILLISECONDS:EPOCH, 5:MINUTES:EPOCH, 1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd
   * @param granularity - size:timeunit eg: 5:MINUTES
   */
  public DateTimeFieldSpec(@Nonnull String name, @Nonnull DataType dataType, @Nonnull String format,
      @Nonnull String granularity, Object defaultNullValue) {
    super(name, dataType, true, defaultNullValue);
    check(name, dataType, format, granularity);

    _format = format;
    _granularity = granularity;
  }


  public DateTimeFieldSpec(@Nonnull String name, @Nonnull DataType dataType, @Nonnull String format,
      @Nonnull String granularity, DateTimeType dateTimeType, Object defaultNullValue) {
    this(name, dataType, format, granularity, defaultNullValue);
    _dateTimeType = dateTimeType;
  }


  public DateTimeFieldSpec(@Nonnull String name, @Nonnull DataType dataType, @Nonnull String format,
      @Nonnull String granularity) {
    super(name, dataType, true);
    check(name, dataType, format, granularity);

    _format = format;
    _granularity = granularity;
  }


  public DateTimeFieldSpec(@Nonnull String name, @Nonnull DataType dataType, @Nonnull String format,
      @Nonnull String granularity, DateTimeType dateTimeType) {
    this(name, dataType, format, granularity);
    _dateTimeType = dateTimeType;
  }

  private void check(String name, DataType dataType, String format, String granularity) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(dataType);

    Preconditions.checkNotNull(format);
    String[] formatTokens = format.split(COLON_SEPARATOR);
    Preconditions.checkArgument(formatTokens.length == 3 || formatTokens.length == 4,
        "format must be of format size:timeunit:timeformat(:pattern)");
    Preconditions.checkArgument(formatTokens[0].matches(NUMBER_REGEX)
        && EnumUtils.isValidEnum(TimeUnit.class, formatTokens[1]), "format must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)");
    if (formatTokens.length == 3) {
      Preconditions.checkArgument(formatTokens[2].equals(TimeFormat.EPOCH.toString()),
          "format must be of format [0-9]+:<TimeUnit>:EPOCH or [0-9]+:<TimeUnit>:SIMPLE_DATE_FORMAT:<format>");
    } else {
      Preconditions.checkArgument(formatTokens[2].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()),
          "format must be of format [0-9]+:<TimeUnit>:EPOCH or [0-9]+:<TimeUnit>:SIMPLE_DATE_FORMAT:<format>");
    }

    Preconditions.checkNotNull(granularity);
    String[] granularityTokens = granularity.split(COLON_SEPARATOR);
    Preconditions.checkArgument(granularityTokens.length == 2, "granularity must be of format size:timeunit");
    Preconditions.checkArgument(granularityTokens[0].matches(NUMBER_REGEX)
        && EnumUtils.isValidEnum(TimeUnit.class, granularityTokens[1]), "granularity must be of format [0-9]+:<TimeUnit>");

  }


  @JsonIgnore
  @Nonnull
  @Override
  public FieldType getFieldType() {
    return FieldType.DATE_TIME;
  }

  @Nonnull
  public String getFormat() {
    return _format;
  }

  public void setFormat(String format) {
    _format = format;
  }

  @Nonnull
  public String getGranularity() {
    return _granularity;
  }

  public void setGranularity(String granularity) {
    _granularity = granularity;
  }

  @Nullable
  public DateTimeType getDateTimeType() {
    return _dateTimeType;
  }

  public void setDateTimeType(DateTimeType dateTimeType) {
    _dateTimeType = dateTimeType;
  }

  public static String constructFormat(int columnSize, TimeUnit columnUnit, String columnTimeFormat) {
    return Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit, columnTimeFormat);
  }

  public static String constructGranularity(int columnSize, TimeUnit columnUnit) {
    return Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit);
  }

  @Override
  public String toString() {
    return "< field type: DATE_TIME, field name: " + getName() + ", datatype: " + getDataType()
        + ", time column format: " + getFormat() + ", time field granularity: " + getGranularity()
        + ", date time type:" + getDateTimeType() + " >";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object instanceof DateTimeFieldSpec) {
      DateTimeFieldSpec that = (DateTimeFieldSpec) object;
      return getName().equals(that.getName())
          && getDataType().equals(that.getDataType())
          && getFormat().equals(that.getFormat())
          && getGranularity().equals(that.getGranularity())
          && getDateTimeType() == that.getDateTimeType()
          && getDefaultNullValue().equals(that.getDefaultNullValue());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = EqualityUtils.hashCodeOf(result, getDataType());
    result = EqualityUtils.hashCodeOf(result, getFormat());
    result = EqualityUtils.hashCodeOf(result, getGranularity());
    result = EqualityUtils.hashCodeOf(result, getDateTimeType());
    return result;
  }

}
