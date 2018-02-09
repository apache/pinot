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
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TimeFieldSpec extends FieldSpec {
  private TimeGranularitySpec _incomingGranularitySpec;
  private TimeGranularitySpec _outgoingGranularitySpec;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public TimeFieldSpec() {
    super();
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType,
      @Nonnull TimeUnit incomingTimeUnit) {
    super(incomingName, incomingDataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType,
      @Nonnull TimeUnit incomingTimeUnit, @Nonnull Object defaultNullValue) {
    super(incomingName, incomingDataType, true, defaultNullValue);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType,
      @Nonnull TimeUnit incomingTimeUnit, @Nonnull String outgoingName, @Nonnull DataType outgoingDataType,
      @Nonnull TimeUnit outgoingTimeUnit) {
    super(outgoingName, outgoingDataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec = new TimeGranularitySpec(outgoingDataType, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType,
      @Nonnull TimeUnit incomingTimeUnit, @Nonnull String outgoingName, @Nonnull DataType outgoingDataType,
      @Nonnull TimeUnit outgoingTimeUnit, @Nonnull Object defaultNullValue) {
    super(outgoingName, outgoingDataType, true, defaultNullValue);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec = new TimeGranularitySpec(outgoingDataType, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType, int incomingTimeUnitSize,
      @Nonnull TimeUnit incomingTimeUnit) {
    super(incomingName, incomingDataType, true);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType, int incomingTimeUnitSize,
      @Nonnull TimeUnit incomingTimeUnit, @Nonnull Object defaultNullValue) {
    super(incomingName, incomingDataType, true, defaultNullValue);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType, int incomingTimeUnitSize,
      @Nonnull TimeUnit incomingTimeUnit, @Nonnull String outgoingName, @Nonnull DataType outgoingDataType,
      int outgoingTimeUnitSize, @Nonnull TimeUnit outgoingTimeUnit) {
    super(outgoingName, outgoingDataType, true);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec =
        new TimeGranularitySpec(outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(@Nonnull String incomingName, @Nonnull DataType incomingDataType, int incomingTimeUnitSize,
      @Nonnull TimeUnit incomingTimeUnit, @Nonnull String outgoingName, @Nonnull DataType outgoingDataType,
      int outgoingTimeUnitSize, @Nonnull TimeUnit outgoingTimeUnit, @Nonnull Object defaultNullValue) {
    super(outgoingName, outgoingDataType, true, defaultNullValue);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec =
        new TimeGranularitySpec(outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(@Nonnull TimeGranularitySpec incomingGranularitySpec) {
    super(incomingGranularitySpec.getName(), incomingGranularitySpec.getDataType(), true);
    _incomingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(@Nonnull TimeGranularitySpec incomingGranularitySpec, @Nonnull Object defaultNullValue) {
    super(incomingGranularitySpec.getName(), incomingGranularitySpec.getDataType(), true, defaultNullValue);
    _incomingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(@Nonnull TimeGranularitySpec incomingGranularitySpec,
      @Nonnull TimeGranularitySpec outgoingGranularitySpec) {
    super(outgoingGranularitySpec.getName(), outgoingGranularitySpec.getDataType(), true);
    _incomingGranularitySpec = incomingGranularitySpec;
    _outgoingGranularitySpec = outgoingGranularitySpec;
  }

  public TimeFieldSpec(@Nonnull TimeGranularitySpec incomingGranularitySpec,
      @Nonnull TimeGranularitySpec outgoingGranularitySpec, @Nonnull Object defaultNullValue) {
    super(outgoingGranularitySpec.getName(), outgoingGranularitySpec.getDataType(), true, defaultNullValue);
    _incomingGranularitySpec = incomingGranularitySpec;
    _outgoingGranularitySpec = outgoingGranularitySpec;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public FieldType getFieldType() {
    return FieldType.TIME;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setName(@Nonnull String name) {
    // Ignore setName for TimeFieldSpec because we pick the name from TimeGranularitySpec.
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setDataType(@Nonnull DataType dataType) {
    // Ignore setDataType for TimeFieldSpec because we pick the data type from TimeGranularitySpec.
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setSingleValueField(boolean isSingleValueField) {
    Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for time field.");
  }

  @JsonIgnore
  @Nonnull
  public String getIncomingTimeColumnName() {
    return _incomingGranularitySpec.getName();
  }

  @JsonIgnore
  @Nonnull
  public String getOutgoingTimeColumnName() {
    return getName();
  }

  // For third-eye backward compatible.
  @Deprecated
  @JsonIgnore
  @Nonnull
  public String getOutGoingTimeColumnName() {
    return getName();
  }

  @Nonnull
  public TimeGranularitySpec getIncomingGranularitySpec() {
    return _incomingGranularitySpec;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setIncomingGranularitySpec(@Nonnull TimeGranularitySpec incomingGranularitySpec) {
    _incomingGranularitySpec = incomingGranularitySpec;
    if (_outgoingGranularitySpec == null) {
      super.setName(incomingGranularitySpec.getName());
      super.setDataType(incomingGranularitySpec.getDataType());
    }
  }

  @Nonnull
  public TimeGranularitySpec getOutgoingGranularitySpec() {
    if (_outgoingGranularitySpec == null) {
      return _incomingGranularitySpec;
    } else {
      return _outgoingGranularitySpec;
    }
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setOutgoingGranularitySpec(@Nonnull TimeGranularitySpec outgoingGranularitySpec) {
    _outgoingGranularitySpec = outgoingGranularitySpec;
    super.setName(outgoingGranularitySpec.getName());
    super.setDataType(outgoingGranularitySpec.getDataType());
  }

  @Nonnull
  @Override
  public JsonObject toJsonObject() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("incomingGranularitySpec", _incomingGranularitySpec.toJsonObject());
    if (!getOutgoingGranularitySpec().equals(_incomingGranularitySpec)) {
      jsonObject.add("outgoingGranularitySpec", _outgoingGranularitySpec.toJsonObject());
    }
    appendDefaultNullValue(jsonObject);
    return jsonObject;
  }

  @Override
  public String toString() {
    return "< field type: TIME, incoming granularity spec: " + _incomingGranularitySpec
        + ", outgoing granularity spec: " + getOutgoingGranularitySpec() + ", default null value: " + _defaultNullValue
        + " >";
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    TimeFieldSpec that = (TimeFieldSpec) o;
    return EqualityUtils.isEqual(_incomingGranularitySpec, that._incomingGranularitySpec) && EqualityUtils.isEqual(
        getOutgoingGranularitySpec(), that.getOutgoingGranularitySpec());
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(super.hashCode(), _incomingGranularitySpec);
    result = EqualityUtils.hashCodeOf(result, getOutgoingGranularitySpec());
    return result;
  }
}
