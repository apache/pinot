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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.JsonUtils;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class TimeFieldSpec extends FieldSpec {
  private TimeGranularitySpec _incomingGranularitySpec;
  private TimeGranularitySpec _outgoingGranularitySpec;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public TimeFieldSpec() {
    super();
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, TimeUnit incomingTimeUnit) {
    super(incomingName, incomingDataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, TimeUnit incomingTimeUnit,
      Object defaultNullValue) {
    super(incomingName, incomingDataType, true, defaultNullValue);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, TimeUnit incomingTimeUnit, String outgoingName,
      DataType outgoingDataType, TimeUnit outgoingTimeUnit) {
    super(outgoingName, outgoingDataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec = new TimeGranularitySpec(outgoingDataType, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, TimeUnit incomingTimeUnit, String outgoingName,
      DataType outgoingDataType, TimeUnit outgoingTimeUnit, Object defaultNullValue) {
    super(outgoingName, outgoingDataType, true, defaultNullValue);
    _incomingGranularitySpec = new TimeGranularitySpec(incomingDataType, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec = new TimeGranularitySpec(outgoingDataType, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, int incomingTimeUnitSize,
      TimeUnit incomingTimeUnit) {
    super(incomingName, incomingDataType, true);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, int incomingTimeUnitSize,
      TimeUnit incomingTimeUnit, Object defaultNullValue) {
    super(incomingName, incomingDataType, true, defaultNullValue);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, int incomingTimeUnitSize,
      TimeUnit incomingTimeUnit, String outgoingName, DataType outgoingDataType, int outgoingTimeUnitSize,
      TimeUnit outgoingTimeUnit) {
    super(outgoingName, outgoingDataType, true);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec =
        new TimeGranularitySpec(outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(String incomingName, DataType incomingDataType, int incomingTimeUnitSize,
      TimeUnit incomingTimeUnit, String outgoingName, DataType outgoingDataType, int outgoingTimeUnitSize,
      TimeUnit outgoingTimeUnit, Object defaultNullValue) {
    super(outgoingName, outgoingDataType, true, defaultNullValue);
    _incomingGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    _outgoingGranularitySpec =
        new TimeGranularitySpec(outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec) {
    super(incomingGranularitySpec.getName(), incomingGranularitySpec.getDataType(), true);
    _incomingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec, Object defaultNullValue) {
    super(incomingGranularitySpec.getName(), incomingGranularitySpec.getDataType(), true, defaultNullValue);
    _incomingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec, TimeGranularitySpec outgoingGranularitySpec) {
    super(outgoingGranularitySpec.getName(), outgoingGranularitySpec.getDataType(), true);
    _incomingGranularitySpec = incomingGranularitySpec;
    _outgoingGranularitySpec = outgoingGranularitySpec;
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec, TimeGranularitySpec outgoingGranularitySpec,
      Object defaultNullValue) {
    super(outgoingGranularitySpec.getName(), outgoingGranularitySpec.getDataType(), true, defaultNullValue);
    _incomingGranularitySpec = incomingGranularitySpec;
    _outgoingGranularitySpec = outgoingGranularitySpec;
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.TIME;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setName(String name) {
    // Ignore setName for TimeFieldSpec because we pick the name from TimeGranularitySpec.
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setDataType(DataType dataType) {
    // Ignore setDataType for TimeFieldSpec because we pick the data type from TimeGranularitySpec.
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setSingleValueField(boolean isSingleValueField) {
    Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for time field.");
  }

  @JsonIgnore
  public String getIncomingTimeColumnName() {
    return _incomingGranularitySpec.getName();
  }

  @JsonIgnore
  public String getOutgoingTimeColumnName() {
    return getName();
  }

  // For third-eye backward compatible.
  @Deprecated
  @JsonIgnore
  public String getOutGoingTimeColumnName() {
    return getName();
  }

  public TimeGranularitySpec getIncomingGranularitySpec() {
    return _incomingGranularitySpec;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setIncomingGranularitySpec(TimeGranularitySpec incomingGranularitySpec) {
    _incomingGranularitySpec = incomingGranularitySpec;
    if (_outgoingGranularitySpec == null) {
      super.setName(incomingGranularitySpec.getName());
      super.setDataType(incomingGranularitySpec.getDataType());
    }
  }

  public TimeGranularitySpec getOutgoingGranularitySpec() {
    if (_outgoingGranularitySpec == null) {
      return _incomingGranularitySpec;
    } else {
      return _outgoingGranularitySpec;
    }
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setOutgoingGranularitySpec(TimeGranularitySpec outgoingGranularitySpec) {
    _outgoingGranularitySpec = outgoingGranularitySpec;
    super.setName(outgoingGranularitySpec.getName());
    super.setDataType(outgoingGranularitySpec.getDataType());
  }

  @Override
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = JsonUtils.newObjectNode();
    jsonObject.set("incomingGranularitySpec", _incomingGranularitySpec.toJsonObject());
    if (!getOutgoingGranularitySpec().equals(_incomingGranularitySpec)) {
      jsonObject.set("outgoingGranularitySpec", _outgoingGranularitySpec.toJsonObject());
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
    return EqualityUtils.isEqual(_incomingGranularitySpec, that._incomingGranularitySpec) && EqualityUtils
        .isEqual(getOutgoingGranularitySpec(), that.getOutgoingGranularitySpec());
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(super.hashCode(), _incomingGranularitySpec);
    result = EqualityUtils.hashCodeOf(result, getOutgoingGranularitySpec());
    return result;
  }
}
