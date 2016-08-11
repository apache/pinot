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
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public final class TimeFieldSpec extends FieldSpec {
  private TimeGranularitySpec _incomingGranularitySpec;
  private TimeGranularitySpec _outgoingGranularitySpec;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public TimeFieldSpec() {
    super();
  }

  public TimeFieldSpec(String name, DataType dataType, TimeUnit timeUnit) {
    super(name, dataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(dataType, timeUnit, name);
  }


  public TimeFieldSpec(String name, DataType dataType, int timeUnitSize, TimeUnit timeUnit) {
    super(name, dataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(dataType, timeUnitSize, timeUnit, name);
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec) {
    super(incomingGranularitySpec.getName(), incomingGranularitySpec.getDataType(), true);
    _incomingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec, TimeGranularitySpec outgoingGranularitySpec) {
    super(outgoingGranularitySpec.getName(), outgoingGranularitySpec.getDataType(), true);
    Preconditions.checkNotNull(incomingGranularitySpec);

    _incomingGranularitySpec = incomingGranularitySpec;
    _outgoingGranularitySpec = outgoingGranularitySpec;
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.TIME;
  }

  @Override
  public void setName(String name) {
    // Ignore setName for TimeFieldSpec because we pick the name from TimeGranularitySpec.
  }

  @Override
  public void setDataType(DataType dataType) {
    // Ignore setDataType for TimeFieldSpec because we pick the data type from TimeGranularitySpec.
  }

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

  @JsonIgnore
  @Deprecated
  // For third-eye backward compatible.
  public String getOutGoingTimeColumnName() {
    return getName();
  }

  public void setIncomingGranularitySpec(TimeGranularitySpec incomingGranularitySpec) {
    Preconditions.checkNotNull(incomingGranularitySpec);

    _incomingGranularitySpec = incomingGranularitySpec;
    if (_outgoingGranularitySpec == null) {
      super.setName(incomingGranularitySpec.getName());
      super.setDataType(incomingGranularitySpec.getDataType());
    }
  }

  public TimeGranularitySpec getIncomingGranularitySpec() {
    return _incomingGranularitySpec;
  }

  public void setOutgoingGranularitySpec(TimeGranularitySpec outgoingGranularitySpec) {
    Preconditions.checkNotNull(outgoingGranularitySpec);

    _outgoingGranularitySpec = outgoingGranularitySpec;
    super.setName(outgoingGranularitySpec.getName());
    super.setDataType(outgoingGranularitySpec.getDataType());
  }

  public TimeGranularitySpec getOutgoingGranularitySpec() {
    if (_outgoingGranularitySpec == null) {
      return _incomingGranularitySpec;
    } else {
      return _outgoingGranularitySpec;
    }
  }

  @Override
  public String toString() {
    return "< field type: TIME, incoming granularity spec: " + _incomingGranularitySpec
        + ", outgoing granularity spec: " + getOutgoingGranularitySpec() + ", default null value: "
        + getDefaultNullValue() + " >";
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof TimeFieldSpec) {
      TimeFieldSpec anotherTimeFieldSpec = (TimeFieldSpec) anObject;
      return _incomingGranularitySpec.equals(anotherTimeFieldSpec._incomingGranularitySpec)
          && getOutgoingGranularitySpec().equals(anotherTimeFieldSpec.getOutgoingGranularitySpec())
          && getDefaultNullValue().equals(anotherTimeFieldSpec.getDefaultNullValue());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = _incomingGranularitySpec.hashCode();
    result = EqualityUtils.hashCodeOf(result, getOutgoingGranularitySpec());
    result = EqualityUtils.hashCodeOf(result, getDefaultNullValue());
    return result;
  }
}
