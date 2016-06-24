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
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonIgnore;


public class TimeFieldSpec extends FieldSpec {

  private TimeGranularitySpec _incomingGranularitySpec;
  private TimeGranularitySpec _outgoingGranularitySpec;

  public TimeFieldSpec() {
    super();
    setFieldType(FieldType.TIME);
  }

  public TimeFieldSpec(String name, DataType dataType, TimeUnit timeUnit) {
    super(name, FieldType.TIME, dataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(dataType, timeUnit, name);
    _outgoingGranularitySpec = _incomingGranularitySpec;
    _isDataTypeSet = true;
  }


  public TimeFieldSpec(String name, DataType dataType, int size, TimeUnit timeUnit) {
    super(name, FieldType.TIME, dataType, true);
    _incomingGranularitySpec = new TimeGranularitySpec(dataType, size, timeUnit, name);
    _outgoingGranularitySpec = _incomingGranularitySpec;
    _isDataTypeSet = true;
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec) {
    super(incomingGranularitySpec.getName(), FieldType.TIME, incomingGranularitySpec.getDataType(), true);
    _incomingGranularitySpec = incomingGranularitySpec;
    _outgoingGranularitySpec = _incomingGranularitySpec;
    _isDataTypeSet = true;
  }

  public TimeFieldSpec(TimeGranularitySpec incomingGranularitySpec, TimeGranularitySpec outgoingGranularitySpec) {
    super(incomingGranularitySpec.getName(), FieldType.TIME, incomingGranularitySpec.getDataType(), true);
    _incomingGranularitySpec = incomingGranularitySpec;
    _outgoingGranularitySpec = outgoingGranularitySpec;
    _isDataTypeSet = true;
  }

  @Override
  public String getName() {
    return getOutgoingGranularitySpec().getName();
  }

  @Override
  public DataType getDataType() {
    return getOutgoingGranularitySpec().getDataType();
  }

  @JsonIgnore
  public String getIncomingTimeColumnName() {
    return _incomingGranularitySpec.getName();
  }

  @JsonIgnore
  public String getOutGoingTimeColumnName() {
    return getOutgoingGranularitySpec().getName();
  }

  public void setIncomingGranularitySpec(TimeGranularitySpec incomingGranularitySpec) {
    Preconditions.checkNotNull(incomingGranularitySpec);

    _isDataTypeSet = true;
    _incomingGranularitySpec = incomingGranularitySpec;

    // If already set default null value but not outgoing granularity spec, update the default null value.
    if (_isDefaultNullValueSet) {
      updateDefaultNullValue();
    }
  }

  public TimeGranularitySpec getIncomingGranularitySpec() {
    return _incomingGranularitySpec;
  }

  public void setOutgoingGranularitySpec(TimeGranularitySpec outgoingGranularitySpec) {
    Preconditions.checkNotNull(outgoingGranularitySpec);

    _isDataTypeSet = true;
    _outgoingGranularitySpec = outgoingGranularitySpec;

    // If already set default null value, update the default null value.
    if (_isDefaultNullValueSet) {
      updateDefaultNullValue();
    }
  }

  public TimeGranularitySpec getOutgoingGranularitySpec() {
    if (_outgoingGranularitySpec == null) {
      _outgoingGranularitySpec = _incomingGranularitySpec;
    }
    return _outgoingGranularitySpec;
  }

  @Override
  public String toString() {
    return "< data type: " + getDataType() + ", field type : " + getFieldType() + ", incoming granularity spec: "
        + getIncomingGranularitySpec() + ", outgoing granularity spec: " + getOutgoingGranularitySpec()
        + ", default null value: " + getDefaultNullValue() + " >";
  }
}
