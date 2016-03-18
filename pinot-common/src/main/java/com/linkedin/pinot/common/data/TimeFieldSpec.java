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
package com.linkedin.pinot.common.data;

import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonIgnore;


public class TimeFieldSpec extends FieldSpec {

  private TimeGranularitySpec incomingGranularitySpec;
  private TimeGranularitySpec outgoingGranularitySpec;

  public TimeFieldSpec() {
    super();
    setFieldType(FieldType.TIME);
    this.incomingGranularitySpec = null;
    this.outgoingGranularitySpec = null;
  }

  public TimeFieldSpec(String name, DataType dType, TimeUnit timeType) {
    super(name, FieldType.TIME, dType, true);
    this.incomingGranularitySpec = new TimeGranularitySpec(dType, timeType, name);
    this.outgoingGranularitySpec = incomingGranularitySpec;
  }


  public TimeFieldSpec(String name, DataType dType, int size, TimeUnit timeType) {
    super(name, FieldType.TIME, dType, true);
    this.incomingGranularitySpec = new TimeGranularitySpec(dType, size, timeType, name);
    this.outgoingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(TimeGranularitySpec incominGranularitySpec) {
    super(incominGranularitySpec.getName(), FieldType.TIME, incominGranularitySpec.getDataType(), true);
    this.incomingGranularitySpec = incominGranularitySpec;
    this.outgoingGranularitySpec = incomingGranularitySpec;
  }

  public TimeFieldSpec(TimeGranularitySpec incominGranularitySpec, TimeGranularitySpec outgoingGranularitySpec) {
    super(incominGranularitySpec.getName(), FieldType.TIME, incominGranularitySpec.getDataType(), true);
    this.incomingGranularitySpec = incominGranularitySpec;
    this.outgoingGranularitySpec = outgoingGranularitySpec;
  }

  @JsonIgnore
  public boolean isSame() {
    if (this.outgoingGranularitySpec == null && this.incomingGranularitySpec != null) {
      return true;
    }
    return incomingGranularitySpec.equals(outgoingGranularitySpec);
  }

  @Override
  public String getName() {
    return getOutgoingGranularitySpec().getName();
  }

  @Override
  public String getDelimiter() {
    return null;
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.TIME;
  }

  @Override
  public DataType getDataType() {
    return getOutgoingGranularitySpec().getDataType();
  }

  @Override
  public boolean isSingleValueField() {
    return true;
  }

  @JsonIgnore(true)
  public String getIncomingTimeColumnName() {
    return incomingGranularitySpec.getName();
  }

  @JsonIgnore(true)
  public String getOutGoingTimeColumnName() {
    return getOutgoingGranularitySpec().getName();
  }

  public void setIncomingGranularitySpec(TimeGranularitySpec incomingGranularitySpec) {
    this.incomingGranularitySpec = incomingGranularitySpec;
  }

  public void setOutgoingGranularitySpec(TimeGranularitySpec outgoingGranularitySpec) {
    this.outgoingGranularitySpec = outgoingGranularitySpec;
  }

  public TimeGranularitySpec getIncomingGranularitySpec() {
    return incomingGranularitySpec;
  }

  public TimeGranularitySpec getOutgoingGranularitySpec() {
    if (outgoingGranularitySpec == null) {
      outgoingGranularitySpec = incomingGranularitySpec;
    }
    return outgoingGranularitySpec;
  }
}
