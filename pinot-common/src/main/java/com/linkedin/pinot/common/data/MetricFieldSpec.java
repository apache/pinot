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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public final class MetricFieldSpec extends FieldSpec {

  private int fieldSize = -1;
  private DerivedMetricType derivedMetricType = null;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public MetricFieldSpec() {
    super();
  }

  public MetricFieldSpec(String name, DataType dataType) {
    super(name, dataType, true);
  }

  /**
   * For Derived Fields
   * @param name
   * @param dataType
   * @param derivedMetricType
   */
  public MetricFieldSpec(String name, DataType dataType, int fieldSize, DerivedMetricType derivedMetricType) {
    super(name, dataType, true);
    this.fieldSize = fieldSize;
    this.derivedMetricType = derivedMetricType;
  }

  public int getFieldSize() {
    if (fieldSize == -1) {
      return getDataType().size();
    } else {
      return fieldSize;
    }
  }

  public void setFieldSize(int fieldSize) {
    this.fieldSize = fieldSize;
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.METRIC;
  }

  @Override
  public void setSingleValueField(boolean isSingleValueField) {
    Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for metric field.");
  }

  @JsonIgnore
  public boolean isDerivedMetric() {
    return derivedMetricType != null;
  }

  public DerivedMetricType getDerivedMetricType() {
    return derivedMetricType;
  }

  public void setDerivedMetricType(DerivedMetricType derivedMetricType) {
    this.derivedMetricType = derivedMetricType;
  }

  /**
   * DerivedMetricType is assigned for all metric fields to allow derived metric field a customized type not in DataType.
   * Since we cannot add a new type directly to DataType since Pinot currently has no support for a custom types.
   *
   * It is currently used for derived field recognition in MetricBuffer, may have other use cases in system later.
   *
   * Generally, a customized type value should be converted to a standard Pinot type value (like String)
   * for serialization, and converted back after deserialization when in use.
   */
  public enum DerivedMetricType {
    // customized types start from here
    HLL
  }
}
