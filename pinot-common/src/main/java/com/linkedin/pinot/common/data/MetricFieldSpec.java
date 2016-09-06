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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


/**
 * The <code>MetricFieldSpec</code> class contains all specs related to any metric field (column) in {@link Schema}.
 * <p>Different with {@link DimensionFieldSpec}, inside <code>MetricFieldSpec</code> we allow user defined
 * {@link DerivedMetricType} and <code>fieldSize</code>.
 * <p>{@link DerivedMetricType} is used when the metric field is derived from some other fields (e.g. HLL).
 * <p><code>fieldSize</code> is used to mark the size of the value when the size is not constant (e.g. STRING).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MetricFieldSpec extends FieldSpec {
  private static final String DEFAULT_DERIVED_METRIC_NULL_VALUE_OF_STRING = "null";

  // These two fields are for derived metric fields.
  private int fieldSize = -1;
  private DerivedMetricType derivedMetricType = null;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public MetricFieldSpec() {
    super();
  }

  public MetricFieldSpec(String name, DataType dataType) {
    super(name, dataType, true);
  }

  // For derived metric fields.
  public MetricFieldSpec(String name, DataType dataType, int fieldSize, DerivedMetricType derivedMetricType) {
    super(name, dataType, true);
    Preconditions.checkNotNull(derivedMetricType);
    this.fieldSize = fieldSize;
    this.derivedMetricType = derivedMetricType;
  }

  public int getFieldSize() {
    if (fieldSize < 0) {
      return getDataType().size();
    } else {
      return fieldSize;
    }
  }

  public void setFieldSize(int fieldSize) {
    this.fieldSize = fieldSize;
  }

  public DerivedMetricType getDerivedMetricType() {
    return derivedMetricType;
  }

  public void setDerivedMetricType(DerivedMetricType derivedMetricType) {
    this.derivedMetricType = derivedMetricType;
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

  @Override
  public Object getDefaultNullValue() {
    if (derivedMetricType != null && getDataType() == DataType.STRING) {
      return DEFAULT_DERIVED_METRIC_NULL_VALUE_OF_STRING;
    } else {
      return super.getDefaultNullValue();
    }
  }

  @JsonIgnore
  public boolean isDerivedMetric() {
    return derivedMetricType != null;
  }

  /**
   * The <code>DerivedMetricType</code> enum is assigned for all metric fields to allow derived metric field, a
   * customized type which is not included in DataType.
   * <p>It is currently used for derived field recognition in star tree <code>MetricBuffer</code>, may have other use
   * cases later.
   * <p>Generally, a customized type value should be converted to a standard
   * {@link com.linkedin.pinot.common.data.FieldSpec.DataType} for storage, and converted back when needed.
   */
  public enum DerivedMetricType {
    // HLL derived metric
    HLL
  }

  @Override
  public String toString() {
    return "< field type: METRIC, field name: " + getName() + ", data type: " + getDataType() + ", default null value: "
        + getDefaultNullValue() + ", field size: " + fieldSize + ", derived metric type: " + derivedMetricType + " >";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object instanceof MetricFieldSpec) {
      MetricFieldSpec that = (MetricFieldSpec) object;

      return getName().equals(that.getName())
          && getDataType() == that.getDataType()
          && getDefaultNullValue().equals(that.getDefaultNullValue())
          && getFieldSize() == that.getFieldSize()
          && derivedMetricType == that.derivedMetricType;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = EqualityUtils.hashCodeOf(result, getDataType());
    result = EqualityUtils.hashCodeOf(result, getDefaultNullValue());
    result = EqualityUtils.hashCodeOf(result, getFieldSize());
    result = EqualityUtils.hashCodeOf(result, derivedMetricType);
    return result;
  }
}
