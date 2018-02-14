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
import javax.annotation.Nullable;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


/**
 * The <code>MetricFieldSpec</code> class contains all specs related to any metric field (column) in {@link Schema}.
 * <p>Different with {@link DimensionFieldSpec}, inside <code>MetricFieldSpec</code> we allow user defined
 * {@link DerivedMetricType} and <code>fieldSize</code>.
 * <p>{@link DerivedMetricType} is used when the metric field is derived from some other fields (e.g. HLL).
 * <p><code>fieldSize</code> is used to mark the size of the value when the size is not constant (e.g. STRING).
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MetricFieldSpec extends FieldSpec {
  private static final int UNDEFINED_FIELD_SIZE = -1;

  // These two fields are for derived metric fields.
  private int _fieldSize = UNDEFINED_FIELD_SIZE;
  private DerivedMetricType _derivedMetricType = null;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public MetricFieldSpec() {
    super();
  }

  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType) {
    super(name, dataType, true);
    _fieldSize = _dataType.size();
  }

  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType, @Nonnull Object defaultNullValue) {
    super(name, dataType, true, defaultNullValue);
    _fieldSize = _dataType.size();
  }

  // For derived metric fields.
  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
      @Nonnull DerivedMetricType derivedMetricType) {
    super(name, dataType, true);
    setFieldSize(fieldSize);
    _derivedMetricType = derivedMetricType;
  }

  // For derived metric fields.
  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
      @Nonnull DerivedMetricType derivedMetricType, @Nonnull Object defaultNullValue) {
    super(name, dataType, true, defaultNullValue);
    setFieldSize(fieldSize);
    _derivedMetricType = derivedMetricType;
  }

  public int getFieldSize() {
    return _fieldSize;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setFieldSize(int fieldSize) {
    Preconditions.checkArgument(fieldSize > 0, "Field size: " + fieldSize + " is not a positive number.");
    if (_dataType != DataType.STRING) {
      Preconditions.checkArgument(fieldSize == _dataType.size(),
          "Field size: " + fieldSize + " does not match data type: " + _dataType);
    }
    _fieldSize = fieldSize;
  }

  @Nullable
  public DerivedMetricType getDerivedMetricType() {
    return _derivedMetricType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDerivedMetricType(@Nullable DerivedMetricType derivedMetricType) {
    _derivedMetricType = derivedMetricType;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public FieldType getFieldType() {
    return FieldType.METRIC;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setDataType(@Nonnull DataType dataType) {
    super.setDataType(dataType);
    if (_dataType != DataType.STRING) {
      _fieldSize = _dataType.size();
    }
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setSingleValueField(boolean isSingleValueField) {
    Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for metric field.");
  }

  @JsonIgnore
  public boolean isDerivedMetric() {
    return _derivedMetricType != null;
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
    // HLL derived metric type.
    HLL
  }

  @Nonnull
  @Override
  public JsonObject toJsonObject() {
    JsonObject jsonObject = super.toJsonObject();
    if (_dataType == DataType.STRING && _fieldSize != UNDEFINED_FIELD_SIZE) {
      jsonObject.addProperty("fieldSize", _fieldSize);
    }
    if (_derivedMetricType != null) {
      jsonObject.addProperty("derivedMetricType", _derivedMetricType.name());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "< field type: METRIC, field name: " + _name + ", data type: " + _dataType + ", default null value: "
        + _defaultNullValue + ", field size: " + _fieldSize + ", derived metric type: " + _derivedMetricType + " >";
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    MetricFieldSpec that = (MetricFieldSpec) o;
    return EqualityUtils.isEqual(_fieldSize, that._fieldSize) && EqualityUtils.isEqual(_derivedMetricType,
        that._derivedMetricType);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(super.hashCode(), _fieldSize);
    result = EqualityUtils.hashCodeOf(result, _derivedMetricType);
    return result;
  }
}
