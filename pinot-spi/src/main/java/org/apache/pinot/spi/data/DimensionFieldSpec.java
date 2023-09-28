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
import javax.annotation.Nullable;


@JsonIgnoreProperties(ignoreUnknown = true)
public final class DimensionFieldSpec extends FieldSpec {

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public DimensionFieldSpec() {
    super();
  }

  public DimensionFieldSpec(String name, DataType dataType, boolean isSingleValueField) {
    super(name, dataType, isSingleValueField);
  }

  public DimensionFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      @Nullable Object defaultNullValue) {
    super(name, dataType, isSingleValueField, defaultNullValue);
  }

  public DimensionFieldSpec(String name, DataType dataType, boolean isSingleValueField, int maxLength,
      @Nullable Object defaultNullValue) {
    super(name, dataType, isSingleValueField, maxLength, defaultNullValue);
  }

  public DimensionFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      Class virtualColumnProviderClass) {
    super(name, dataType, isSingleValueField);
    _virtualColumnProvider = virtualColumnProviderClass.getName();
  }

  public DimensionFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      Class virtualColumnProviderClass, @Nullable Object defaultNullValue) {
    super(name, dataType, isSingleValueField, defaultNullValue);
    _virtualColumnProvider = virtualColumnProviderClass.getName();
  }

  public DimensionFieldSpec(String name, DataType dataType, DataType vectorDataType, int vectorLength,
      Object defaultNullValue) {
    super(name, dataType, vectorDataType, vectorLength, defaultNullValue);
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.DIMENSION;
  }

  @Override
  public String toString() {
    return "< field type: DIMENSION, field name: " + _name + ", data type: " + _dataType + ", is single-value field: "
        + _isSingleValueField + ", default null value: " + _defaultNullValue + " >";
  }
}
