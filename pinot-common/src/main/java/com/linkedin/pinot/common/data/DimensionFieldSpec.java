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

import javax.annotation.Nonnull;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public final class DimensionFieldSpec extends FieldSpec {

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public DimensionFieldSpec() {
    super();
  }

  public DimensionFieldSpec(@Nonnull String name, @Nonnull DataType dataType, boolean isSingleValueField) {
    super(name, dataType, isSingleValueField);
  }

  public DimensionFieldSpec(@Nonnull String name, @Nonnull DataType dataType, boolean isSingleValueField,
      @Nonnull Object defaultNullValue) {
    super(name, dataType, isSingleValueField, defaultNullValue);
  }

  @JsonIgnore
  @Nonnull
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
