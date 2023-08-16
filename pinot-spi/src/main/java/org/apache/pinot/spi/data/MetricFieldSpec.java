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
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;


/**
 * The <code>MetricFieldSpec</code> class contains all specs related to any metric field (column) in {@link Schema}.\
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MetricFieldSpec extends FieldSpec {

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public MetricFieldSpec() {
    super();
  }

  public MetricFieldSpec(String name, DataType dataType) {
    super(name, dataType, true);
  }

  public MetricFieldSpec(String name, DataType dataType, @Nullable Object defaultNullValue) {
    super(name, dataType, true, defaultNullValue);
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.METRIC;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  @Override
  public void setSingleValueField(boolean isSingleValueField) {
    Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for metric field.");
  }

  @Override
  public String toString() {
    return "< field type: METRIC, field name: " + _name + ", data type: " + _dataType + ", default null value: "
        + _defaultNullValue + " >";
  }
}
