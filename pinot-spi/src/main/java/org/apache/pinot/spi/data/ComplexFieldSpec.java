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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


@JsonIgnoreProperties(ignoreUnknown = true)
public final class ComplexFieldSpec extends FieldSpec {

  private final Map<String, FieldSpec> _childFieldSpecs;

  // Default constructor required by JSON de-serializer
  public ComplexFieldSpec() {
    super();
    _childFieldSpecs = new HashMap<>();
  }

  public ComplexFieldSpec(@Nonnull String name, DataType dataType, boolean isSingleValueField) {
    super(name, dataType, isSingleValueField);
    Preconditions.checkArgument(dataType == DataType.STRUCT || dataType == DataType.LIST);
    _childFieldSpecs = new HashMap<>();
  }

  public FieldSpec getChildFieldSpec(String child) {
    return _childFieldSpecs.get(child);
  }

  public void addChildFieldSpec(String child, FieldSpec fieldSpec) {
    _childFieldSpecs.put(child, fieldSpec);
  }

  public Map<String,FieldSpec> getChildFieldSpecs() {
    return _childFieldSpecs;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public FieldType getFieldType() {
    return FieldType.COMPLEX;
  }

  @Override
  public String toString() {
    return "< field type: COMPLEX, field name: " + _name + ", data type: " + _dataType + ", is single-value field: "
        + _isSingleValueField + ", default null value: " + _defaultNullValue + " >";
  }
}