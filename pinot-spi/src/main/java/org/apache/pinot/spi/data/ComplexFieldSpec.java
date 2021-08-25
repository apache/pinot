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


/**
 * FieldSpec for complex fields. The {@link org.apache.pinot.spi.data.FieldSpec.FieldType}
 * is COMPLEX and the inner data type represents the root data type of the field.
 * It could be STRUCT, MAP or LIST. A complex field is composable with a single root type
 * and a number of child types. Although we have multi-value primitive columns, LIST
 * is for representing lists of both complex and primitives inside a complex field.
 *
 * Consider a person json where the root type is STRUCT and composes of inner members:
 *  STRUCT(
 *          name: STRING
 *          age: INT
 *          salary: INT
 *          addresses: LIST (STRUCT
 *                              apt: INT
 *                              street: STRING
 *                              city: STRING
 *                              zip: INT
 *                          )
 *        )
 *
 * The fieldspec would be COMPLEX with type as STRUCT and 4 inner members
 * to model the hierarchy
 */
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
    Preconditions.checkArgument(dataType == DataType.STRUCT || dataType == DataType.MAP || dataType == DataType.LIST);
    _childFieldSpecs = new HashMap<>();
  }

  public FieldSpec getChildFieldSpec(String child) {
    return _childFieldSpecs.get(child);
  }

  public void addChildFieldSpec(String child, FieldSpec fieldSpec) {
    _childFieldSpecs.put(child, fieldSpec);
  }

  public Map<String, FieldSpec> getChildFieldSpecs() {
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
    return "field type: COMPLEX, field name: " + _name + ", root data type: " + _dataType;
  }
}
