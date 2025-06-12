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
package org.apache.pinot.core.udf;

import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

/// A parameter on the signature of a [UDF][Udf].
///
/// This class contains the DataType of the parameter and whether it is multivalued.
public class UdfParameter {
  private final FieldSpec.DataType _dataType;
  private final boolean _multivalued;

  private UdfParameter(FieldSpec.DataType dataType, boolean multivalued) {
    _dataType = dataType;
    _multivalued = multivalued;
  }

  public FieldSpec.DataType getDataType() {
    return _dataType;
  }

  public boolean isMultivalued() {
    return _multivalued;
  }

  /// Returns the list of constraints that this parameter has.
  ///
  /// This can be useful to define specific static constraints on the parameter, such as having a specific index.
  public List<Constraint> getConstraints() {
    return List.of();
  }

  public static UdfParameter of(FieldSpec.DataType dataType, boolean multivalued) {
    return new UdfParameter(dataType, multivalued);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof UdfParameter)) {
      return false;
    }
    UdfParameter other = (UdfParameter) o;
    return getDataType() == other.getDataType() && isMultivalued() == other.isMultivalued();
  }

  @Override
  public int hashCode() {
    return 31 * getDataType().hashCode() + (isMultivalued() ? 1 : 0);
  }

  @Override
  public String toString() {
    // This strange way to represent multivalued types is close to Java type descriptor
    // see https://docs.oracle.com/javase/specs/jvms/se17/html/jvms-4.html#jvms-4.3
    return (isMultivalued() ? "[L" : "") + getDataType().name().toLowerCase();
  }

  public static abstract class Constraint {
    public abstract void updateTableConfig(TableConfigBuilder tableConfigBuilder, String columnName);
  }
}
