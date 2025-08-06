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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

/// A parameter on the signature of a [UDF][Udf].
///
/// This class contains the DataType of the parameter and whether it is multivalued.
public class UdfParameter {
  private final String _name;
  private final FieldSpec.DataType _dataType;
  private final boolean _multivalued;
  private final List<Constraint> _constraints;
  @Nullable
  private final String _description;

  private UdfParameter(
      String name,
      FieldSpec.DataType dataType,
      boolean multivalued,
      List<Constraint> constraints,
      String description) {
    _name = name;
    _dataType = dataType;
    _multivalued = multivalued;
    _constraints = constraints;
    _description = description;
  }

  public static UdfParameter result(FieldSpec.DataType dataType) {
    return of("result", dataType);
  }

  public static UdfParameter of(String name, FieldSpec.DataType dataType) {
    return new UdfParameter(name, dataType, false, List.of(), null);
  }

  public UdfParameter asMultiValued() {
    if (_multivalued) {
      return this;
    }
    return new UdfParameter(_name, _dataType, true, _constraints, _description);
  }

  public String getName() {
    return _name;
  }

  public FieldSpec.DataType getDataType() {
    return _dataType;
  }

  public boolean isMultivalued() {
    return _multivalued;
  }

  public boolean isLiteralOnly() {
    return _constraints.contains(LiteralConstraint.INSTANCE);
  }

  @Nullable
  public String getDescription() {
    return _description;
  }

  public UdfParameter withDescription(String description) {
    return new UdfParameter(_name, _dataType, _multivalued, _constraints, description);
  }

  public UdfParameter asLiteralOnly() {
    if (isLiteralOnly()) {
      return this;
    }
    return new UdfParameter(_name, _dataType, _multivalued, List.of(LiteralConstraint.INSTANCE), _description);
  }

  /// Returns the list of constraints that this parameter has.
  ///
  /// This can be useful to define specific static constraints on the parameter, such as having a specific index.
  public List<Constraint> getConstraints() {
    return _constraints;
  }

  public UdfParameter constrainedWith(Constraint constraint) {
    List<Constraint> newConstraints;
    if (_constraints.isEmpty()) {
      newConstraints = List.of(constraint);
    } else {
      newConstraints = new ArrayList<>(_constraints.size() + 1);
      newConstraints.addAll(_constraints);
      newConstraints.add(constraint);
    }
    return new UdfParameter(_name, _dataType, _multivalued, newConstraints, _description);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof UdfParameter)) {
      return false;
    }
    UdfParameter that = (UdfParameter) o;
    return isMultivalued() == that.isMultivalued() && Objects.equals(_name, that._name)
        && getDataType() == that.getDataType() && Objects.equals(getConstraints(), that.getConstraints())
        && Objects.equals(getDescription(), that.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, getDataType(), isMultivalued(), getConstraints(), getDescription());
  }

  @Override
  public String toString() {
    return _name + ": " + getTypeString();
  }

  public String getTypeString() {
    String type;
    if (isMultivalued()) {
      type = "ARRAY(" + getDataType().name().toLowerCase() + ")";
    } else {
      type = getDataType().name().toLowerCase();
    }
    return type;
  }

  /// A constraint on a UDF parameter.
  ///
  /// This can be used to define specific constraints on the parameter, such as requiring it to be a literal value,
  /// or to have a specific index.
  ///
  /// Remember equals and hashcode should be implemented. Otherwise UdfParameter.equals() may fail unexpectedly.
  public interface Constraint {
    void updateTableConfig(TableConfigBuilder tableConfigBuilder, String columnName);
  }

  public static class LiteralConstraint implements Constraint {
    public static final LiteralConstraint INSTANCE = new LiteralConstraint();
    private LiteralConstraint() {
    }

    @Override
    public void updateTableConfig(TableConfigBuilder tableConfigBuilder, String columnName) {
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof LiteralConstraint;
    }
  }
}
