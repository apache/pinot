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
package org.apache.pinot.common.request.context;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.AggregationFunctionBinding;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/// Immutable logical argument and result types for one aggregation call. Safe to share across segment threads.
///
/// This describes the original SQL arguments, including on stages that consume an intermediate accumulator.
/// The accumulator type and its serialization remain properties of the bound aggregation implementation.
public final class AggregateCallBinding {
  private final List<ColumnDataType> _argumentTypes;
  private final ColumnDataType _resultType;

  public AggregateCallBinding(List<ColumnDataType> argumentTypes, ColumnDataType resultType) {
    _argumentTypes = List.copyOf(argumentTypes);
    _resultType = Objects.requireNonNull(resultType);
  }

  public List<ColumnDataType> getArgumentTypes() {
    return _argumentTypes;
  }

  public ColumnDataType getResultType() {
    return _resultType;
  }

  public AggregationFunctionBinding toThrift() {
    return new AggregationFunctionBinding(
        _argumentTypes.stream().map(Enum::name).collect(Collectors.toList()), _resultType.name());
  }

  public static AggregateCallBinding fromThrift(AggregationFunctionBinding binding) {
    return new AggregateCallBinding(
        binding.getArgumentTypes().stream().map(ColumnDataType::valueOf).collect(Collectors.toList()),
        ColumnDataType.valueOf(binding.getResultType()));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AggregateCallBinding)) {
      return false;
    }
    AggregateCallBinding that = (AggregateCallBinding) other;
    return _argumentTypes.equals(that._argumentTypes) && _resultType == that._resultType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_argumentTypes, _resultType);
  }
}
