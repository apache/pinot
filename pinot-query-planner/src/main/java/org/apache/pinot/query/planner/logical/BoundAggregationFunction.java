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
package org.apache.pinot.query.planner.logical;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.common.request.context.AggregateCallBinding;


/// Carries immutable logical aggregation types through physical stage splitting and Rex serialization.
public final class BoundAggregationFunction extends PinotSqlAggFunction {
  private final AggregateCallBinding _binding;

  public BoundAggregationFunction(String name, SqlKind kind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory functionCategory, AggregateCallBinding binding) {
    super(name, kind, returnTypeInference, operandTypeChecker, functionCategory);
    _binding = binding;
  }

  public AggregateCallBinding getBinding() {
    return _binding;
  }
}
