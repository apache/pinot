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
package org.apache.pinot.common.function.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;


/**
 * Pinot custom SqlFunction to be registered into SqlOperatorTable.
 */
public class PinotSqlFunction extends SqlFunction {
  private final boolean _deterministic;

  public PinotSqlFunction(String name, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, boolean deterministic) {
    super(name.toUpperCase(), SqlKind.OTHER_FUNCTION, returnTypeInference, null, operandTypeChecker,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    _deterministic = deterministic;
  }

  public PinotSqlFunction(String name, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    this(name, returnTypeInference, operandTypeChecker, true);
  }

  @Override
  public boolean isDeterministic() {
    return _deterministic;
  }
}
