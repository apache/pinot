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
package org.apache.calcite.sql;

import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Pinot SqlAggFunction class to register the Pinot aggregation functions with the Calcite operator table.
 */
public class PinotSqlAggFunction extends SqlAggFunction {

  public PinotSqlAggFunction(String name, @Nullable SqlIdentifier sqlIdentifier, SqlKind sqlKind,
      SqlReturnTypeInference returnTypeInference, @Nullable SqlOperandTypeInference sqlOperandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory sqlFunctionCategory) {
    this(name, sqlIdentifier, sqlKind, returnTypeInference, sqlOperandTypeInference, operandTypeChecker,
        sqlFunctionCategory, false, false, Optionality.FORBIDDEN);
  }

  public PinotSqlAggFunction(String name, @Nullable SqlIdentifier sqlIdentifier, SqlKind sqlKind,
      SqlReturnTypeInference returnTypeInference, @Nullable SqlOperandTypeInference sqlOperandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory sqlFunctionCategory,
      boolean requiresOrder, boolean requiresOver, Optionality optionality) {
    super(name, sqlIdentifier, sqlKind, returnTypeInference, sqlOperandTypeInference, operandTypeChecker,
        sqlFunctionCategory, requiresOrder, requiresOver, optionality);
  }
}
