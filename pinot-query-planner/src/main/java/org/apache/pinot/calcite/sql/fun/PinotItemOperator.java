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
package org.apache.pinot.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlItemOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Static;
import org.apache.pinot.query.type.OpenStructSqlType;


/// The SQL `ITEM` operator (`x[i]`, `m['k']`), typed for OPEN_STRUCT columns.
///
/// Every operand type behaves as Calcite's standard ITEM. When the operand is an [OpenStructSqlType] and the key is a
/// literal, the result is the key's declared type, so `v['n'] > 50` and `SUM(v['n'])` are numeric when `n` is
/// declared numeric; an undeclared key is VARCHAR. Either way the result is nullable, since any row may lack the key.
/// This matches the single-stage engine, which reads a declared key with its declared type and an undeclared key as
/// STRING. The key must be a literal: the leaf stage resolves it once per segment, so `col[otherCol]` is rejected at
/// validation. Stateless and thread-safe.
public class PinotItemOperator extends SqlItemOperator {
  public static final PinotItemOperator INSTANCE = new PinotItemOperator();

  private PinotItemOperator() {
    // Calcite's ITEM is built with a single-operand checker (the operand-type rules for MAP, ARRAY and ROW).
    super("ITEM", (SqlSingleOperandTypeChecker) SqlStdOperatorTable.ITEM.getOperandTypeChecker(), 0, false);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    if (callBinding.getOperandType(0) instanceof OpenStructSqlType && !callBinding.isOperandLiteral(1, false)) {
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(callBinding.operand(1),
            Static.RESOURCE.argumentMustBeLiteral(getName()));
      }
      return false;
    }
    return super.checkOperandTypes(callBinding, throwOnFailure);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    if (opBinding.getOperandType(0) instanceof OpenStructSqlType openStruct) {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      String key = opBinding.isOperandLiteral(1, false) ? opBinding.getOperandLiteralValue(1, String.class) : null;
      RelDataType declared = key == null ? null : openStruct.getDeclaredKeyType(key);
      RelDataType type = declared != null ? declared : typeFactory.createSqlType(SqlTypeName.VARCHAR);
      return typeFactory.createTypeWithNullability(type, true);
    }
    return super.inferReturnType(opBinding);
  }
}
