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
package org.apache.pinot.query.validate;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.util.Util;


public class PinotTypeCoercionImpl extends TypeCoercionImpl {
  public PinotTypeCoercionImpl(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  @Override
  public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();

    if (binding.getOperandCount() == 2 && kind.belongsTo(SqlKind.BINARY_ARITHMETIC)) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);

      if ((SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isCharacter(type2)) || (SqlTypeUtil.isCharacter(type1)
          && SqlTypeUtil.isNumeric(type2))) {
        throw binding.newValidationSignatureError();
      }
    }

    return super.binaryArithmeticCoercion(binding);
  }

  @Override
  public boolean binaryComparisonCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    int operandCnt = binding.getOperandCount();

    if (operandCnt == 2 && kind.belongsTo(SqlKind.BINARY_COMPARISON)) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);

      if ((SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isCharacter(type2)) || (SqlTypeUtil.isCharacter(type1)
          && SqlTypeUtil.isNumeric(type2))) {
        throw binding.newValidationSignatureError();
      }
    }

    if (kind == SqlKind.BETWEEN) {
      final List<RelDataType> operandTypes = Util.range(operandCnt).stream()
          .map(binding::getOperandType)
          .collect(Collectors.toList());
      final RelDataType commonType = commonTypeForComparison(operandTypes);
      final boolean hasCharCol = operandTypes.stream().anyMatch(SqlTypeUtil::isCharacter);
      if (commonType != null && SqlTypeUtil.isNumeric(commonType) && hasCharCol) {
        throw binding.newValidationSignatureError();
      }
    }
    return super.binaryComparisonCoercion(binding);
  }
}
