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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Static;


public class BytesCastVisitor extends SqlBasicVisitor<Void> {

  private final SqlValidator _originalValidator;

  public BytesCastVisitor(SqlValidator originalValidator) {
    _originalValidator = originalValidator;
  }

  @Override
  public Void visit(SqlCall call) {
    if (call.getOperator() instanceof SqlCastFunction) {
      List<SqlNode> operands = call.getOperandList();
      SqlNode sqlNode = operands.get(1);
      Preconditions.checkState(sqlNode instanceof SqlDataTypeSpec);
      RelDataType toType = ((SqlDataTypeSpec) sqlNode).deriveType(_originalValidator);
      if (!SqlTypeUtil.isBinary(toType)) {
        return super.visit(call);
      }
      SqlNode srcNode = operands.get(0);
      RelDataType fromType = _originalValidator.getValidatedNodeTypeIfKnown(srcNode);
      if (fromType != null && SqlTypeUtil.isBinary(fromType)) {
        return super.visit(call);
      }
      String message = "Cannot cast " + srcNode + " as " + toType + ".";
      if (srcNode instanceof SqlCharStringLiteral) {
        message += " Try to use binary literal instead (like X" + srcNode + ")";
      } else if (fromType != null && SqlTypeUtil.isCharacter(fromType)) {
        message += " Try to wrap the expression in hexToBytes (like hexToBytes(" + srcNode + "))";
      }
      SqlParserPos pos = call.getParserPosition();
      RuntimeException ex = new InvalidCastException(message);
      throw Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(),
          pos.getEndColumnNum()).ex(ex);
    }
    return super.visit(call);
  }
}
