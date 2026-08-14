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
package org.apache.pinot.sql.parsers;

import java.util.List;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.util.SqlShuttle;


/// Normalizes PostgreSQL hex-format BYTEA literals to Calcite binary literals before validation or Pinot query
/// conversion. This deliberately supports only quoted constants so it cannot introduce engine-specific per-row
/// STRING-to-BYTES cast behavior.
final class PostgreSqlByteaLiteralRewriter extends SqlShuttle {
  private static final PostgreSqlByteaLiteralRewriter INSTANCE = new PostgreSqlByteaLiteralRewriter();
  private static final String HEX_PREFIX = "\\x";

  private PostgreSqlByteaLiteralRewriter() {
  }

  static SqlNode rewrite(SqlNode sqlNode) {
    return sqlNode.accept(INSTANCE);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    SqlNode visitedNode = super.visit(call);
    if (!(visitedNode instanceof SqlCall)) {
      return visitedNode;
    }
    SqlCall visitedCall = (SqlCall) visitedNode;
    List<SqlNode> operands = visitedCall.getOperandList();
    boolean infixCast = visitedCall.getOperator() == SqlLibraryOperators.INFIX_CAST;
    if (visitedCall.getKind() != SqlKind.CAST || operands.size() != 2
        || !(operands.get(1) instanceof SqlDataTypeSpec)) {
      return visitedCall;
    }

    SqlDataTypeSpec targetType = (SqlDataTypeSpec) operands.get(1);
    boolean bytea = targetType.getTypeName().isSimple()
        && targetType.getTypeName().getSimple().equalsIgnoreCase("BYTEA");
    if (!bytea) {
      if (infixCast) {
        throw new SqlCompilationException("PostgreSQL-style :: casts are only supported for BYTEA hex literals");
      }
      return visitedCall;
    }

    SqlNode source = operands.get(0);
    if (source instanceof SqlBinaryStringLiteral) {
      return source;
    }
    if (!(source instanceof SqlCharStringLiteral)) {
      throw invalidByteaLiteral();
    }
    String value = ((SqlCharStringLiteral) source).getNlsString().getValue();
    if (!value.startsWith(HEX_PREFIX)) {
      throw invalidByteaLiteral();
    }
    return SqlBinaryStringLiteral.createBinaryString(normalizeHex(value.substring(HEX_PREFIX.length())),
        visitedCall.getParserPosition());
  }

  private static String normalizeHex(String value) {
    StringBuilder hex = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (Character.isWhitespace(c)) {
        if ((hex.length() & 1) != 0) {
          throw invalidByteaLiteral();
        }
      } else if (Character.digit(c, 16) >= 0) {
        hex.append(c);
      } else {
        throw invalidByteaLiteral();
      }
    }
    if ((hex.length() & 1) != 0) {
      throw invalidByteaLiteral();
    }
    return hex.toString();
  }

  private static SqlCompilationException invalidByteaLiteral() {
    return new SqlCompilationException(
        "PostgreSQL BYTEA hex literals must be quoted, begin with \\x, and contain complete hexadecimal byte pairs");
  }
}
