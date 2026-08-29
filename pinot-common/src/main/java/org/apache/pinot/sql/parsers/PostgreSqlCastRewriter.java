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


/// Normalizes PostgreSQL hex-format BYTEA constants (`'\x0102'::bytea` and `CAST('\x0102' AS BYTEA)`) to Calcite
/// binary literals, so both query engines see the same representation they would get from SQL `X'0102'`. Only quoted
/// constants are normalized: a dynamic `STRING`-to-`BYTES` conversion would have to be implemented per row in each
/// engine, and the two would be easy to drift apart.
///
/// Every other use of the PostgreSQL `::` operator is rejected here. That is a deliberate scope limit, not a
/// technical one — `SqlLibraryOperators.INFIX_CAST` is an ordinary `SqlCastOperator` of kind
/// [org.apache.calcite.sql.SqlKind#CAST], so `intCol::double` would in fact plan and run like `CAST(intCol AS
/// DOUBLE)`. Supporting the full operator means committing to `::` type-name semantics across both engines and is
/// left to a follow-up; until then the grammar accepts `::` only so that this rewriter can give a clear error
/// instead of a parse failure.
///
/// Stateless and safe to share; `rewrite` uses a single immutable instance.
final class PostgreSqlCastRewriter extends SqlShuttle {
  private static final PostgreSqlCastRewriter INSTANCE = new PostgreSqlCastRewriter();
  private static final String HEX_PREFIX = "\\x";
  private static final String BYTEA_TYPE_NAME = "BYTEA";

  private PostgreSqlCastRewriter() {
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
      if (infixCast) {
        // The target type did not survive as a type spec, e.g. `col::bytea[1]`, where the item accessor binds
        // tighter than `::`. Reject it here rather than letting the malformed call reach the planner.
        throw new SqlCompilationException("Unsupported PostgreSQL :: cast target in '" + visitedCall
            + "'. Note that [] binds tighter than ::, so write CAST(<expr> AS <type>) instead");
      }
      return visitedCall;
    }

    SqlDataTypeSpec targetType = (SqlDataTypeSpec) operands.get(1);
    boolean bytea = targetType.getTypeName().isSimple()
        && targetType.getTypeName().getSimple().equalsIgnoreCase(BYTEA_TYPE_NAME);
    if (!bytea) {
      if (infixCast) {
        throw new SqlCompilationException("PostgreSQL-style :: casts are supported only for BYTEA hex constants, "
            + "not for target type '" + targetType.getTypeName() + "'. Use CAST(<expr> AS <type>) instead");
      }
      return visitedCall;
    }

    SqlNode source = operands.get(0);
    if (source instanceof SqlBinaryStringLiteral) {
      return source;
    }
    if (!(source instanceof SqlCharStringLiteral)) {
      throw new SqlCompilationException("BYTEA casts are supported only for quoted hex constants such as "
          + "'\\x0102', not for the expression '" + source + "'");
    }
    String value = ((SqlCharStringLiteral) source).getValueAs(String.class);
    if (!value.startsWith(HEX_PREFIX)) {
      throw invalidByteaLiteral(value);
    }
    return SqlBinaryStringLiteral.createBinaryString(normalizeHex(value), visitedCall.getParserPosition());
  }

  /// Decodes the digits after the leading `\x`. PostgreSQL allows whitespace between byte pairs but not inside one.
  private static String normalizeHex(String literal) {
    String value = literal.substring(HEX_PREFIX.length());
    StringBuilder hex = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (isAsciiWhitespace(c)) {
        if ((hex.length() & 1) != 0) {
          throw invalidByteaLiteral(literal);
        }
      } else if (isAsciiHexDigit(c)) {
        hex.append(c);
      } else {
        throw invalidByteaLiteral(literal);
      }
    }
    if ((hex.length() & 1) != 0) {
      throw invalidByteaLiteral(literal);
    }
    return hex.toString();
  }

  /// Deliberately not `Character.digit(c, 16)`: that also accepts full-width and non-Latin digits such as `Ａ` and
  /// `١`, which PostgreSQL rejects.
  private static boolean isAsciiHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
  }

  /// Deliberately not `Character.isWhitespace(c)`, for the same reason as [#isAsciiHexDigit]: PostgreSQL only skips
  /// ASCII whitespace between byte pairs, so `'\x01 02'` is an error rather than `0x0102`.
  private static boolean isAsciiWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == 0x0B;
  }

  private static SqlCompilationException invalidByteaLiteral(String value) {
    return new SqlCompilationException("Invalid PostgreSQL BYTEA hex constant '" + value + "': it must begin with "
        + "\\x and contain complete hexadecimal byte pairs, optionally separated by ASCII whitespace");
  }
}
