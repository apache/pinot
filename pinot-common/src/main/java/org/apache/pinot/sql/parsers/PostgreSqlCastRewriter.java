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
import org.apache.calcite.sql.SqlNodeList;
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
/// tracked in https://github.com/apache/pinot/issues/19628; until then the grammar accepts `::` only so that this
/// rewriter can give a clear error instead of a parse failure. Rejecting now keeps that open: relaxing a rejection
/// later is backward compatible, while narrowing an accepted type would not be.
///
/// The tree is rewritten in place. A plain [SqlShuttle] is copy-on-write: it rebuilds every ancestor of a replaced
/// node through `SqlOperator.createCall`, and that does not preserve Pinot's own statement classes. A
/// `SqlPhysicalExplain` comes back as a plain `SqlExplain` and a `SqlPinotCreateMaterializedView` as a `SqlBasicCall`,
/// which silently changes how the statement is planned or classified. Here only a constant's direct parent is
/// modified, through `SqlCall.setOperand` or `SqlNodeList.set`, so every other node keeps its identity and class.
/// Every node under which Pinot's grammar accepts an arbitrary expression supports that. The exceptions are positions
/// the grammar restricts: `SqlOrderBy` holds its offset and fetch directly, but they must be numeric, and Pinot's own
/// statement nodes hold queries, identifiers and literals rather than bare expressions. A parent that cannot be
/// updated in place is rejected with a clear error rather than rebuilt.
///
/// Stateless and safe to share across threads. It mutates only the tree passed to `rewrite`, which must be owned by
/// the caller, such as a freshly parsed statement.
final class PostgreSqlCastRewriter extends SqlShuttle {
  private static final PostgreSqlCastRewriter INSTANCE = new PostgreSqlCastRewriter();
  private static final String HEX_PREFIX = "\\x";
  private static final String BYTEA_TYPE_NAME = "BYTEA";

  private PostgreSqlCastRewriter() {
  }

  /// Rewrites `sqlNode` in place and returns it, or returns the replacement literal when `sqlNode` is itself a bytea
  /// constant, as for an expression parsed on its own.
  static SqlNode rewrite(SqlNode sqlNode) {
    return sqlNode.accept(INSTANCE);
  }

  @Override
  public SqlNode visit(SqlNodeList nodeList) {
    for (int i = 0; i < nodeList.size(); i++) {
      SqlNode node = nodeList.get(i);
      if (node != null) {
        SqlNode rewritten = node.accept(this);
        if (rewritten != node) {
          try {
            nodeList.set(i, rewritten);
          } catch (UnsupportedOperationException e) {
            throw unsupportedParent("an immutable node list", e);
          }
        }
      }
    }
    return nodeList;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    List<SqlNode> operands = call.getOperandList();
    for (int i = 0; i < operands.size(); i++) {
      SqlNode operand = operands.get(i);
      if (operand != null) {
        SqlNode rewritten = operand.accept(this);
        if (rewritten != operand) {
          setOperand(call, i, rewritten);
        }
      }
    }
    return rewriteCast(call);
  }

  private static void setOperand(SqlCall parent, int index, SqlNode replacement) {
    try {
      parent.setOperand(index, replacement);
    } catch (UnsupportedOperationException e) {
      throw unsupportedParent(parent.getKind().toString(), e);
    }
  }

  /// Not reachable from Pinot's grammar today. Fails with a clear error if a future node holds a bytea constant where
  /// it cannot be replaced in place, rather than falling back to rebuilding the node.
  private static SqlCompilationException unsupportedParent(String parent, UnsupportedOperationException cause) {
    return new SqlCompilationException(
        "PostgreSQL BYTEA constants are not supported inside " + parent + "; use X'...' instead", cause);
  }

  /// Returns the binary literal that a supported bytea constant normalizes to, or `call` itself when it is not a cast
  /// this rewriter handles. Throws for every other use of the PostgreSQL `::` operator.
  private static SqlNode rewriteCast(SqlCall call) {
    List<SqlNode> operands = call.getOperandList();
    boolean infixCast = call.getOperator() == SqlLibraryOperators.INFIX_CAST;
    if (call.getKind() != SqlKind.CAST || operands.size() != 2 || !(operands.get(1) instanceof SqlDataTypeSpec)) {
      if (infixCast) {
        // The target type did not survive as a type spec, e.g. `col::bytea[1]`, where the item accessor binds
        // tighter than `::`. Reject it here rather than letting the malformed call reach the planner.
        throw new SqlCompilationException("Unsupported PostgreSQL :: cast target in '" + call
            + "'. Note that [] binds tighter than ::, so write CAST(<expr> AS <type>) instead");
      }
      return call;
    }

    SqlDataTypeSpec targetType = (SqlDataTypeSpec) operands.get(1);
    boolean bytea = targetType.getTypeName().isSimple()
        && targetType.getTypeName().getSimple().equalsIgnoreCase(BYTEA_TYPE_NAME);
    if (!bytea) {
      if (infixCast) {
        throw new SqlCompilationException("PostgreSQL-style :: casts are supported only for BYTEA hex constants, "
            + "not for target type '" + targetType.getTypeName() + "'. Use CAST(<expr> AS <type>) instead");
      }
      return call;
    }

    SqlNode source = operands.get(0);
    if (source instanceof SqlBinaryStringLiteral) {
      return source;
    }
    if (!(source instanceof SqlCharStringLiteral)) {
      throw new SqlCompilationException("BYTEA casts are supported only for quoted hex constants such as "
          + "'\\x0102', not for the expression '" + source + "'. To convert hex strings per row, use "
          + "hexToBytes(<expr>), which takes plain hexadecimal digits without the \\x prefix");
    }
    String value = ((SqlCharStringLiteral) source).getValueAs(String.class);
    if (!value.startsWith(HEX_PREFIX)) {
      throw invalidByteaLiteral(value);
    }
    return SqlBinaryStringLiteral.createBinaryString(normalizeHex(value), call.getParserPosition());
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
  /// ASCII whitespace between byte pairs, so a non-ASCII space such as U+205F between them is an error.
  private static boolean isAsciiWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == 0x0B;
  }

  private static SqlCompilationException invalidByteaLiteral(String value) {
    return new SqlCompilationException("Invalid PostgreSQL BYTEA constant '" + value + "': only the hex format is "
        + "supported, so it must begin with \\x and contain complete hexadecimal byte pairs, optionally separated by "
        + "ASCII whitespace");
  }
}
