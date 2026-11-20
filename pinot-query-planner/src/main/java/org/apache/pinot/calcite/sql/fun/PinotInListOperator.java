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
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;


/// Marks an `IN` or `NOT IN` call with a large value list while the query is converted to relational algebra.
///
/// `SqlToRelConverter` registers every `IN` call as a sub-query and, because Pinot sets `inSubQueryThreshold` to
/// `Integer.MAX_VALUE`, expands a value list into `x = v1 OR x = v2 OR ...`. Simplifying this OR costs `O(N log N)` in
/// a WHERE clause and much more elsewhere (CASE, SELECT list, aggregate FILTER), because `RexSimplify` then simplifies
/// every term against the negation of all earlier terms.
///
/// After validation, `SearchSealer#markInLists` sets this operator on large `IN` calls in place. This keeps the node
/// identity, so the types that the validator derived stay valid. Its kind is not `IN`, so `SqlToRelConverter` does not
/// expand the list. It converts the call with `PinotConvertletTable`, which builds one `SEARCH`. The original
/// operator is set again after the conversion, so the rest of Pinot never sees this operator.
///
/// The name, precedence, return type and unparse are the ones of the original operator. `SqlNode#equalsDeep` compares
/// operators by name, so a marked expression still matches the same expression in the GROUP BY clause.
///
/// The two instances are immutable and thread safe.
public final class PinotInListOperator extends SqlSpecialOperator {
  public static final PinotInListOperator IN = new PinotInListOperator(SqlStdOperatorTable.IN);
  public static final PinotInListOperator NOT_IN = new PinotInListOperator(SqlStdOperatorTable.NOT_IN);

  private final SqlBinaryOperator _original;

  private PinotInListOperator(SqlBinaryOperator original) {
    super(original.getName(), SqlKind.OTHER_FUNCTION, original.getLeftPrec(), true, ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN, null);
    _original = original;
  }

  /// Returns the marker for an `IN` or `NOT IN` call.
  public static PinotInListOperator of(SqlKind kind) {
    switch (kind) {
      case IN:
        return IN;
      case NOT_IN:
        return NOT_IN;
      default:
        throw new IllegalArgumentException("Not an IN list kind: " + kind);
    }
  }

  /// Returns whether this operator marks a `NOT IN` call.
  public boolean isNegated() {
    return this == NOT_IN;
  }

  /// Returns the operator that this operator replaces.
  public SqlBinaryOperator getOriginal() {
    return _original;
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return _original.deriveType(validator, scope, call);
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    _original.unparse(writer, call, leftPrec, rightPrec);
  }
}
