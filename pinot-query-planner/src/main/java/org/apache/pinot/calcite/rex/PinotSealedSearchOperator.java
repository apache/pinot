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
package org.apache.pinot.calcite.rex;

import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;


/// A `SEARCH(operand, sarg)` whose [Sarg] is hidden from Calcite while the query is optimized.
///
/// Calcite keeps an IN list as one `SEARCH($x, Sarg[...])` call. Rules and metadata handlers (for example predicate
/// inference on joins) rebuild the Sarg's range set every time they simplify a predicate list that holds it. For large
/// lists this makes planning time grow with the list size times the number of rule and metadata calls.
///
/// A call to this operator has exactly one operand, the operand of the `SEARCH`. The Sarg literal is a field of the
/// operator instance, not an operand. The call digest (for example `$SEARCH#0($3)`), `equals` and `hashCode` therefore
/// cost `O(1)`, and no Calcite code can read or rebuild the values. [SearchSealer] creates one instance per distinct
/// Sarg of a query and turns the calls back into `SEARCH` after optimization.
///
/// The operator keeps what Calcite knows about `SEARCH` without looking at the values:
/// - The kind is [SqlKind#OTHER_FUNCTION], not [SqlKind#SEARCH]: Calcite code reads operand 1 of every `SEARCH` call
///   as a Sarg literal.
/// - The return type is `BOOLEAN`. It is nullable only if the operand is nullable and the Sarg treats NULL as
///   UNKNOWN. This is the rule of `SqlSearchOperator`.
/// - The [Strong] policy is `ANY` for `NULL AS UNKNOWN` and `NOT_NULL` otherwise. These are the answers that
///   [Strong#isNull] gives for `SEARCH`, so a null-rejecting filter above an outer join still makes it an inner join.
/// - It is deterministic and safe (it never throws), like `SEARCH`. It is not a dynamic function: Pinot does not
///   relocate dynamic functions (see `PinotRuleUtils#isRelocatable`), and the result only depends on the operand.
///   A call with a literal operand is not folded during optimization. Pinot's optimizer has no `RexExecutor`, so
///   `ReduceExpressionsRule` does not evaluate calls, and `PinotEvaluateLiteralRule` only evaluates registered scalar
///   functions. `RexExpressionUtils` evaluates such a call after optimization, as it does for `SEARCH`.
/// - Equality is identity. [SearchSealer] shares one instance per distinct Sarg within a query.
///
/// Instances are immutable and thread safe.
public final class PinotSealedSearchOperator extends SqlFunction {
  /// Name prefix of all sealed search operators.
  static final String NAME_PREFIX = "$SEARCH#";

  private final RexLiteral _sargLiteral;
  private final Sarg<?> _sarg;

  PinotSealedSearchOperator(int id, RexLiteral sargLiteral) {
    this(id, sargLiteral, sargLiteral.getValueAs(Sarg.class));
  }

  private PinotSealedSearchOperator(int id, RexLiteral sargLiteral, Sarg<?> sarg) {
    super(NAME_PREFIX + id, SqlKind.OTHER_FUNCTION, returnTypeInference(sarg.nullAs), null, OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);
    _sargLiteral = sargLiteral;
    _sarg = sarg;
  }

  private static SqlReturnTypeInference returnTypeInference(RexUnknownAs nullAs) {
    return binding -> {
      RelDataTypeFactory typeFactory = binding.getTypeFactory();
      boolean nullable = nullAs == RexUnknownAs.UNKNOWN && binding.getOperandType(0).isNullable();
      return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullable);
    };
  }

  /// Returns the Sarg literal, which is operand 1 of the `SEARCH` call that this operator seals.
  public RexLiteral getSargLiteral() {
    return _sargLiteral;
  }

  /// Returns the sealed Sarg.
  Sarg<?> getSarg() {
    return _sarg;
  }

  @Override
  public Supplier<Strong.Policy> getStrongPolicyInference() {
    return _sarg.nullAs == RexUnknownAs.UNKNOWN ? () -> Strong.Policy.ANY : () -> Strong.Policy.NOT_NULL;
  }

  @Override
  public Boolean isSafeOperator() {
    return true;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return this == obj;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }
}
