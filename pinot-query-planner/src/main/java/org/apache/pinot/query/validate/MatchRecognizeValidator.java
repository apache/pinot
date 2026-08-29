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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Pinot specific validation and rewriting of SQL:2016 `MATCH_RECOGNIZE` (row pattern recognition) clauses.
///
/// This visitor runs on the **raw parsed** [SqlNode] tree, before Calcite's own validation and before
/// [org.apache.calcite.sql2rel.SqlToRelConverter] runs. Running this early is deliberate and load bearing:
///
///  1. **The `AFTER MATCH` default must be corrected at the `SqlNode` level.** When the `AFTER MATCH` clause is
///     omitted, the parser leaves [SqlMatchRecognize#getAfter()] as `null`, and `SqlToRelConverter` then silently
///     substitutes [SqlMatchRecognize.AfterOption#SKIP_TO_NEXT_ROW]. SQL:2016 (and Trino, Snowflake and Oracle) all
///     default to `SKIP PAST LAST ROW`. The two differ in whether matches may overlap, so a query ported from
///     another engine would silently return different rows. Conversion erases the omitted-vs-explicit distinction,
///     so the fix has to happen here. See [#applyStandardAfterMatchDefault].
///  2. **Unsupported constructs must be rejected before conversion.** Several of them (`ORDER BY` / `PARTITION BY`
///     on arbitrary expressions in particular) blow up inside `SqlToRelConverter` with an `AssertionError` or
///     `ClassCastException` rather than a usable message.
///
/// Everything this class rejects is deferred, not permanently unsupported; each message names the construct so the
/// user can rewrite the query.
///
/// This class is stateless apart from the tree it is walking and is **not** thread-safe; create a new instance per
/// query, exactly like [RowExpressionValidationVisitor].
public class MatchRecognizeValidator extends SqlBasicVisitor<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MatchRecognizeValidator.class);

  private static final String PERMUTE = "PERMUTE";
  private static final String PERMUTE_NOT_SUPPORTED =
      "PERMUTE is not supported yet in the MATCH_RECOGNIZE PATTERN clause. Expand the permutation into an explicit "
          + "alternation, e.g. PATTERN ((A B) | (B A)).";

  /// Whether the node currently being visited sits inside a MEASURES or DEFINE list, the only two places where the
  /// `RUNNING` / `FINAL` semantics modifiers are legal.
  private boolean _inMeasureOrDefine;
  private boolean _matchRecognizeFound;

  /// Whether the visited SQL tree contains at least one MATCH_RECOGNIZE clause.
  public boolean hasMatchRecognize() {
    return _matchRecognizeFound;
  }

  @Override
  public Void visit(SqlCall call) {
    if (call instanceof SqlMatchRecognize) {
      visitMatchRecognize((SqlMatchRecognize) call);
      return null;
    }
    SqlKind kind = call.getKind();
    if ((kind == SqlKind.RUNNING || kind == SqlKind.FINAL) && !_inMeasureOrDefine) {
      // The RUNNING / FINAL prefix operators are registered for MATCH_RECOGNIZE, but unlike PREV / NEXT / FIRST /
      // LAST / CLASSIFIER / MATCH_NUMBER, Calcite does not confine them to a MatchRecognizeScope. Reject them here
      // so they cannot leak into an ordinary projection.
      throw unsupported(kind + " can only be used in the MEASURES or DEFINE clause of a MATCH_RECOGNIZE query: '"
          + call + "'.");
    }
    return super.visit(call);
  }

  private void visitMatchRecognize(SqlMatchRecognize match) {
    _matchRecognizeFound = true;
    validateAndRewrite(match);
    // Only MEASURES and DEFINE may contain RUNNING / FINAL. Every other operand has already been checked by
    // validateAndRewrite(), so the only one left to descend into is the table reference, which may itself hold a
    // nested MATCH_RECOGNIZE.
    visitIn(match.getMeasureList(), true);
    visitIn(match.getPatternDefList(), true);
    visitIn(match.getTableRef(), false);
  }

  private void visitIn(SqlNode node, boolean inMeasureOrDefine) {
    boolean saved = _inMeasureOrDefine;
    _inMeasureOrDefine = inMeasureOrDefine;
    try {
      node.accept(this);
    } finally {
      _inMeasureOrDefine = saved;
    }
  }

  private void validateAndRewrite(SqlMatchRecognize match) {
    rejectUnsupportedClauses(match);
    validatePartitionBy(match.getPartitionList());
    validateOrderBy(match.getOrderList());
    Set<String> definedVariables = definedVariables(match.getPatternDefList());
    validatePattern(match.getPattern(), definedVariables);
    validateDefinitions(match.getPatternDefList());
    // Deliberately last: a query that also uses a deferred construct should be told about that construct first.
    validateMeasures(match);
    rejectRowSourceAliasCollision(match, definedVariables);
    applyStandardAfterMatchDefault(match);
    warnOnUndefinedPatternVariables(match, definedVariables);
  }

  /// Rewrites an omitted `AFTER MATCH` clause into an explicit `AFTER MATCH SKIP PAST LAST ROW`.
  ///
  /// Calcite's `SqlToRelConverter` defaults a `null` `AFTER` operand to `SKIP TO NEXT ROW`, which produces
  /// overlapping matches. SQL:2016 mandates `SKIP PAST LAST ROW` (non-overlapping matches), and that is what
  /// Trino, Snowflake and Oracle do. An explicitly written `AFTER MATCH SKIP TO NEXT ROW` is left untouched: the
  /// parser only leaves the operand `null` when the clause is absent from the query text, so the omitted and the
  /// explicit case are perfectly distinguishable here.
  private void applyStandardAfterMatchDefault(SqlMatchRecognize match) {
    if (match.getAfter() != null) {
      return;
    }
    match.setOperand(SqlMatchRecognize.OPERAND_AFTER,
        SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW.symbol(match.getParserPosition()));
  }

  private void rejectUnsupportedClauses(SqlMatchRecognize match) {
    SqlLiteral rowsPerMatch = match.getRowsPerMatch();
    if (rowsPerMatch != null
        && rowsPerMatch.getValue() == SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS) {
      throw unsupported("ALL ROWS PER MATCH is not supported yet in MATCH_RECOGNIZE. Use ONE ROW PER MATCH (the "
          + "default) and expose the per-match values you need through the MEASURES clause.");
    }
    if (!isEmpty(match.getSubsetList())) {
      throw unsupported("SUBSET is not supported yet in MATCH_RECOGNIZE. Remove the SUBSET clause and reference the "
          + "individual pattern variables directly.");
    }
    if (match.getInterval() != null) {
      throw unsupported("WITHIN is not supported yet in MATCH_RECOGNIZE. Remove the WITHIN clause and bound the "
          + "match with a predicate in the DEFINE clause instead.");
    }
  }

  /// Rejects the two `MEASURES` shapes whose Calcite row type is not `partition columns ++ measures`, which is what
  /// `MatchNode` and `MatchOperator` assume. Both plan without complaint today and only fail on the server, with a
  /// message describing an internal invariant rather than the user's mistake.
  ///
  ///  1. **No `MEASURES` clause at all.** `SqlValidatorImpl.validateMatchRecognize` ends with
  ///     `if (measureList.size() == 0) ns.setType(<the table reference row type>)`, i.e. the whole *input* row type,
  ///     which is not the `ONE ROW PER MATCH` shape at all. `MEASURES` is optional in SQL:2016 and in Trino, so this
  ///     is ordinary valid SQL; we do not own the Calcite code, so rejecting is the only sound option until
  ///     measures-less `MATCH_RECOGNIZE` is implemented properly.
  ///  2. **An alias that collides with a `PARTITION BY` column or with an earlier measure alias.** Calcite adds each
  ///     measure to the row type only `if (!typeBuilder.nameExists(name))`, so a colliding alias is dropped
  ///     altogether and the query plans with a column missing.
  ///
  /// The name comparison is case-sensitive on purpose, because `RelDataTypeFactory.Builder#nameExists` is:
  /// `PARTITION BY col1 ... MEASURES LAST(x) AS COL1` really does produce two distinct columns and must keep working.
  private void validateMeasures(SqlMatchRecognize match) {
    SqlNodeList measureList = match.getMeasureList();
    if (isEmpty(measureList)) {
      throw unsupported("MATCH_RECOGNIZE requires a MEASURES clause. ONE ROW PER MATCH emits the PARTITION BY "
          + "columns plus the MEASURES, so add at least one measure, e.g. MEASURES MATCH_NUMBER() AS mno.");
    }
    Set<String> names = new LinkedHashSet<>();
    for (SqlNode partitionKey : match.getPartitionList()) {
      // validatePartitionBy() already established that every partition key is a plain identifier.
      names.add(lastName((SqlIdentifier) partitionKey));
    }
    for (SqlNode measure : measureList) {
      SqlNode alias = asOperand(measure, 1);
      if (alias instanceof SqlIdentifier && ((SqlIdentifier) alias).isSimple()
          && !names.add(((SqlIdentifier) alias).getSimple())) {
        throw unsupported("MATCH_RECOGNIZE measure alias '" + ((SqlIdentifier) alias).getSimple() + "' collides with "
            + "a PARTITION BY column or an earlier measure alias. Calcite drops the duplicate from the output row "
            + "type, which would silently lose a column; rename the measure.");
      }
    }
  }

  /// Rejects a pattern variable whose name equals the row source alias.
  ///
  /// Calcite has no separate representation for the SQL:2016 universal row pattern variable: it qualifies an
  /// unqualified column reference in `MEASURES` / `DEFINE` with the row source alias, and
  /// `RelToPlanNodeConverter#bindPatternFieldRefs` recognises the universal variable purely by that alpha not being a
  /// pattern variable name. When the two names coincide, `LAST(col)` silently becomes "last row mapped to that
  /// variable" instead of "last row of the match", and an unqualified reference in a `DEFINE` predicate changes which
  /// rows match at all. Nothing is left at the `RexNode` level to disambiguate, so it has to be caught here.
  ///
  /// The comparison is case-sensitive because the symbol table is a plain `Map<String, Integer>` lookup:
  /// `FROM a AS aa ... PATTERN (AA B+)` binds to the universal variable correctly today and must keep working.
  private void rejectRowSourceAliasCollision(SqlMatchRecognize match, Set<String> definedVariables) {
    String alias = SqlValidatorUtil.alias(match.getTableRef());
    if (alias == null) {
      // A join or a sub-query with no derivable alias; Calcite does not qualify anything with a row source alias.
      return;
    }
    Set<String> variables = new LinkedHashSet<>(definedVariables);
    collectPatternVariables(match.getPattern(), variables);
    if (variables.contains(alias)) {
      throw unsupported("MATCH_RECOGNIZE pattern variable '" + alias + "' collides with the row source alias '"
          + alias + "'. Calcite qualifies unqualified column references in MEASURES and DEFINE with the row source "
          + "alias, so they would silently bind to that pattern variable instead of to the universal row pattern "
          + "variable. Rename the pattern variable, or alias the input table differently.");
    }
  }

  private static String lastName(SqlIdentifier identifier) {
    return identifier.names.get(identifier.names.size() - 1);
  }

  private void validatePartitionBy(SqlNodeList partitionList) {
    for (SqlNode partitionKey : partitionList) {
      if (!(partitionKey instanceof SqlIdentifier)) {
        throw unsupported("PARTITION BY on expressions is not supported yet in MATCH_RECOGNIZE: '" + partitionKey
            + "'. Only plain column references are supported. Compute the expression in a sub-query and partition "
            + "by the resulting column.");
      }
    }
  }

  private void validateOrderBy(SqlNodeList orderList) {
    if (isEmpty(orderList)) {
      throw unsupported("MATCH_RECOGNIZE requires an ORDER BY clause. Row pattern recognition is only well defined "
          + "over an ordered sequence of rows, so add ORDER BY <column> inside the MATCH_RECOGNIZE clause.");
    }
    for (SqlNode orderKey : orderList) {
      SqlNode key = orderKey;
      SqlKind kind = key.getKind();
      if (kind == SqlKind.NULLS_FIRST || kind == SqlKind.NULLS_LAST) {
        throw unsupported("NULLS FIRST / NULLS LAST is not supported yet in the MATCH_RECOGNIZE ORDER BY clause: '"
            + orderKey + "'. Remove the null ordering, or filter the null values out before the MATCH_RECOGNIZE "
            + "clause.");
      }
      if (kind == SqlKind.DESCENDING) {
        key = ((SqlCall) key).operand(0);
      }
      if (!(key instanceof SqlIdentifier)) {
        throw unsupported("ORDER BY on expressions is not supported yet in MATCH_RECOGNIZE: '" + orderKey + "'. Only "
            + "plain column references, optionally followed by ASC or DESC, are supported. Compute the expression in "
            + "a sub-query and order by the resulting column.");
      }
    }
  }

  private void validatePattern(SqlNode pattern, Set<String> definedVariables) {
    if (pattern instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) pattern;
      // PERMUTE is a non-reserved keyword, so `PATTERN (PERMUTE(A))` parses as the concatenation of a pattern
      // variable named PERMUTE and the group (A) instead of as a permutation. Rejecting an undefined PERMUTE
      // variable turns that silently wrong plan into an explicit error. A variable that is actually DEFINEd is a
      // legitimate (if unfortunate) name and is left alone.
      if (identifier.isSimple() && PERMUTE.equalsIgnoreCase(identifier.getSimple())
          && !definedVariables.contains(identifier.getSimple())) {
        throw unsupported(PERMUTE_NOT_SUPPORTED);
      }
      return;
    }
    if (!(pattern instanceof SqlCall)) {
      return;
    }
    SqlCall call = (SqlCall) pattern;
    SqlKind kind = call.getKind();
    if (kind == SqlKind.PATTERN_PERMUTE) {
      throw unsupported(PERMUTE_NOT_SUPPORTED);
    }
    if (kind == SqlKind.PATTERN_EXCLUDED) {
      throw unsupported("Pattern exclusions '{- -}' are not supported yet in the MATCH_RECOGNIZE PATTERN clause. "
          + "Remove the exclusion; note that exclusions only affect ALL ROWS PER MATCH output, which is also not "
          + "supported yet.");
    }
    for (SqlNode operand : call.getOperandList()) {
      if (operand != null) {
        validatePattern(operand, definedVariables);
      }
    }
  }

  /// Rejects aggregate calls inside `DEFINE`. Aggregates are legal in SQL:2016 `DEFINE` (they aggregate over the
  /// rows matched so far), but Pinot does not implement running aggregation inside the pattern matcher yet.
  private void validateDefinitions(SqlNodeList patternDefList) {
    for (SqlNode definition : patternDefList) {
      SqlNode condition = patternDefinitionCondition(definition);
      if (condition != null) {
        rejectAggregates(condition, definition);
      }
    }
  }

  private void rejectAggregates(SqlNode node, SqlNode definition) {
    if (!(node instanceof SqlCall)) {
      return;
    }
    SqlCall call = (SqlCall) node;
    if (isAggregate(call.getOperator())) {
      throw unsupported("Aggregate function '" + call.getOperator().getName() + "' is not supported yet in the "
          + "MATCH_RECOGNIZE DEFINE clause: '" + definition + "'. Only row level predicates, optionally using "
          + "PREV / NEXT / FIRST / LAST / CLASSIFIER / MATCH_NUMBER, are supported.");
    }
    for (SqlNode operand : call.getOperandList()) {
      if (operand != null) {
        rejectAggregates(operand, definition);
      }
    }
  }

  /// Warns about pattern variables that appear in `PATTERN` but are never `DEFINE`d. SQL:2016 says such a variable
  /// matches every row (its condition defaults to `TRUE`), so this is legal, but it is far more often a typo.
  private void warnOnUndefinedPatternVariables(SqlMatchRecognize match, Set<String> definedVariables) {
    Set<String> undefinedVariables = new LinkedHashSet<>();
    collectPatternVariables(match.getPattern(), undefinedVariables);
    undefinedVariables.removeAll(definedVariables);
    if (!undefinedVariables.isEmpty()) {
      LOGGER.warn("MATCH_RECOGNIZE pattern variable(s) {} are used in PATTERN but never DEFINEd. Per SQL:2016 they "
          + "match every row (their condition defaults to TRUE), which is usually an unintended typo. Query pattern: "
          + "{}", undefinedVariables, match.getPattern());
    }
  }

  private static Set<String> definedVariables(SqlNodeList patternDefList) {
    Set<String> definedVariables = new LinkedHashSet<>();
    for (SqlNode definition : patternDefList) {
      SqlIdentifier variable = patternDefinitionVariable(definition);
      if (variable != null && variable.isSimple()) {
        definedVariables.add(variable.getSimple());
      }
    }
    return definedVariables;
  }

  private void collectPatternVariables(SqlNode pattern, Set<String> variables) {
    if (pattern instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) pattern;
      if (identifier.isSimple()) {
        variables.add(identifier.getSimple());
      }
      return;
    }
    if (!(pattern instanceof SqlCall)) {
      // Quantifier bounds and the reluctant flag are literals; they are not pattern variables.
      return;
    }
    for (SqlNode operand : ((SqlCall) pattern).getOperandList()) {
      if (operand != null) {
        collectPatternVariables(operand, variables);
      }
    }
  }

  /// Returns the condition of a `DEFINE` item. The parser represents `DEFINE var AS condition` as
  /// `AS(condition, var)`.
  @Nullable
  private static SqlNode patternDefinitionCondition(SqlNode definition) {
    return asOperand(definition, 0);
  }

  /// Returns the variable of a `DEFINE` item. The parser represents `DEFINE var AS condition` as `AS(condition, var)`.
  @Nullable
  private static SqlIdentifier patternDefinitionVariable(SqlNode definition) {
    SqlNode variable = asOperand(definition, 1);
    return variable instanceof SqlIdentifier ? (SqlIdentifier) variable : null;
  }

  @Nullable
  private static SqlNode asOperand(SqlNode definition, int index) {
    if (definition.getKind() != SqlKind.AS) {
      return null;
    }
    List<SqlNode> operands = ((SqlCall) definition).getOperandList();
    return operands.size() > index ? operands.get(index) : null;
  }

  /// The parser half-resolves calls against [org.apache.calcite.sql.fun.SqlStdOperatorTable], so standard aggregates
  /// such as `SUM` and `COUNT` already carry a [SqlAggFunction] here. Pinot specific aggregates (`DISTINCTCOUNT`,
  /// `PERCENTILE...`, ...) are still [SqlUnresolvedFunction] at this point and are matched by name instead.
  private static boolean isAggregate(SqlOperator operator) {
    if (operator instanceof SqlAggFunction) {
      return true;
    }
    return operator instanceof SqlUnresolvedFunction
        && AggregationFunctionType.isAggregationFunction(operator.getName());
  }

  private static boolean isEmpty(@Nullable SqlNodeList nodeList) {
    return nodeList == null || nodeList.isEmpty();
  }

  private static UnsupportedMatchRecognizeException unsupported(String message) {
    return new UnsupportedMatchRecognizeException(message);
  }

  /// Thrown when a `MATCH_RECOGNIZE` query uses a construct that Pinot does not support yet. The message always names
  /// the construct and suggests a rewrite.
  public static class UnsupportedMatchRecognizeException extends RuntimeException {
    public UnsupportedMatchRecognizeException(String message) {
      super(message);
    }
  }
}
