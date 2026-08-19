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
package org.apache.pinot.query.runtime.operator.match;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.exception.QueryErrorCode;


/// A compiled MEASURES item or DEFINE predicate, evaluated against the state of one match.
///
/// ## How it reuses Pinot's row expression evaluation
///
/// A MATCH_RECOGNIZE expression such as `PREV(A.price, 1) > B.price * 2` is only *partly* match specific:
/// the navigations `PREV(A.price, 1)` and `B.price` depend on the match, but the `>` and the
/// `*` are ordinary scalar operators. Compilation therefore splits the expression in two:
/// 1. every maximal match specific sub-expression - a navigation, `CLASSIFIER()`, `MATCH_NUMBER()` or
///    a single variable aggregate - becomes a [MatchTerm] bound to a slot of a synthetic row;
/// 2. what is left is a plain [RexExpression] over those slots, compiled by
///    [TransformOperandFactory] exactly like any other Pinot row expression.
///
/// The example compiles to slots `[PREV(A.price,1), B.price]` plus the operand `$0 > $1 * 2`. Comparisons,
/// boolean connectives, arithmetic, casts, null semantics and every scalar UDF therefore behave exactly as they do
/// elsewhere in the multi-stage engine, and this class does not reimplement any of them.
///
/// `RUNNING` and `FINAL` are stripped during compilation: with ONE ROW PER MATCH the measures are
/// computed once the match is complete, and running semantics evaluated at the last row of a match coincide with final
/// semantics. Both modifiers therefore have no effect and are unwrapped rather than rejected.
///
/// Not thread safe: the slot array is reused between evaluations, so one instance belongs to one operator.
public class MatchExpression {
  /// Navigation and match state functions, as Calcite names them in the converted expression.
  private static final String PREV = "PREV";
  private static final String NEXT = "NEXT";
  private static final String FIRST = "FIRST";
  private static final String LAST = "LAST";
  private static final String CLASSIFIER = "CLASSIFIER";
  private static final String MATCH_NUMBER = "MATCH_NUMBER";
  private static final String RUNNING = "RUNNING";
  private static final String FINAL = "FINAL";
  private static final Set<String> NAVIGATION_FUNCTIONS = Set.of(PREV, NEXT, FIRST, LAST);
  private static final Set<String> MATCH_FUNCTIONS =
      Set.of(PREV, NEXT, FIRST, LAST, CLASSIFIER, MATCH_NUMBER, RUNNING, FINAL);

  private final TransformOperand _operand;
  private final List<MatchTerm> _terms;
  private final Object[] _slots;

  private MatchExpression(TransformOperand operand, List<MatchTerm> terms) {
    _operand = operand;
    _terms = terms;
    _slots = new Object[terms.size()];
  }

  /// Compiles `expression`, which addresses columns of `inputSchema` through pattern field references.
  ///
  /// @throws org.apache.pinot.spi.exception.QueryException if the expression uses a construct that is not supported
  ///         yet. Nothing is dropped or approximated, because either would return wrong rows instead of an error.
  public static MatchExpression compile(RexExpression expression, DataSchema inputSchema) {
    List<MatchTerm> terms = new ArrayList<>();
    RexExpression slotExpression = rewrite(expression, inputSchema, terms);
    int numTerms = terms.size();
    String[] slotNames = new String[numTerms];
    ColumnDataType[] slotTypes = new ColumnDataType[numTerms];
    for (int i = 0; i < numTerms; i++) {
      slotNames[i] = "$" + i;
      slotTypes[i] = terms.get(i).getResultType();
    }
    TransformOperand operand =
        TransformOperandFactory.getTransformOperand(slotExpression, new DataSchema(slotNames, slotTypes));
    return new MatchExpression(operand, terms);
  }

  /// Evaluates this expression against the current state of `tape`.
  @Nullable
  public Object evaluate(MatchTape tape) {
    for (int i = 0; i < _terms.size(); i++) {
      _slots[i] = _terms.get(i).evaluate(tape);
    }
    return _operand.apply(_slots);
  }

  /// Evaluates this expression as a DEFINE predicate. SQL three valued logic collapses to false here: a row is mapped
  /// to a pattern variable only if its condition is definitely true.
  public boolean test(MatchTape tape) {
    Object value = evaluate(tape);
    if (value == null) {
      return false;
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    return ((Number) value).intValue() != 0;
  }

  /// Replaces every match specific sub-expression of `expression` with an input reference to a freshly appended
  /// slot of `terms`, and returns the resulting expression over the synthetic slot row.
  private static RexExpression rewrite(RexExpression expression, DataSchema inputSchema, List<MatchTerm> terms) {
    if (expression instanceof RexExpression.PatternFieldRef) {
      // A bare column reference such as `A.price` is `LAST(A.price, 0)` per SQL:2016.
      RexExpression.PatternFieldRef ref = (RexExpression.PatternFieldRef) expression;
      return addTerm(terms, new MatchTerm.Navigation(ref.getSymbolOrdinal(), true, 0, 0, ref.getIndex(),
          columnType(inputSchema, ref)));
    }
    if (expression instanceof RexExpression.InputRef) {
      // Column references inside MEASURES / DEFINE always arrive as pattern field references. A plain input
      // reference would index the synthetic slot row instead of the input row, so refuse it rather than read the
      // wrong column.
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "Unsupported column reference in a MATCH_RECOGNIZE MEASURES or DEFINE expression: " + expression
              + ". Qualify the column with a pattern variable, e.g. 'A.column'.");
    }
    if (!(expression instanceof RexExpression.FunctionCall)) {
      return expression;
    }

    RexExpression.FunctionCall call = (RexExpression.FunctionCall) expression;
    String functionName = call.getFunctionName();
    if (RUNNING.equals(functionName) || FINAL.equals(functionName)) {
      return rewrite(singleOperand(call), inputSchema, terms);
    }
    if (CLASSIFIER.equals(functionName)) {
      if (!call.getFunctionOperands().isEmpty()) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "CLASSIFIER with a pattern variable argument is not supported yet in MATCH_RECOGNIZE. Use the no "
                + "argument form CLASSIFIER().");
      }
      return addTerm(terms, MatchTerm.Classifier.INSTANCE);
    }
    if (MATCH_NUMBER.equals(functionName)) {
      return addTerm(terms, MatchTerm.MatchNumber.INSTANCE);
    }
    if (NAVIGATION_FUNCTIONS.contains(functionName)) {
      // Calcite distributes navigation over an expression, so `LAST(A.price * 2)` arrives as
      // `LAST(A.price, 0) * LAST(2, 0)`. Navigating a constant is the constant itself.
      if (!containsPatternFieldRef(call)) {
        return rewrite(singleOperand(call), inputSchema, terms);
      }
      return addTerm(terms, navigation(call));
    }
    if (AggregationFunctionType.isAggregationFunction(functionName)) {
      return addTerm(terms, aggregate(call, inputSchema));
    }

    List<RexExpression> operands = call.getFunctionOperands();
    List<RexExpression> rewritten = new ArrayList<>(operands.size());
    for (RexExpression operand : operands) {
      rewritten.add(rewrite(operand, inputSchema, terms));
    }
    return new RexExpression.FunctionCall(call.getDataType(), functionName, rewritten, call.isDistinct(),
        call.isIgnoreNulls());
  }

  private static RexExpression addTerm(List<MatchTerm> terms, MatchTerm term) {
    terms.add(term);
    return new RexExpression.InputRef(terms.size() - 1);
  }

  /// Flattens a nest of navigation calls such as `PREV(LAST(A.price, 1), 2)` into a single
  /// [MatchTerm.Navigation]: at most one logical step (`FIRST` / `LAST`) selects a row of the match,
  /// and the `PREV` / `NEXT` offsets around it add up into one physical delta.
  private static MatchTerm.Navigation navigation(RexExpression.FunctionCall call) {
    boolean fromEnd = true;
    int logicalOffset = 0;
    boolean logicalSeen = false;
    int physicalDelta = 0;
    RexExpression current = call;
    while (current instanceof RexExpression.FunctionCall) {
      RexExpression.FunctionCall currentCall = (RexExpression.FunctionCall) current;
      String functionName = currentCall.getFunctionName();
      if (RUNNING.equals(functionName) || FINAL.equals(functionName)) {
        current = singleOperand(currentCall);
        continue;
      }
      if (!NAVIGATION_FUNCTIONS.contains(functionName)) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "Unsupported expression inside a MATCH_RECOGNIZE row pattern navigation: '" + currentCall
                + "'. Only a column reference qualified by a pattern variable, optionally wrapped in "
                + "FIRST / LAST / PREV / NEXT, is supported.");
      }
      int offset = navigationOffset(currentCall);
      if (PREV.equals(functionName)) {
        physicalDelta -= offset;
      } else if (NEXT.equals(functionName)) {
        physicalDelta += offset;
      } else {
        if (logicalSeen) {
          throw QueryErrorCode.QUERY_EXECUTION.asException(
              "Nested FIRST / LAST is not supported in MATCH_RECOGNIZE: '" + call + "'.");
        }
        logicalSeen = true;
        fromEnd = LAST.equals(functionName);
        logicalOffset = offset;
      }
      current = currentCall.getFunctionOperands().get(0);
    }
    if (!(current instanceof RexExpression.PatternFieldRef)) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "Unsupported operand of a MATCH_RECOGNIZE row pattern navigation: '" + call
              + "'. Expecting a column reference qualified by a pattern variable.");
    }
    RexExpression.PatternFieldRef ref = (RexExpression.PatternFieldRef) current;
    return new MatchTerm.Navigation(ref.getSymbolOrdinal(), fromEnd, logicalOffset, physicalDelta, ref.getIndex(),
        call.getDataType());
  }

  /// The offset operand of a navigation call. `PREV` and `NEXT` default to one row, `FIRST` and
  /// `LAST` to the first / last row itself.
  private static int navigationOffset(RexExpression.FunctionCall call) {
    List<RexExpression> operands = call.getFunctionOperands();
    if (operands.size() < 2) {
      String functionName = call.getFunctionName();
      return PREV.equals(functionName) || NEXT.equals(functionName) ? 1 : 0;
    }
    RexExpression offset = operands.get(1);
    if (!(offset instanceof RexExpression.Literal)) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "The offset of a MATCH_RECOGNIZE row pattern navigation must be a constant, got: '" + offset + "'.");
    }
    Object value = ((RexExpression.Literal) offset).getValue();
    if (!(value instanceof Number)) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "The offset of a MATCH_RECOGNIZE row pattern navigation must be an integer, got: '" + value + "'.");
    }
    int intValue = ((Number) value).intValue();
    if (intValue < 0) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "The offset of a MATCH_RECOGNIZE row pattern navigation must not be negative, got: " + intValue + ".");
    }
    return intValue;
  }

  /// Compiles a single variable aggregate. The aggregated expression is compiled against the input schema so that it
  /// can be evaluated per row of the match, which is why `SUM(A.price * A.quantity)` works.
  private static MatchTerm.Aggregate aggregate(RexExpression.FunctionCall call, DataSchema inputSchema) {
    String functionName = call.getFunctionName();
    if (call.isDistinct()) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "DISTINCT is not supported in a MATCH_RECOGNIZE MEASURES aggregate: '" + call + "'.");
    }
    MatchTerm.Aggregate.Kind kind = MatchTerm.Aggregate.Kind.resolveOrThrow(functionName);
    List<RexExpression> operands = call.getFunctionOperands();
    if (operands.isEmpty()) {
      if (kind != MatchTerm.Aggregate.Kind.COUNT) {
        throw QueryErrorCode.QUERY_EXECUTION.asException(
            "Aggregate '" + functionName + "' requires an argument in a MATCH_RECOGNIZE MEASURES clause.");
      }
      // COUNT(*) counts every row of the match, whatever variable it is mapped to.
      return new MatchTerm.Aggregate(kind, RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL, null,
          call.getDataType());
    }
    if (operands.size() != 1) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "Multi argument aggregate '" + functionName + "' is not supported in a MATCH_RECOGNIZE MEASURES clause.");
    }
    RexExpression argument = operands.get(0);
    if (containsMatchFunction(argument)) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "Row pattern navigation inside a MATCH_RECOGNIZE MEASURES aggregate is not supported yet: '" + call + "'.");
    }
    IntSet symbolOrdinals = new IntOpenHashSet();
    collectSymbolOrdinals(argument, symbolOrdinals);
    if (symbolOrdinals.size() > 1) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "Only single variable aggregates are supported in a MATCH_RECOGNIZE MEASURES clause, but '" + call
              + "' references " + symbolOrdinals.size() + " pattern variables.");
    }
    int symbolOrdinal = symbolOrdinals.isEmpty() ? RexExpression.PatternFieldRef.UNIVERSAL_SYMBOL_ORDINAL
        : symbolOrdinals.iterator().nextInt();
    TransformOperand argumentOperand =
        TransformOperandFactory.getTransformOperand(toRowExpression(argument), inputSchema);
    return new MatchTerm.Aggregate(kind, symbolOrdinal, argumentOperand, call.getDataType());
  }

  /// Rewrites pattern field references into plain input references so the expression can be evaluated against a single
  /// input row.
  private static RexExpression toRowExpression(RexExpression expression) {
    if (expression instanceof RexExpression.PatternFieldRef) {
      return new RexExpression.InputRef(((RexExpression.PatternFieldRef) expression).getIndex());
    }
    if (!(expression instanceof RexExpression.FunctionCall)) {
      return expression;
    }
    RexExpression.FunctionCall call = (RexExpression.FunctionCall) expression;
    List<RexExpression> operands = call.getFunctionOperands();
    List<RexExpression> rewritten = new ArrayList<>(operands.size());
    for (RexExpression operand : operands) {
      rewritten.add(toRowExpression(operand));
    }
    return new RexExpression.FunctionCall(call.getDataType(), call.getFunctionName(), rewritten, call.isDistinct(),
        call.isIgnoreNulls());
  }

  private static void collectSymbolOrdinals(RexExpression expression, IntSet symbolOrdinals) {
    if (expression instanceof RexExpression.PatternFieldRef) {
      symbolOrdinals.add(((RexExpression.PatternFieldRef) expression).getSymbolOrdinal());
    } else if (expression instanceof RexExpression.FunctionCall) {
      for (RexExpression operand : ((RexExpression.FunctionCall) expression).getFunctionOperands()) {
        collectSymbolOrdinals(operand, symbolOrdinals);
      }
    }
  }

  private static boolean containsPatternFieldRef(RexExpression expression) {
    if (expression instanceof RexExpression.PatternFieldRef) {
      return true;
    }
    if (expression instanceof RexExpression.FunctionCall) {
      for (RexExpression operand : ((RexExpression.FunctionCall) expression).getFunctionOperands()) {
        if (containsPatternFieldRef(operand)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsMatchFunction(RexExpression expression) {
    if (!(expression instanceof RexExpression.FunctionCall)) {
      return false;
    }
    RexExpression.FunctionCall call = (RexExpression.FunctionCall) expression;
    if (MATCH_FUNCTIONS.contains(call.getFunctionName())) {
      return true;
    }
    for (RexExpression operand : call.getFunctionOperands()) {
      if (containsMatchFunction(operand)) {
        return true;
      }
    }
    return false;
  }

  private static RexExpression singleOperand(RexExpression.FunctionCall call) {
    List<RexExpression> operands = call.getFunctionOperands();
    if (operands.isEmpty()) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "Expecting an operand in MATCH_RECOGNIZE expression: '" + call + "'.");
    }
    return operands.get(0);
  }

  /// Type of the column a bare pattern field reference reads. Unlike a navigation call, a bare reference carries no
  /// declared type of its own.
  private static ColumnDataType columnType(DataSchema inputSchema, RexExpression.PatternFieldRef ref) {
    int index = ref.getIndex();
    if (index < 0 || index >= inputSchema.size()) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(
          "MATCH_RECOGNIZE pattern field reference '" + ref + "' is out of range for input schema " + inputSchema);
    }
    return inputSchema.getColumnDataType(index);
  }
}
