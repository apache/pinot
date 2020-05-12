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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ThetaSketchParams;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.parsers.utils.ParserUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * Implementation of {@link AggregationFunction} to perform the distinct count aggregation using
 * Theta Sketches.
 */
@SuppressWarnings("Duplicates")
public class DistinctCountThetaSketchAggregationFunction implements AggregationFunction<Map<String, Sketch>, Integer> {

  private String _thetaSketchColumn;
  private TransformExpressionTree _thetaSketchIdentifier;
  private Set<String> _predicateStrings;
  private Expression _postAggregationExpression;
  private Set<PredicateInfo> _predicateInfoSet;
  private Map<Expression, String> _expressionMap;
  private ThetaSketchParams _thetaSketchParams;
  private List<TransformExpressionTree> _inputExpressions;

  /**
   * Constructor for the class.
   * @param arguments List of parameters as arguments strings. At least three arguments are expected:
   *                    <ul>
   *                    <li> Required: First expression is interpreted as theta sketch column to aggregate on. </li>
   *                    <li> Required: Second argument is the thetaSketchParams. </li>
   *                    <li> Optional: Third to penultimate are predicates with LHS and RHS. </li>
   *                    <li> Required: Last expression is the one that will be evaluated to compute final result. </li>
   *                    </ul>
   */
  public DistinctCountThetaSketchAggregationFunction(List<String> arguments)
      throws SqlParseException {
    int numExpressions = arguments.size();

    // This function expects at least 3 arguments: Theta Sketch Column, Predicates & final aggregation expression.
    Preconditions.checkArgument(numExpressions >= 3, "DistinctCountThetaSketch expects at least three arguments, got: ",
        numExpressions);

    // Initialize all the internal state.
    init(arguments);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTTHETASKETCH;
  }

  @Override
  public String getColumnName() {
    return AggregationFunctionType.DISTINCTCOUNTTHETASKETCH.getName() + "_" + _thetaSketchColumn;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.DISTINCTCOUNTTHETASKETCH.getName().toLowerCase() + "(" + _thetaSketchColumn + ")";
  }

  @Override
  public List<TransformExpressionTree> getInputExpressions() {
    return _inputExpressions;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {

    Map<String, Union> result = getDefaultResult(aggregationResultHolder, _predicateStrings);
    Sketch[] sketches = deserializeSketches(blockValSetMap.get(_thetaSketchIdentifier).getBytesValuesSV(), length);

    for (PredicateInfo predicateInfo : _predicateInfoSet) {
      String predicate = predicateInfo.getStringVal();

      BlockValSet blockValSet = blockValSetMap.get(predicateInfo.getExpression());
      FieldSpec.DataType valueType = blockValSet.getValueType();
      PredicateEvaluator predicateEvaluator = predicateInfo.getPredicateEvaluator(valueType);

      Union union = result.get(predicate);
      switch (valueType) {
        case INT:
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(longValues[i])) {
              union.update(sketches[i]);
            }
          }
          break;

        case FLOAT:
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(doubleValues[i])) {
              union.update(sketches[i]);
            }
          }
          break;

        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(stringValues[i])) {
              union.update(sketches[i]);
            }
          }
          break;

        default: // Predicates on BYTES is not allowed.
          throw new IllegalStateException("Illegal data type for " + getType() + " aggregation function: " + valueType);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    Sketch[] sketches = deserializeSketches(blockValSetMap.get(_thetaSketchIdentifier).getBytesValuesSV(), length);

    for (PredicateInfo predicateInfo : _predicateInfoSet) {
      String predicate = predicateInfo.getStringVal();

      BlockValSet blockValSet = blockValSetMap.get(predicateInfo.getExpression());
      FieldSpec.DataType valueType = blockValSet.getValueType();
      PredicateEvaluator predicateEvaluator = predicateInfo.getPredicateEvaluator(valueType);

      Map<String, Union> result;
      switch (valueType) {
        case INT:
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(longValues[i])) {
              result = getDefaultResult(groupByResultHolder, groupKeyArray[i], _predicateStrings);
              Union union = result.get(predicate);
              union.update(sketches[i]);
            }
          }
          break;

        case FLOAT:
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(doubleValues[i])) {
              result = getDefaultResult(groupByResultHolder, groupKeyArray[i], _predicateStrings);
              Union union = result.get(predicate);
              union.update(sketches[i]);
            }
          }
          break;

        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();

          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(stringValues[i])) {
              result = getDefaultResult(groupByResultHolder, groupKeyArray[i], _predicateStrings);
              Union union = result.get(predicate);
              union.update(sketches[i]);
            }
          }
          break;

        default: // Predicates on BYTES is not allowed.
          throw new IllegalStateException("Illegal data type for " + getType() + " aggregation function: " + valueType);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    Sketch[] sketches = deserializeSketches(blockValSetMap.get(_thetaSketchIdentifier).getBytesValuesSV(), length);

    for (PredicateInfo predicateInfo : _predicateInfoSet) {
      String predicate = predicateInfo.getStringVal();

      BlockValSet blockValSet = blockValSetMap.get(predicateInfo.getExpression());
      FieldSpec.DataType valueType = blockValSet.getValueType();
      PredicateEvaluator predicateEvaluator = predicateInfo.getPredicateEvaluator(valueType);

      Map<String, Union> result;
      switch (valueType) {
        case INT:
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();

          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(longValues[i])) {

              for (int groupKey : groupKeysArray[i]) {
                result = getDefaultResult(groupByResultHolder, groupKey, _predicateStrings);
                Union union = result.get(predicate);
                union.update(sketches[i]);
              }
            }
          }
          break;

        case FLOAT:
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();

          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(doubleValues[i])) {

              for (int groupKey : groupKeysArray[i]) {
                result = getDefaultResult(groupByResultHolder, groupKey, _predicateStrings);
                Union union = result.get(predicate);
                union.update(sketches[i]);
              }
            }
          }
          break;

        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();

          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(stringValues[i])) {

              for (int groupKey : groupKeysArray[i]) {
                result = getDefaultResult(groupByResultHolder, groupKey, _predicateStrings);
                Union union = result.get(predicate);
                union.update(sketches[i]);
              }
            }
          }
          break;

        default: // Predicates on BYTES is not allowed.
          throw new IllegalStateException("Illegal data type for " + getType() + " aggregation function: " + valueType);
      }
    }
  }

  @Override
  public Map<String, Sketch> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Map<String, Union> result = aggregationResultHolder.getResult();
    if (result == null) {
      result = getDefaultResult(aggregationResultHolder, _predicateStrings);
    }

    return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getResult()));
  }

  @Override
  public Map<String, Sketch> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Map<String, Union> result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      result = getDefaultResult(groupByResultHolder, groupKey, _predicateStrings);
    }

    return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getResult()));
  }

  @Override
  public Map<String, Sketch> merge(Map<String, Sketch> intermediateResult1, Map<String, Sketch> intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    } else if (intermediateResult2 == null) {
      return intermediateResult1;
    }

    for (Map.Entry<String, Sketch> entry : intermediateResult1.entrySet()) {
      String predicate = entry.getKey();
      Union union = getSetOperationBuilder().buildUnion();
      union.update(entry.getValue());
      union.update(intermediateResult2.get(predicate));
      intermediateResult1.put(predicate, union.getResult());
    }
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(Map<String, Sketch> intermediateResult) {
    // Compute the post aggregation expression and return the result.
    Sketch finalSketch = evalPostAggregationExpression(_postAggregationExpression, intermediateResult);
    return (int) Math.round(finalSketch.getEstimate());
  }

  /**
   * Returns the Default result for the given expression.
   *
   * @param aggregationResultHolder Aggregation result holder
   * @param expressions Set of expressions that are expected in the result holder
   * @return Default result
   */
  private Map<String, Union> getDefaultResult(AggregationResultHolder aggregationResultHolder,
      Set<String> expressions) {
    Map<String, Union> result = aggregationResultHolder.getResult();

    if (result == null) {
      result = new HashMap<>();
      aggregationResultHolder.setValue(result);
    }

    for (String expression : expressions) {
      result.putIfAbsent(expression, getSetOperationBuilder().buildUnion());
    }
    return result;
  }

  /**
   * Returns the Default result for the given group key if exists, or creates a new one.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the default result
   * @param expressions Set of expressions that are expected in the result holder
   *
   * @return Default result for the group-key
   */
  private Map<String, Union> getDefaultResult(GroupByResultHolder groupByResultHolder, int groupKey,
      Set<String> expressions) {
    Map<String, Union> result = groupByResultHolder.getResult(groupKey);

    if (result == null) {
      result = new HashMap<>();
      groupByResultHolder.setValueForKey(groupKey, result);
    }

    for (String expression : expressions) {
      result.putIfAbsent(expression, getSetOperationBuilder().buildUnion());
    }
    return result;
  }

  private Sketch[] deserializeSketches(byte[][] serializedSketches, int length) {
    Sketch[] sketches = new Sketch[length];
    for (int i = 0; i < length; i++) {
      sketches[i] = Sketch.wrap(Memory.wrap(serializedSketches[i]));
    }
    return sketches;
  }

  private void init(List<String> arguments)
      throws SqlParseException {
    int numArgs = arguments.size();

    // Predicate Strings are optional. When not specified, they are derived from postAggregationExpression
    boolean predicatesSpecified = numArgs > 3;

    // Initialize the Theta-Sketch Column.
    _thetaSketchColumn = arguments.get(0);
    _thetaSketchIdentifier =
        new TransformExpressionTree(TransformExpressionTree.ExpressionType.IDENTIFIER, _thetaSketchColumn, null);

    // Initialize input expressions. It is expected they are covered between the theta-sketch column and the predicates.
    _inputExpressions = new ArrayList<>();
    _inputExpressions.add(_thetaSketchIdentifier);

    // Initialize thetaSketchParams
    String paramsString = arguments.get(1);
    _thetaSketchParams = ThetaSketchParams.fromString(paramsString);

    String postAggrExpressionString = arguments.get(numArgs - 1);
    _postAggregationExpression = CalciteSqlParser.compileToExpression(postAggrExpressionString);

    _predicateInfoSet = new LinkedHashSet<>();
    _predicateStrings = new LinkedHashSet<>(arguments.subList(2, numArgs - 1));
    _expressionMap = new HashMap<>();

    if (predicatesSpecified) {
      for (String predicateString : _predicateStrings) {
        // FIXME: Standardize predicate string?

        Expression expression = CalciteSqlParser.compileToExpression(predicateString);

        // TODO: Add support for complex predicates with AND/OR.
        String filterColumn = ParserUtils.getFilterColumn(expression);
        Predicate predicate = Predicate
            .newPredicate(ParserUtils.getFilterType(expression), filterColumn, ParserUtils.getFilterValues(expression));
        TransformExpressionTree filterExpression =
            new TransformExpressionTree(TransformExpressionTree.ExpressionType.IDENTIFIER, filterColumn, null);

        _predicateInfoSet.add(new PredicateInfo(predicateString, filterExpression, predicate));
        _expressionMap.put(expression, predicateString);
        _inputExpressions.add(new TransformExpressionTree(new IdentifierAstNode(filterColumn)));
      }
    } else {
      // Auto-derive predicates from postAggregationExpression.
      Set<Expression> predicateExpressions = extractPredicatesFromString(postAggrExpressionString);
      for (Expression predicateExpression : predicateExpressions) {
        String filterColumn = ParserUtils.getFilterColumn(predicateExpression);
        Predicate predicate = Predicate.newPredicate(ParserUtils.getFilterType(predicateExpression), filterColumn,
            ParserUtils.getFilterValues(predicateExpression));
        TransformExpressionTree filterExpression =
            new TransformExpressionTree(TransformExpressionTree.ExpressionType.IDENTIFIER, filterColumn, null);

        String predicateString = ParserUtils.standardizeExpression(predicateExpression, false);
        _predicateStrings.add(predicateString);
        _predicateInfoSet.add(new PredicateInfo(predicateString, filterExpression, predicate));
        _expressionMap.put(predicateExpression, predicateString);
        _inputExpressions.add(filterExpression);
      }
    }
  }

  /**
   * Given a post aggregation String of form like ((p1 and p2) or (p3 and p4)), returns the individual
   * predicates p1, p2, p3, p4.
   *
   * @param postAggrExpressionString Post aggregation expression String input
   * @return Set of predicates that compose the input expression
   * @throws SqlParseException If invalid expression String specified
   */
  private Set<Expression> extractPredicatesFromString(String postAggrExpressionString)
      throws SqlParseException {
    Set<Expression> predicates = new LinkedHashSet<>();
    _postAggregationExpression = CalciteSqlParser.compileToExpression(postAggrExpressionString);
    extractPredicatesFromExpression(_postAggregationExpression, predicates);
    return predicates;
  }

  private void extractPredicatesFromExpression(Expression expression, Set<Expression> predicates) {
    ExpressionType type = expression.getType();

    if (type.equals(ExpressionType.FUNCTION)) {
      Function function = expression.getFunctionCall();
      FilterKind filterKind = FilterKind.valueOf(function.getOperator());

      List<Expression> operands = function.getOperands();
      if (filterKind.equals(FilterKind.AND) || filterKind.equals(FilterKind.OR)) {
        for (Expression operand : operands) {
          extractPredicatesFromExpression(operand, predicates);
        }
      } else {
        predicates.add(expression);
      }
    } // else do nothing
  }

  /**
   * Evaluates the theta-sketch post-aggregation expression, which is composed by performing AND/OR on top of
   * pre-defined predicates. These predicates are evaluated during the aggregation phase, and the cached
   * result is passed to this method to be used when evaluating the expression.
   *
   * @param expression Expression to evaluate, this is built by applying AND/OR on precomputed sketches
   * @param intermediateResult Precomputed sketches for predicates that are part of the expression.
   * @return Overall evaluated sketch for the expression.
   */
  private Sketch evalPostAggregationExpression(Expression expression, Map<String, Sketch> intermediateResult) {
    Function functionCall = expression.getFunctionCall();
    FilterKind kind = FilterKind.valueOf(functionCall.getOperator());
    Sketch result;

    switch (kind) {
      case AND:
        Intersection intersection = getSetOperationBuilder().buildIntersection();
        for (Expression operand : functionCall.getOperands()) {
          intersection.update(evalPostAggregationExpression(operand, intermediateResult));
        }
        result = intersection.getResult();
        break;

      case OR:
        Union union = getSetOperationBuilder().buildUnion();
        for (Expression operand : functionCall.getOperands()) {
          union.update(evalPostAggregationExpression(operand, intermediateResult));
        }
        result = union.getResult();
        break;

      default:
        String predicate = _expressionMap.get(expression);
        result = intermediateResult.get(predicate);
        Preconditions.checkState(result != null, "Precomputed sketch for predicate not provided: " + predicate);
        break;
    }

    return result;
  }

  /**
   * Returns the theta-sketch SetOperation builder properly configured.
   * Currently, only setting of nominalEntries is supported.
   * @return SetOperationBuilder
   */
  private SetOperationBuilder getSetOperationBuilder() {
    return (_thetaSketchParams == null) ? SetOperation.builder()
        : SetOperation.builder().setNominalEntries(_thetaSketchParams.getNominalEntries());
  }

  /**
   * Helper class to store predicate related information:
   * <ul>
   *   <li> String representation of the predicate. </li>
   *   <li> LHS column of the predicate. </li>
   *   <li> Complied {@link Predicate}. </li>
   *   <li> Predicate Evaluator. </li>
   * </ul>
   *
   */
  private static class PredicateInfo {
    private final String _stringVal;
    private final TransformExpressionTree _expression; // LHS
    // FIXME: Predicate does not have equals() and hashCode() implemented
    private final Predicate _predicate;
    private PredicateEvaluator _predicateEvaluator;

    private PredicateInfo(String stringVal, TransformExpressionTree expression, Predicate predicate) {
      _stringVal = stringVal;
      _expression = expression;
      _predicate = predicate;
      _predicateEvaluator = null; // Initialized lazily
    }

    public String getStringVal() {
      return _stringVal;
    }

    public TransformExpressionTree getExpression() {
      return _expression;
    }

    public Predicate getPredicate() {
      return _predicate;
    }

    /**
     * Since PredicateEvaluator requires data-type, it is initialized lazily.
     *
     * @param dataType Data type for RHS of the predicate
     * @return Predicate Evaluator
     */
    public PredicateEvaluator getPredicateEvaluator(FieldSpec.DataType dataType) {
      if (_predicateEvaluator != null) {
        return _predicateEvaluator;
      }

      // Theta-sketch does not work on INT and FLOAT.
      if (dataType == FieldSpec.DataType.INT) {
        dataType = FieldSpec.DataType.LONG;
      } else if (dataType == FieldSpec.DataType.FLOAT) {
        dataType = FieldSpec.DataType.DOUBLE;
      }

      _predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(_predicate, null, dataType);
      return _predicateEvaluator;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof PredicateInfo)) {
        return false;
      }

      PredicateInfo that = (PredicateInfo) o;
      return Objects.equals(_stringVal, that._stringVal) && Objects.equals(_expression, that._expression) && Objects
          .equals(_predicate, that._predicate);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_stringVal, _expression, _predicate);
    }
  }
}
