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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.MapUtils;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.AnotB;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.RawThetaSketchAggregationFunction.Parameters;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * Implementation of {@link AggregationFunction} to perform the distinct count aggregation using
 * Theta Sketches.
 * <p>TODO: For performance concern, use {@code List<Sketch>} as the intermediate result.
 */
public class DistinctCountThetaSketchAggregationFunction implements AggregationFunction<Map<String, Sketch>, Long> {

  public enum MergeFunction {
    SET_UNION, SET_INTERSECT, SET_DIFF;

    public static final ImmutableList<String> STRING_VALUES =
        ImmutableList.of(SET_UNION.name(), SET_INTERSECT.name(), SET_DIFF.name());

    public static final String CSV_VALUES = String.join(",", STRING_VALUES);

    public static boolean isValid(String name) {
      return SET_UNION.name().equalsIgnoreCase(name) || SET_INTERSECT.name().equalsIgnoreCase(name) || SET_DIFF.name()
          .equalsIgnoreCase(name);
    }
  }

  private static final Pattern ARGUMENT_SUBSTITUTION = Pattern.compile("\\$(\\d+)");

  private final ExpressionContext _thetaSketchColumn;
  private final SetOperationBuilder _setOperationBuilder;
  private final List<ExpressionContext> _inputExpressions;
  private final ExpressionContext _postAggregationExpression;
  private final List<Predicate> _predicates;
  private final Map<Predicate, PredicateInfo> _predicateInfoMap;

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
  public DistinctCountThetaSketchAggregationFunction(List<ExpressionContext> arguments)
      throws SqlParseException {
    int numArguments = arguments.size();

    // NOTE: This function expects at least 4 arguments: theta-sketch column, nominalEntries, predicate(s), post-aggregation expression.
    Preconditions.checkArgument(numArguments > 3,
        "DistinctCountThetaSketch expects at least four arguments (theta-sketch column, parameter(s), post-aggregation expression), got: ",
        numArguments);

    // Initialize the theta-sketch column
    _thetaSketchColumn = arguments.get(0);
    Preconditions.checkArgument(_thetaSketchColumn.getType() == ExpressionContext.Type.IDENTIFIER,
        "First argument of DistinctCountThetaSketch must be identifier (theta-sketch column)");

    // Initialize the theta-sketch parameters
    ExpressionContext parametersExpression = arguments.get(1);
    Preconditions.checkArgument(parametersExpression.getType() == ExpressionContext.Type.LITERAL,
        "Second argument of DistinctCountThetaSketch must be literal (parameters)");
    Parameters parameters = new Parameters(parametersExpression.getLiteral());

    // Initialize the theta-sketch set operation builder
    _setOperationBuilder = new SetOperationBuilder().setNominalEntries(parameters.getNominalEntries());

    // Index of the original input predicates
    // This list is zero indexed, whereas argument substitution is 1-indexed: index[0] = $1
    _predicates = new ArrayList<>();

    // Initialize the input expressions
    // NOTE: It is expected to cover the theta-sketch column and the lhs of the predicates.
    _inputExpressions = new ArrayList<>();
    _inputExpressions.add(_thetaSketchColumn);

    // Initialize the post-aggregation expression
    // NOTE: It is modeled as a filter
    ExpressionContext postAggregationExpression = arguments.get(numArguments - 1);
    Preconditions.checkArgument(parametersExpression.getType() == ExpressionContext.Type.LITERAL,
        "Last argument of DistinctCountThetaSketch must be literal (post-aggregation expression)");
    _postAggregationExpression = QueryContextConverterUtils
        .getExpression(CalciteSqlParser.compileToExpression(postAggregationExpression.getLiteral()));

    // Initialize the predicate map
    _predicateInfoMap = new HashMap<>();

    // Predicates are explicitly specified
    for (int i = 2; i < numArguments - 1; i++) {
      ExpressionContext predicateExpression = arguments.get(i);
      Preconditions.checkArgument(predicateExpression.getType() == ExpressionContext.Type.LITERAL,
          "Third to second last argument of DistinctCountThetaSketch must be literal (predicate expression)");
      Predicate predicate = getPredicate(predicateExpression.getLiteral());
      _inputExpressions.add(predicate.getLhs());
      _predicates.add(predicate);
      _predicateInfoMap.put(predicate, new PredicateInfo(predicate));
    }

    // First expression is the nominal entries parameter
    validatePostAggregationExpression(_postAggregationExpression, _predicates.size());
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
  public List<ExpressionContext> getInputExpressions() {
    return _inputExpressions;
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
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    Map<Predicate, Union> unionMap = getUnionMap(aggregationResultHolder);

    Sketch[] sketches = deserializeSketches(blockValSetMap.get(_thetaSketchColumn).getBytesValuesSV(), length);
    for (PredicateInfo predicateInfo : _predicateInfoMap.values()) {
      Predicate predicate = predicateInfo.getPredicate();
      BlockValSet blockValSet = blockValSetMap.get(predicate.getLhs());
      FieldSpec.DataType valueType = blockValSet.getValueType();
      PredicateEvaluator predicateEvaluator = predicateInfo.getPredicateEvaluator(valueType);

      Union union = unionMap.get(predicate);
      switch (valueType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(intValues[i])) {
              union.update(sketches[i]);
            }
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(longValues[i])) {
              union.update(sketches[i]);
            }
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(floatValues[i])) {
              union.update(sketches[i]);
            }
          }
          break;
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
        case BYTES:
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(bytesValues[i])) {
              union.update(sketches[i]);
            }
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    Sketch[] sketches = deserializeSketches(blockValSetMap.get(_thetaSketchColumn).getBytesValuesSV(), length);
    for (PredicateInfo predicateInfo : _predicateInfoMap.values()) {
      Predicate predicate = predicateInfo.getPredicate();
      BlockValSet blockValSet = blockValSetMap.get(predicate.getLhs());
      FieldSpec.DataType valueType = blockValSet.getValueType();
      PredicateEvaluator predicateEvaluator = predicateInfo.getPredicateEvaluator(valueType);

      switch (valueType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(intValues[i])) {
              getUnionMap(groupByResultHolder, groupKeyArray[i]).get(predicate).update(sketches[i]);
            }
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(longValues[i])) {
              getUnionMap(groupByResultHolder, groupKeyArray[i]).get(predicate).update(sketches[i]);
            }
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(floatValues[i])) {
              getUnionMap(groupByResultHolder, groupKeyArray[i]).get(predicate).update(sketches[i]);
            }
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(doubleValues[i])) {
              getUnionMap(groupByResultHolder, groupKeyArray[i]).get(predicate).update(sketches[i]);
            }
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(stringValues[i])) {
              getUnionMap(groupByResultHolder, groupKeyArray[i]).get(predicate).update(sketches[i]);
            }
          }
          break;
        case BYTES:
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(bytesValues[i])) {
              getUnionMap(groupByResultHolder, groupKeyArray[i]).get(predicate).update(sketches[i]);
            }
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    Sketch[] sketches = deserializeSketches(blockValSetMap.get(_thetaSketchColumn).getBytesValuesSV(), length);
    for (PredicateInfo predicateInfo : _predicateInfoMap.values()) {
      Predicate predicate = predicateInfo.getPredicate();
      BlockValSet blockValSet = blockValSetMap.get(predicate.getLhs());
      FieldSpec.DataType valueType = blockValSet.getValueType();
      PredicateEvaluator predicateEvaluator = predicateInfo.getPredicateEvaluator(valueType);

      switch (valueType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(intValues[i])) {
              for (int groupKey : groupKeysArray[i]) {
                getUnionMap(groupByResultHolder, groupKey).get(predicate).update(sketches[i]);
              }
            }
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(longValues[i])) {
              for (int groupKey : groupKeysArray[i]) {
                getUnionMap(groupByResultHolder, groupKey).get(predicate).update(sketches[i]);
              }
            }
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(floatValues[i])) {
              for (int groupKey : groupKeysArray[i]) {
                getUnionMap(groupByResultHolder, groupKey).get(predicate).update(sketches[i]);
              }
            }
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(doubleValues[i])) {
              for (int groupKey : groupKeysArray[i]) {
                getUnionMap(groupByResultHolder, groupKey).get(predicate).update(sketches[i]);
              }
            }
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(stringValues[i])) {
              for (int groupKey : groupKeysArray[i]) {
                getUnionMap(groupByResultHolder, groupKey).get(predicate).update(sketches[i]);
              }
            }
          }
          break;
        case BYTES:
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            if (predicateEvaluator.applySV(bytesValues[i])) {
              for (int groupKey : groupKeysArray[i]) {
                getUnionMap(groupByResultHolder, groupKey).get(predicate).update(sketches[i]);
              }
            }
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public Map<String, Sketch> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Map<Predicate, Union> unionMap = aggregationResultHolder.getResult();
    if (unionMap == null) {
      return Collections.emptyMap();
    }

    Map<String, Sketch> result = new HashMap<>();
    for (PredicateInfo predicateInfo : _predicateInfoMap.values()) {
      Sketch sketch = unionMap.get(predicateInfo.getPredicate()).getResult();

      // Skip empty sketches, as they lead to unnecessary unions (and cost performance)
      if (!sketch.isEmpty()) {
        result.put(predicateInfo.getStringPredicate(), sketch);
      }
    }
    return result;
  }

  @Override
  public Map<String, Sketch> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Map<Predicate, Union> unionMap = groupByResultHolder.getResult(groupKey);
    if (unionMap == null) {
      return Collections.emptyMap();
    }

    Map<String, Sketch> result = new HashMap<>();
    for (PredicateInfo predicateInfo : _predicateInfoMap.values()) {
      Sketch sketch = unionMap.get(predicateInfo.getPredicate()).getResult();

      // Skip empty sketches, as they lead to unnecessary unions (and cost performance)
      if (!sketch.isEmpty()) {
        result.put(predicateInfo.getStringPredicate(), sketch);
      }
    }
    return result;
  }

  @Override
  public Map<String, Sketch> merge(Map<String, Sketch> intermediateResult1, Map<String, Sketch> intermediateResult2) {
    if (MapUtils.isEmpty(intermediateResult1)) {
      return intermediateResult2;
    }
    if (MapUtils.isEmpty(intermediateResult2)) {
      return intermediateResult1;
    }

    // Add sketches from intermediateResult1, merged with overlapping ones from intermediateResult2
    Map<String, Sketch> mergedResult = new HashMap<>();
    for (Map.Entry<String, Sketch> entry : intermediateResult1.entrySet()) {
      String predicate = entry.getKey();
      Sketch sketch = intermediateResult2.get(predicate);
      if (sketch != null) {
        // Merge the overlapping ones
        Union union = _setOperationBuilder.buildUnion();
        union.update(entry.getValue());
        union.update(sketch);
        mergedResult.put(predicate, union.getResult());
      } else {
        // Collect the non-overlapping ones
        mergedResult.put(predicate, entry.getValue());
      }
    }

    // Add sketches that are only in intermediateResult2
    for (Map.Entry<String, Sketch> entry : intermediateResult2.entrySet()) {
      // If key already present, it was already merged in the previous iteration.
      mergedResult.putIfAbsent(entry.getKey(), entry.getValue());
    }

    return mergedResult;
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
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(Map<String, Sketch> intermediateResult) {
    Sketch finalSketch = extractFinalSketch(intermediateResult);
    return finalSketch != null ? Math.round(finalSketch.getEstimate()) : 0;
  }

  private Predicate getPredicate(String predicateString) {
    FilterContext filter;
    try {
      filter = QueryContextConverterUtils.getFilter(CalciteSqlParser.compileToExpression(predicateString));
    } catch (SqlParseException e) {
      throw new IllegalArgumentException("Invalid predicate string: " + predicateString);
    }
    // TODO: Add support for complex predicates with AND/OR.
    Preconditions.checkArgument(filter.getType() == FilterContext.Type.PREDICATE, "Invalid predicate string: %s",
        predicateString);
    return filter.getPredicate();
  }

  private Map<Predicate, Union> getUnionMap(AggregationResultHolder aggregationResultHolder) {
    Map<Predicate, Union> unionMap = aggregationResultHolder.getResult();
    if (unionMap == null) {
      unionMap = getDefaultUnionMap();
      aggregationResultHolder.setValue(unionMap);
    }
    return unionMap;
  }

  private Map<Predicate, Union> getUnionMap(GroupByResultHolder groupByResultHolder, int groupKey) {
    Map<Predicate, Union> unionMap = groupByResultHolder.getResult(groupKey);
    if (unionMap == null) {
      unionMap = getDefaultUnionMap();
      groupByResultHolder.setValueForKey(groupKey, unionMap);
    }
    return unionMap;
  }

  private Map<Predicate, Union> getDefaultUnionMap() {
    Map<Predicate, Union> unionMap = new HashMap<>();
    for (Predicate predicate : _predicateInfoMap.keySet()) {
      unionMap.put(predicate, _setOperationBuilder.buildUnion());
    }
    return unionMap;
  }

  private Sketch[] deserializeSketches(byte[][] serializedSketches, int length) {
    Sketch[] sketches = new Sketch[length];
    for (int i = 0; i < length; i++) {
      sketches[i] = Sketch.wrap(Memory.wrap(serializedSketches[i]));
    }
    return sketches;
  }

  /**
   * Evaluates the theta-sketch post-aggregation expression, which is composed by performing AND/OR on top of the
   * pre-defined predicates. These predicates are evaluated during the aggregation phase, and the cached results are
   * passed to this method to be used when evaluating the expression.
   *
   * @param postAggregationExpression Post-aggregation expression to evaluate (modeled as a filter)
   * @param sketchMap Precomputed sketches for predicates that are part of the expression.
   * @return Overall evaluated sketch for the expression.
   */
  private Sketch evalPostAggregationExpression(ExpressionContext postAggregationExpression,
      Map<Predicate, Sketch> sketchMap) {
    if (postAggregationExpression.getType() == ExpressionContext.Type.LITERAL) {
      throw new IllegalArgumentException("Literal not supported in post-aggregation function");
    }

    if (postAggregationExpression.getType() == ExpressionContext.Type.IDENTIFIER) {
      final Predicate exp = _predicates.get(extractSubstitutionPosition(postAggregationExpression.getLiteral()) - 1);
      return sketchMap.get(exp);
    }

    // shouldn't throw exception because of the validation in the constructor
    MergeFunction func = MergeFunction.valueOf(postAggregationExpression.getFunction().getFunctionName().toUpperCase());

    // handle functions recursively
    switch (func) {
      case SET_UNION:
        Union union = _setOperationBuilder.buildUnion();
        for (ExpressionContext exp : postAggregationExpression.getFunction().getArguments()) {
          union.update(evalPostAggregationExpression(exp, sketchMap));
        }
        return union.getResult();
      case SET_INTERSECT:
        Intersection intersection = _setOperationBuilder.buildIntersection();
        for (ExpressionContext exp : postAggregationExpression.getFunction().getArguments()) {
          intersection.update(evalPostAggregationExpression(exp, sketchMap));
        }
        return intersection.getResult();
      case SET_DIFF:
        List<ExpressionContext> args = postAggregationExpression.getFunction().getArguments();
        AnotB diff = _setOperationBuilder.buildANotB();
        Sketch a = evalPostAggregationExpression(args.get(0), sketchMap);
        Sketch b = evalPostAggregationExpression(args.get(1), sketchMap);
        diff.update(a, b);
        return diff.getResult();
      default:
        throw new IllegalStateException(String.format("Invalid post-aggregation function: %s",
            postAggregationExpression.getFunction().getFunctionName().toUpperCase()));
    }
  }

  /**
   * Extracts the final sketch from the intermediate result by applying the post-aggregation expression on it.
   *
   * @param intermediateResult Intermediate result
   * @return Final Sketch obtained by computing the post-aggregation expression on intermediate result
   */
  protected Sketch extractFinalSketch(Map<String, Sketch> intermediateResult) {
    Map<Predicate, Sketch> sketchMap = new HashMap<>();
    for (Map.Entry<String, Sketch> entry : intermediateResult.entrySet()) {
      Predicate predicate = getPredicate(entry.getKey());
      sketchMap.put(predicate, entry.getValue());
    }
    return evalPostAggregationExpression(_postAggregationExpression, sketchMap);
  }

  /**
   * Validates that the function context's substitution parameters ($1, $2, etc) does not exceed the number
   * of predicates passed into the post-aggregation function.
   *
   * For example, if the post aggregation function is:
   * INTERSECT($1, $2, $3)
   * But there are only 2 arguments passed into the aggregation function, throw an error
   *
   * SET_DIFF should contain exactly 2 arguments - throw an error otherwise.
   * SET_UNION and SET_INTERSECT should contain 2 or more arguments - throw an error otherwise.
   *
   * @param context The parsed function context that's a tree structure
   * @param numPredicates Max number of predicates available to be substituted
   */
  private static void validatePostAggregationExpression(ExpressionContext context, int numPredicates) {
    if (context.getType() == ExpressionContext.Type.LITERAL) {
      throw new IllegalArgumentException("Invalid post-aggregation function expression syntax.");
    }

    if (context.getType() == ExpressionContext.Type.IDENTIFIER) {
      int id = extractSubstitutionPosition(context.getIdentifier());
      if (id <= 0) {
        throw new IllegalArgumentException("Argument substitution starts at $1");
      }
      if (id > numPredicates) {
        throw new IllegalArgumentException("Argument substitution exceeded number of predicates");
      }
      // if none of the invalid conditions are met above, exit out early
      return;
    }

    if (!MergeFunction.isValid(context.getFunction().getFunctionName())) {
      throw new IllegalArgumentException(
          String.format("Invalid Theta Sketch aggregation function. Allowed: [%s]", MergeFunction.CSV_VALUES));
    }

    switch (MergeFunction.valueOf(context.getFunction().getFunctionName().toUpperCase())) {
      case SET_DIFF:
        // set diff can only have 2 arguments
        if (context.getFunction().getArguments().size() != 2) {
          throw new IllegalArgumentException("SET_DIFF function can only have 2 arguments.");
        }
        validatePostAggregationExpression(context.getFunction().getArguments().get(0), numPredicates);
        validatePostAggregationExpression(context.getFunction().getArguments().get(1), numPredicates);
        break;
      case SET_UNION:
      case SET_INTERSECT:
        if (context.getFunction().getArguments().size() < 2) {
          throw new IllegalArgumentException("SET_UNION and SET_INTERSECT should have at least 2 arguments.");
        }
        for (ExpressionContext arg : context.getFunction().getArguments()) {
          validatePostAggregationExpression(arg, numPredicates);
        }
        break;
      default:
        throw new IllegalStateException("Invalid merge function");
    }
  }

  private static int extractSubstitutionPosition(String str) {
    Matcher matcher = ARGUMENT_SUBSTITUTION.matcher(str);
    if (matcher.find() && matcher.groupCount() == 1) {
      return Integer.parseInt(matcher.group(1));
    } else {
      throw new IllegalArgumentException(
          String.format("Invalid argument substitution: [%s]. Use $1, $2, ... (starting from $1)", str));
    }
  }

  /**
   * Helper class to store predicate related information:
   * <ul>
   *   <li>Predicate</li>
   *   <li>String representation of the predicate</li>
   *   <li>Predicate evaluator</li>
   * </ul>
   */
  private static class PredicateInfo {
    final Predicate _predicate;
    final String _stringPredicate;
    PredicateEvaluator _predicateEvaluator;

    PredicateInfo(Predicate predicate) {
      _predicate = predicate;
      _stringPredicate = predicate.toString();
      _predicateEvaluator = null; // Initialized lazily
    }

    Predicate getPredicate() {
      return _predicate;
    }

    String getStringPredicate() {
      return _stringPredicate;
    }

    /**
     * Since PredicateEvaluator requires data-type, it is initialized lazily.
     */
    PredicateEvaluator getPredicateEvaluator(FieldSpec.DataType dataType) {
      if (_predicateEvaluator == null) {
        _predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(_predicate, null, dataType);
      }
      return _predicateEvaluator;
    }
  }
}
