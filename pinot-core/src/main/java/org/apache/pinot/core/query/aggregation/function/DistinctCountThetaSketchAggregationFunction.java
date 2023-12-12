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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.AnotB;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.ThetaSketchAccumulator;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * The {@code DistinctCountThetaSketchAggregationFunction} can be used in 2 modes:
 * <ul>
 *   <li>
 *     Simple union without post-aggregation (1 or 2 arguments): main expression to aggregate on, optional theta-sketch
 *     parameters
 *     <p>E.g. DISTINCT_COUNT_THETA_SKETCH(col)
 *   </li>
 *   <li>
 *     Union with post-aggregation (at least 4 arguments): main expression to aggregate on, theta-sketch parameters,
 *     filter(s), post-aggregation expression
 *     <p>E.g. DISTINCT_COUNT_THETA_SKETCH(col, '', 'dimName=''gender'' AND dimValue=''male''',
 *     'dimName=''course'' AND dimValue=''math''', 'SET_INTERSECT($1,$2)')
 *   </li>
 * </ul>
 * Currently, there are 5 parameters to the function:
 * <ul>
 *   <li>
 *     nominalEntries: The nominal entries used to create the sketch. (Default 4096)
 *     resizeFactor: Controls the size multiple that affects how fast the internal cache grows (Default 2^3=8)
 *     samplingProbability: Sets the upfront uniform sampling probability, p. (Default 1.0)
 *     intermediateOrdering: Whether compacted sketches should be ordered. (Default false)
 *     accumulatorThreshold: How many sketches should be kept in memory before merging. (Default 2)
 *   </li>
 * </ul>
 * <p>E.g. DISTINCT_COUNT_THETA_SKETCH(col, 'nominalEntries=8192')
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountThetaSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<List<ThetaSketchAccumulator>, Comparable> {
  private static final String SET_UNION = "setunion";
  private static final String SET_INTERSECT = "setintersect";
  private static final String SET_DIFF = "setdiff";
  private static final String DEFAULT_SKETCH_IDENTIFIER = "$0";
  private static final int DEFAULT_ACCUMULATOR_THRESHOLD = 2;
  private static final boolean DEFAULT_INTERMEDIATE_ORDERING = false;

  private final List<ExpressionContext> _inputExpressions;
  private final boolean _includeDefaultSketch;
  private final List<FilterEvaluator> _filterEvaluators;
  private final ExpressionContext _postAggregationExpression;
  private final UpdateSketchBuilder _updateSketchBuilder = new UpdateSketchBuilder();
  protected final SetOperationBuilder _setOperationBuilder = new SetOperationBuilder();
  protected boolean _intermediateOrdering = DEFAULT_INTERMEDIATE_ORDERING;
  protected int _accumulatorThreshold = DEFAULT_ACCUMULATOR_THRESHOLD;

  public DistinctCountThetaSketchAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));

    // Initialize the UpdateSketchBuilder and SetOperationBuilder with the parameters
    int numArguments = arguments.size();
    if (numArguments > 1) {
      ExpressionContext paramsExpression = arguments.get(1);
      Preconditions.checkArgument(paramsExpression.getType() == ExpressionContext.Type.LITERAL,
          "Second argument of DISTINCT_COUNT_THETA_SKETCH aggregation function must be literal (parameters)");
      Parameters parameters = new Parameters(paramsExpression.getLiteral().getStringValue());
      // Allows the user to trade-off memory usage for merge CPU; higher values use more memory
      _accumulatorThreshold = parameters.getAccumulatorThreshold();
      // Ordering controls whether intermediate compact sketches are ordered in set operations
      _intermediateOrdering = parameters.getIntermediateOrdering();
      // Nominal entries controls sketch accuracy and size
      int nominalEntries = parameters.getNominalEntries();
      _updateSketchBuilder.setNominalEntries(nominalEntries);
      _setOperationBuilder.setNominalEntries(nominalEntries);
      // Sampling probability sets the initial value of Theta, defaults to 1.0
      float p = parameters.getSamplingProbability();
      _setOperationBuilder.setP(p);
      _updateSketchBuilder.setP(p);
      // Resize factor controls the size multiple that affects how fast the internal cache grows
      ResizeFactor rf = parameters.getResizeFactor();
      _setOperationBuilder.setResizeFactor(rf);
      _updateSketchBuilder.setResizeFactor(rf);
    }

    if (numArguments < 4) {
      // Simple union without post-aggregation

      _inputExpressions = Collections.singletonList(_expression);
      _includeDefaultSketch = true;
      _filterEvaluators = Collections.emptyList();
      _postAggregationExpression = ExpressionContext.forIdentifier(DEFAULT_SKETCH_IDENTIFIER);
    } else {
      // Union with post-aggregation

      // Input expressions should include the main expression and the lhs of the predicates in the filters
      _inputExpressions = new ArrayList<>();
      _inputExpressions.add(_expression);
      Map<ExpressionContext, Integer> expressionIndexMap = new HashMap<>();
      expressionIndexMap.put(_expression, 0);

      // Process the filter expressions
      _filterEvaluators = new ArrayList<>(numArguments - 3);
      for (int i = 2; i < numArguments - 1; i++) {
        ExpressionContext filterExpression = arguments.get(i);
        Preconditions.checkArgument(filterExpression.getType() == ExpressionContext.Type.LITERAL,
            "Third to second last argument of DISTINCT_COUNT_THETA_SKETCH aggregation function must be literal "
                + "(filter expression)");
        FilterContext filter = RequestContextUtils.getFilter(
            CalciteSqlParser.compileToExpression(filterExpression.getLiteral().getStringValue()));
        Preconditions.checkArgument(!filter.isConstant(), "Filter must not be constant: %s", filter);
        // NOTE: Collect expressions before constructing the FilterInfo so that expressionIndexMap always include the
        //       expressions in the filter.
        collectExpressions(filter, _inputExpressions, expressionIndexMap);
        _filterEvaluators.add(getFilterEvaluator(filter, expressionIndexMap));
      }

      // Process the post-aggregation expression
      ExpressionContext postAggregationExpression = arguments.get(numArguments - 1);
      Preconditions.checkArgument(postAggregationExpression.getType() == ExpressionContext.Type.LITERAL,
          "Last argument of DISTINCT_COUNT_THETA_SKETCH aggregation function must be literal (post-aggregation "
              + "expression)");
      Expression expr = CalciteSqlParser.compileToExpression(postAggregationExpression.getLiteral().getStringValue());
      _postAggregationExpression = RequestContextUtils.getExpression(expr);

      // Validate the post-aggregation expression
      _includeDefaultSketch = validatePostAggregationExpression(_postAggregationExpression, _filterEvaluators.size());
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTTHETASKETCH;
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
    int numExpressions = _inputExpressions.size();
    boolean[] singleValues = new boolean[numExpressions];
    DataType[] valueTypes = new DataType[numExpressions];
    Object[] valueArrays = new Object[numExpressions];
    extractValues(blockValSetMap, singleValues, valueTypes, valueArrays);
    int numFilters = _filterEvaluators.size();

    // Main expression is always index 0
    if (valueTypes[0] != DataType.BYTES) {
      List<UpdateSketch> updateSketches = getUpdateSketches(aggregationResultHolder);
      if (singleValues[0]) {
        switch (valueTypes[0]) {
          case INT:
            int[] intValues = (int[]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                defaultSketch.update(intValues[i]);
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  updateSketch.update(intValues[j]);
                }
              }
            }
            break;
          case LONG:
            long[] longValues = (long[]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                defaultSketch.update(longValues[i]);
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  updateSketch.update(longValues[j]);
                }
              }
            }
            break;
          case FLOAT:
            float[] floatValues = (float[]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                defaultSketch.update(floatValues[i]);
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  updateSketch.update(floatValues[j]);
                }
              }
            }
            break;
          case DOUBLE:
            double[] doubleValues = (double[]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                defaultSketch.update(doubleValues[i]);
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  updateSketch.update(doubleValues[j]);
                }
              }
            }
            break;
          case STRING:
            String[] stringValues = (String[]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                defaultSketch.update(stringValues[i]);
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  updateSketch.update(stringValues[j]);
                }
              }
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal single-value data type for DISTINCT_COUNT_THETA_SKETCH aggregation function: "
                    + valueTypes[0]);
        }
      } else {
        switch (valueTypes[0]) {
          case INT:
            int[][] intValues = (int[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                for (int value : intValues[i]) {
                  defaultSketch.update(value);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int value : intValues[j]) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case LONG:
            long[][] longValues = (long[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                for (long value : longValues[i]) {
                  defaultSketch.update(value);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (long value : longValues[j]) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case FLOAT:
            float[][] floatValues = (float[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                for (float value : floatValues[i]) {
                  defaultSketch.update(value);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (float value : floatValues[j]) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case DOUBLE:
            double[][] doubleValues = (double[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                for (double value : doubleValues[i]) {
                  defaultSketch.update(value);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (double value : doubleValues[j]) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case STRING:
            String[][] stringValues = (String[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              UpdateSketch defaultSketch = updateSketches.get(0);
              for (int i = 0; i < length; i++) {
                for (String value : stringValues[i]) {
                  defaultSketch.update(value);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              UpdateSketch updateSketch = updateSketches.get(i + 1);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (String value : stringValues[j]) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal multi-value data type for DISTINCT_COUNT_THETA_SKETCH aggregation function: " + valueTypes[0]);
        }
      }
    } else {
      // Serialized sketch
      List<ThetaSketchAccumulator> thetaSketchAccumulators = getUnions(aggregationResultHolder);
      Sketch[] sketches = deserializeSketches((byte[][]) valueArrays[0], length);
      if (_includeDefaultSketch) {
        ThetaSketchAccumulator defaultThetaAccumulator = thetaSketchAccumulators.get(0);
        for (Sketch sketch : sketches) {
          defaultThetaAccumulator.apply(sketch);
        }
      }
      for (int i = 0; i < numFilters; i++) {
        FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
        ThetaSketchAccumulator thetaSketchAccumulator = thetaSketchAccumulators.get(i + 1);
        for (int j = 0; j < length; j++) {
          if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
            thetaSketchAccumulator.apply(sketches[j]);
          }
        }
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int numExpressions = _inputExpressions.size();
    boolean[] singleValues = new boolean[numExpressions];
    DataType[] valueTypes = new DataType[numExpressions];
    Object[] valueArrays = new Object[numExpressions];
    extractValues(blockValSetMap, singleValues, valueTypes, valueArrays);
    int numFilters = _filterEvaluators.size();

    // Main expression is always index 0
    if (valueTypes[0] != DataType.BYTES) {
      if (singleValues[0]) {
        switch (valueTypes[0]) {
          case INT:
            int[] intValues = (int[]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              int value = intValues[i];
              if (_includeDefaultSketch) {
                updateSketches.get(0).update(value);
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  updateSketches.get(j + 1).update(value);
                }
              }
            }
            break;
          case LONG:
            long[] longValues = (long[]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              long value = longValues[i];
              if (_includeDefaultSketch) {
                updateSketches.get(0).update(value);
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  updateSketches.get(j + 1).update(value);
                }
              }
            }
            break;
          case FLOAT:
            float[] floatValues = (float[]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              float value = floatValues[i];
              if (_includeDefaultSketch) {
                updateSketches.get(0).update(value);
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  updateSketches.get(j + 1).update(value);
                }
              }
            }
            break;
          case DOUBLE:
            double[] doubleValues = (double[]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              double value = doubleValues[i];
              if (_includeDefaultSketch) {
                updateSketches.get(0).update(value);
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  updateSketches.get(j + 1).update(value);
                }
              }
            }
            break;
          case STRING:
            String[] stringValues = (String[]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              String value = stringValues[i];
              if (_includeDefaultSketch) {
                updateSketches.get(0).update(value);
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  updateSketches.get(j + 1).update(value);
                }
              }
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal single-value data type for DISTINCT_COUNT_THETA_SKETCH aggregation function: "
                    + valueTypes[0]);
        }
      } else {
        switch (valueTypes[0]) {
          case INT:
            int[][] intValues = (int[][]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              int[] values = intValues[i];
              if (_includeDefaultSketch) {
                UpdateSketch defaultSketch = updateSketches.get(0);
                for (int value : values) {
                  defaultSketch.update(value);
                }
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  UpdateSketch updateSketch = updateSketches.get(j + 1);
                  for (int value : values) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case LONG:
            long[][] longValues = (long[][]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              long[] values = longValues[i];
              if (_includeDefaultSketch) {
                UpdateSketch defaultSketch = updateSketches.get(0);
                for (long value : values) {
                  defaultSketch.update(value);
                }
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  UpdateSketch updateSketch = updateSketches.get(j + 1);
                  for (long value : values) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case FLOAT:
            float[][] floatValues = (float[][]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              float[] values = floatValues[i];
              if (_includeDefaultSketch) {
                UpdateSketch defaultSketch = updateSketches.get(0);
                for (float value : values) {
                  defaultSketch.update(value);
                }
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  UpdateSketch updateSketch = updateSketches.get(j + 1);
                  for (float value : values) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case DOUBLE:
            double[][] doubleValues = (double[][]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              double[] values = doubleValues[i];
              if (_includeDefaultSketch) {
                UpdateSketch defaultSketch = updateSketches.get(0);
                for (double value : values) {
                  defaultSketch.update(value);
                }
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  UpdateSketch updateSketch = updateSketches.get(j + 1);
                  for (double value : values) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          case STRING:
            String[][] stringValues = (String[][]) valueArrays[0];
            for (int i = 0; i < length; i++) {
              List<UpdateSketch> updateSketches = getUpdateSketches(groupByResultHolder, groupKeyArray[i]);
              String[] values = stringValues[i];
              if (_includeDefaultSketch) {
                UpdateSketch defaultSketch = updateSketches.get(0);
                for (String value : values) {
                  defaultSketch.update(value);
                }
              }
              for (int j = 0; j < numFilters; j++) {
                if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
                  UpdateSketch updateSketch = updateSketches.get(j + 1);
                  for (String value : values) {
                    updateSketch.update(value);
                  }
                }
              }
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal multi-value data type for DISTINCT_COUNT_THETA_SKETCH aggregation function: " + valueTypes[0]);
        }
      }
    } else {
      // Serialized sketch
      Sketch[] sketches = deserializeSketches((byte[][]) valueArrays[0], length);
      for (int i = 0; i < length; i++) {
        List<ThetaSketchAccumulator> thetaSketchAccumulators = getUnions(groupByResultHolder, groupKeyArray[i]);
        Sketch sketch = sketches[i];
        if (_includeDefaultSketch) {
          thetaSketchAccumulators.get(0).apply(sketch);
        }
        for (int j = 0; j < numFilters; j++) {
          if (_filterEvaluators.get(j).evaluate(singleValues, valueTypes, valueArrays, i)) {
            thetaSketchAccumulators.get(j + 1).apply(sketch);
          }
        }
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int numExpressions = _inputExpressions.size();
    boolean[] singleValues = new boolean[numExpressions];
    DataType[] valueTypes = new DataType[numExpressions];
    Object[] valueArrays = new Object[numExpressions];
    extractValues(blockValSetMap, singleValues, valueTypes, valueArrays);
    int numFilters = _filterEvaluators.size();

    // Main expression is always index 0
    if (valueTypes[0] != DataType.BYTES) {
      if (singleValues[0]) {
        switch (valueTypes[0]) {
          case INT:
            int[] intValues = (int[]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  getUpdateSketches(groupByResultHolder, groupKey).get(0).update(intValues[i]);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    getUpdateSketches(groupByResultHolder, groupKey).get(i + 1).update(intValues[j]);
                  }
                }
              }
            }
            break;
          case LONG:
            long[] longValues = (long[]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  getUpdateSketches(groupByResultHolder, groupKey).get(0).update(longValues[i]);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    getUpdateSketches(groupByResultHolder, groupKey).get(i + 1).update(longValues[j]);
                  }
                }
              }
            }
            break;
          case FLOAT:
            float[] floatValues = (float[]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  getUpdateSketches(groupByResultHolder, groupKey).get(0).update(floatValues[i]);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    getUpdateSketches(groupByResultHolder, groupKey).get(i + 1).update(floatValues[j]);
                  }
                }
              }
            }
            break;
          case DOUBLE:
            double[] doubleValues = (double[]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  getUpdateSketches(groupByResultHolder, groupKey).get(0).update(doubleValues[i]);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    getUpdateSketches(groupByResultHolder, groupKey).get(i + 1).update(doubleValues[j]);
                  }
                }
              }
            }
            break;
          case STRING:
            String[] stringValues = (String[]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  getUpdateSketches(groupByResultHolder, groupKey).get(0).update(stringValues[i]);
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    getUpdateSketches(groupByResultHolder, groupKey).get(i + 1).update(stringValues[j]);
                  }
                }
              }
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal single-value data type for DISTINCT_COUNT_THETA_SKETCH aggregation function: "
                    + valueTypes[0]);
        }
      } else {
        switch (valueTypes[0]) {
          case INT:
            int[][] intValues = (int[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  UpdateSketch defaultSketch = getUpdateSketches(groupByResultHolder, groupKey).get(0);
                  for (int value : intValues[i]) {
                    defaultSketch.update(value);
                  }
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    UpdateSketch updateSketch = getUpdateSketches(groupByResultHolder, groupKey).get(i + 1);
                    for (int value : intValues[i]) {
                      updateSketch.update(value);
                    }
                  }
                }
              }
            }
            break;
          case LONG:
            long[][] longValues = (long[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  UpdateSketch defaultSketch = getUpdateSketches(groupByResultHolder, groupKey).get(0);
                  for (long value : longValues[i]) {
                    defaultSketch.update(value);
                  }
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    UpdateSketch updateSketch = getUpdateSketches(groupByResultHolder, groupKey).get(i + 1);
                    for (long value : longValues[i]) {
                      updateSketch.update(value);
                    }
                  }
                }
              }
            }
            break;
          case FLOAT:
            float[][] floatValues = (float[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  UpdateSketch defaultSketch = getUpdateSketches(groupByResultHolder, groupKey).get(0);
                  for (float value : floatValues[i]) {
                    defaultSketch.update(value);
                  }
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    UpdateSketch updateSketch = getUpdateSketches(groupByResultHolder, groupKey).get(i + 1);
                    for (float value : floatValues[i]) {
                      updateSketch.update(value);
                    }
                  }
                }
              }
            }
            break;
          case DOUBLE:
            double[][] doubleValues = (double[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  UpdateSketch defaultSketch = getUpdateSketches(groupByResultHolder, groupKey).get(0);
                  for (double value : doubleValues[i]) {
                    defaultSketch.update(value);
                  }
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    UpdateSketch updateSketch = getUpdateSketches(groupByResultHolder, groupKey).get(i + 1);
                    for (double value : doubleValues[i]) {
                      updateSketch.update(value);
                    }
                  }
                }
              }
            }
            break;
          case STRING:
            String[][] stringValues = (String[][]) valueArrays[0];
            if (_includeDefaultSketch) {
              for (int i = 0; i < length; i++) {
                for (int groupKey : groupKeysArray[i]) {
                  UpdateSketch defaultSketch = getUpdateSketches(groupByResultHolder, groupKey).get(0);
                  for (String value : stringValues[i]) {
                    defaultSketch.update(value);
                  }
                }
              }
            }
            for (int i = 0; i < numFilters; i++) {
              FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
              for (int j = 0; j < length; j++) {
                if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
                  for (int groupKey : groupKeysArray[i]) {
                    UpdateSketch updateSketch = getUpdateSketches(groupByResultHolder, groupKey).get(i + 1);
                    for (String value : stringValues[i]) {
                      updateSketch.update(value);
                    }
                  }
                }
              }
            }
            break;
          default:
            throw new IllegalStateException(
                "Illegal multi-value data type for DISTINCT_COUNT_THETA_SKETCH aggregation function: " + valueTypes[0]);
        }
      }
    } else {
      // Serialized sketch
      Sketch[] sketches = deserializeSketches((byte[][]) valueArrays[0], length);
      if (_includeDefaultSketch) {
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getUnions(groupByResultHolder, groupKey).get(0).apply(sketches[i]);
          }
        }
      }
      for (int i = 0; i < numFilters; i++) {
        FilterEvaluator filterEvaluator = _filterEvaluators.get(i);
        for (int j = 0; j < length; j++) {
          if (filterEvaluator.evaluate(singleValues, valueTypes, valueArrays, j)) {
            for (int groupKey : groupKeysArray[i]) {
              getUnions(groupByResultHolder, groupKey).get(i + 1).apply(sketches[i]);
            }
          }
        }
      }
    }
  }

  @Override
  public List<ThetaSketchAccumulator> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    List result = aggregationResultHolder.getResult();
    if (result == null) {
      int numSketches = _filterEvaluators.size() + 1;
      List<ThetaSketchAccumulator> sketches = new ArrayList<>(numSketches);
      for (int i = 0; i < numSketches; i++) {
        sketches.add(new ThetaSketchAccumulator(_setOperationBuilder, _intermediateOrdering, _accumulatorThreshold));
      }
      return sketches;
    }

    if (result.get(0) instanceof Sketch) {
      int numSketches = result.size();
      ArrayList<ThetaSketchAccumulator> thetaSketchAccumulators = new ArrayList<>(numSketches);
      for (Object o : result) {
        ThetaSketchAccumulator thetaSketchAccumulator =
            new ThetaSketchAccumulator(_setOperationBuilder, _intermediateOrdering, _accumulatorThreshold);
        thetaSketchAccumulator.apply((Sketch) o);
        thetaSketchAccumulators.add(thetaSketchAccumulator);
      }
      return thetaSketchAccumulators;
    } else {
      return result;
    }
  }

  @Override
  public List<ThetaSketchAccumulator> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    List result = groupByResultHolder.getResult(groupKey);

    if (result.get(0) instanceof Sketch) {
      int numSketches = result.size();
      ArrayList<ThetaSketchAccumulator> thetaSketchAccumulators = new ArrayList<>(numSketches);
      for (Object o : result) {
        ThetaSketchAccumulator thetaSketchAccumulator =
            new ThetaSketchAccumulator(_setOperationBuilder, _intermediateOrdering, _accumulatorThreshold);
        thetaSketchAccumulator.apply((Sketch) o);
        thetaSketchAccumulators.add(thetaSketchAccumulator);
      }
      return thetaSketchAccumulators;
    }

    return result;
  }

  @Override
  public List<ThetaSketchAccumulator> merge(List<ThetaSketchAccumulator> acc1, List<ThetaSketchAccumulator> acc2) {
    int numAccumulators = acc1.size();
    List<ThetaSketchAccumulator> mergedAccumulators = new ArrayList<>(numAccumulators);
    for (int i = 0; i < numAccumulators; i++) {
      ThetaSketchAccumulator thetaSketchAccumulator1 = acc1.get(i);
      ThetaSketchAccumulator thetaSketchAccumulator2 = acc2.get(i);
      if (thetaSketchAccumulator1.isEmpty()) {
        mergedAccumulators.add(thetaSketchAccumulator2);
        continue;
      }
      if (thetaSketchAccumulator2.isEmpty()) {
        mergedAccumulators.add(thetaSketchAccumulator1);
        continue;
      }
      thetaSketchAccumulator1.merge(thetaSketchAccumulator2);
      mergedAccumulators.add(thetaSketchAccumulator1);
    }
    return mergedAccumulators;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Comparable extractFinalResult(List<ThetaSketchAccumulator> accumulators) {
    int numAccumulators = accumulators.size();
    List<Sketch> mergedSketches = new ArrayList<>(numAccumulators);

    for (ThetaSketchAccumulator accumulator : accumulators) {
      accumulator.setOrdered(_intermediateOrdering);
      accumulator.setThreshold(_accumulatorThreshold);
      accumulator.setSetOperationBuilder(_setOperationBuilder);
      mergedSketches.add(accumulator.getResult());
    }

    return Math.round(evaluatePostAggregationExpression(_postAggregationExpression, mergedSketches).getEstimate());
  }

  /**
   * Helper method to collect expressions in the filter.
   */
  private static void collectExpressions(FilterContext filter, List<ExpressionContext> expressions,
      Map<ExpressionContext, Integer> expressionIndexMap) {
    List<FilterContext> children = filter.getChildren();
    if (children != null) {
      for (FilterContext child : children) {
        collectExpressions(child, expressions, expressionIndexMap);
      }
    } else {
      ExpressionContext expression = filter.getPredicate().getLhs();
      if (expressionIndexMap.putIfAbsent(expression, expressions.size()) == null) {
        expressions.add(expression);
      }
    }
  }

  /**
   * Creates a FilterEvaluator for the given filter.
   */
  private static FilterEvaluator getFilterEvaluator(FilterContext filter,
      Map<ExpressionContext, Integer> expressionIndexMap) {
    switch (filter.getType()) {
      case AND:
        List<FilterContext> children = filter.getChildren();
        List<FilterEvaluator> childEvaluators = new ArrayList<>(children.size());
        for (FilterContext child : children) {
          childEvaluators.add(getFilterEvaluator(child, expressionIndexMap));
        }
        return new AndFilterEvaluator(childEvaluators);
      case OR:
        children = filter.getChildren();
        childEvaluators = new ArrayList<>(children.size());
        for (FilterContext child : children) {
          childEvaluators.add(getFilterEvaluator(child, expressionIndexMap));
        }
        return new OrFilterEvaluator(childEvaluators);
      case NOT:
        assert filter.getChildren().size() == 1;
        return new NotFilterEvaluator(getFilterEvaluator(filter.getChildren().get(0), expressionIndexMap));
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        int expressionIndex = expressionIndexMap.get(predicate.getLhs());
        return new PredicateFilterEvaluator(predicate, expressionIndex);
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Validates the post-aggregation expression:
   *   - The sketch id ($0, $1, etc.) does not exceed the number of filters
   *   - Only contains valid set operations (SET_UNION/SET_INTERSECT/SET_DIFF)
   *   - SET_UNION/SET_INTERSECT contains at least 2 arguments
   *   - SET_DIFF contains exactly 2 arguments
   * Returns whether the post-aggregation expression contains the default sketch ($0).
   */
  private static boolean validatePostAggregationExpression(ExpressionContext expression, int numFilters) {
    Preconditions.checkArgument(expression.getType() != ExpressionContext.Type.LITERAL,
        "Post-aggregation expression should not contain literal expression: %s", expression.toString());
    if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
      int sketchId = extractSketchId(expression.getIdentifier());
      Preconditions.checkArgument(sketchId <= numFilters, "Sketch id: %s exceeds number of filters: %s", sketchId,
          numFilters);
      return sketchId == 0;
    }

    FunctionContext function = expression.getFunction();
    String functionName = function.getFunctionName();
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    boolean includeDefaultSketch = false;
    switch (functionName) {
      case SET_UNION:
      case SET_INTERSECT:
        Preconditions.checkArgument(numArguments >= 2,
            "SET_UNION and SET_INTERSECT should have at least 2 arguments, got: %s", numArguments);
        for (ExpressionContext argument : arguments) {
          includeDefaultSketch |= validatePostAggregationExpression(argument, numFilters);
        }
        break;
      case SET_DIFF:
        Preconditions.checkArgument(numArguments == 2, "SET_DIFF should have 2 arguments, got: %s", numArguments);
        for (ExpressionContext argument : arguments) {
          includeDefaultSketch |= validatePostAggregationExpression(argument, numFilters);
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid set operation: " + functionName);
    }
    return includeDefaultSketch;
  }

  /**
   * Extracts the sketch id from the identifier (e.g. $0 -> 0, $1 -> 1).
   */
  private static int extractSketchId(String identifier) {
    Preconditions.checkArgument(identifier.charAt(0) == '$', "Invalid identifier: %s, expecting $0, $1, etc.",
        identifier);
    int sketchId = Integer.parseInt(identifier.substring(1));
    Preconditions.checkArgument(sketchId >= 0, "Invalid identifier: %s, expecting $0, $1, etc.", identifier);
    return sketchId;
  }

  /**
   * Extracts values from the BlockValSet map.
   */
  private void extractValues(Map<ExpressionContext, BlockValSet> blockValSetMap, boolean[] singleValues,
      DataType[] valueTypes, Object[] valueArrays) {
    int numExpressions = _inputExpressions.size();
    for (int i = 0; i < numExpressions; i++) {
      BlockValSet blockValSet = blockValSetMap.get(_inputExpressions.get(i));
      boolean singleValue = blockValSet.isSingleValue();
      DataType storedType = blockValSet.getValueType().getStoredType();
      singleValues[i] = singleValue;
      valueTypes[i] = storedType;
      if (singleValue) {
        switch (storedType) {
          case INT:
            valueArrays[i] = blockValSet.getIntValuesSV();
            break;
          case LONG:
            valueArrays[i] = blockValSet.getLongValuesSV();
            break;
          case FLOAT:
            valueArrays[i] = blockValSet.getFloatValuesSV();
            break;
          case DOUBLE:
            valueArrays[i] = blockValSet.getDoubleValuesSV();
            break;
          case STRING:
            valueArrays[i] = blockValSet.getStringValuesSV();
            break;
          case BYTES:
            valueArrays[i] = blockValSet.getBytesValuesSV();
            break;
          default:
            throw new IllegalStateException();
        }
      } else {
        switch (storedType) {
          case INT:
            valueArrays[i] = blockValSet.getIntValuesMV();
            break;
          case LONG:
            valueArrays[i] = blockValSet.getLongValuesMV();
            break;
          case FLOAT:
            valueArrays[i] = blockValSet.getFloatValuesMV();
            break;
          case DOUBLE:
            valueArrays[i] = blockValSet.getDoubleValuesMV();
            break;
          case STRING:
            valueArrays[i] = blockValSet.getStringValuesMV();
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }
  }

  /**
   * Returns the UpdateSketch list from the result holder or creates a new one if it does not exist.
   */
  private List<UpdateSketch> getUpdateSketches(AggregationResultHolder aggregationResultHolder) {
    List<UpdateSketch> updateSketches = aggregationResultHolder.getResult();
    if (updateSketches == null) {
      updateSketches = buildUpdateSketches();
      aggregationResultHolder.setValue(updateSketches);
    }
    return updateSketches;
  }

  /**
   * Returns the Union list from the result holder or creates a new one if it does not exist.
   */
  private List<ThetaSketchAccumulator> getUnions(AggregationResultHolder aggregationResultHolder) {
    List<ThetaSketchAccumulator> unions = aggregationResultHolder.getResult();
    if (unions == null) {
      unions = buildUnions();
      aggregationResultHolder.setValue(unions);
    }
    return unions;
  }

  /**
   * Returns the UpdateSketch list for the given group key or creates a new one if it does not exist.
   */
  private List<UpdateSketch> getUpdateSketches(GroupByResultHolder groupByResultHolder, int groupKey) {
    List<UpdateSketch> updateSketches = groupByResultHolder.getResult(groupKey);
    if (updateSketches == null) {
      updateSketches = buildUpdateSketches();
      groupByResultHolder.setValueForKey(groupKey, updateSketches);
    }
    return updateSketches;
  }

  /**
   * Returns the Union list for the given group key or creates a new one if it does not exist.
   */
  private List<ThetaSketchAccumulator> getUnions(GroupByResultHolder groupByResultHolder, int groupKey) {
    List<ThetaSketchAccumulator> unions = groupByResultHolder.getResult(groupKey);
    if (unions == null) {
      unions = buildUnions();
      groupByResultHolder.setValueForKey(groupKey, unions);
    }
    return unions;
  }

  /**
   * Builds the UpdateSketch list.
   */
  private List<UpdateSketch> buildUpdateSketches() {
    int numSketches = _filterEvaluators.size() + 1;
    List<UpdateSketch> updateSketches = new ArrayList<>(numSketches);
    for (int i = 0; i < numSketches; i++) {
      updateSketches.add(_updateSketchBuilder.build());
    }
    return updateSketches;
  }

  /**
   * Builds the Union list.
   */
  private List<ThetaSketchAccumulator> buildUnions() {
    int numUnions = _filterEvaluators.size() + 1;
    List<ThetaSketchAccumulator> unions = new ArrayList<>(numUnions);
    for (int i = 0; i < numUnions; i++) {
      ThetaSketchAccumulator thetaSketchAccumulator =
          new ThetaSketchAccumulator(_setOperationBuilder, _intermediateOrdering, _accumulatorThreshold);
      unions.add(thetaSketchAccumulator);
    }
    return unions;
  }

  /**
   * Deserializes the sketches from the bytes.
   */
  private Sketch[] deserializeSketches(byte[][] serializedSketches, int length) {
    Sketch[] sketches = new Sketch[length];
    for (int i = 0; i < length; i++) {
      sketches[i] = Sketch.wrap(Memory.wrap(serializedSketches[i]));
    }
    return sketches;
  }

  /**
   * Evaluates the post-aggregation expression.
   */
  protected Sketch evaluatePostAggregationExpression(List<Sketch> sketches) {
    return evaluatePostAggregationExpression(_postAggregationExpression, sketches);
  }

  /**
   * Evaluates the post-aggregation expression.
   */
  private Sketch evaluatePostAggregationExpression(ExpressionContext expression, List<Sketch> sketches) {
    if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
      return sketches.get(extractSketchId(expression.getIdentifier()));
    }

    FunctionContext function = expression.getFunction();
    String functionName = function.getFunctionName();
    List<ExpressionContext> arguments = function.getArguments();
    switch (functionName) {
      case SET_UNION:
        Union union = _setOperationBuilder.buildUnion();
        for (ExpressionContext argument : arguments) {
          union.union(evaluatePostAggregationExpression(argument, sketches));
        }
        return union.getResult(_intermediateOrdering, null);
      case SET_INTERSECT:
        Intersection intersection = _setOperationBuilder.buildIntersection();
        for (ExpressionContext argument : arguments) {
          intersection.intersect(evaluatePostAggregationExpression(argument, sketches));
        }
        return intersection.getResult(_intermediateOrdering, null);
      case SET_DIFF:
        AnotB diff = _setOperationBuilder.buildANotB();
        diff.setA(evaluatePostAggregationExpression(arguments.get(0), sketches));
        diff.notB(evaluatePostAggregationExpression(arguments.get(1), sketches));
        return diff.getResult(_intermediateOrdering, null, false);
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Helper class to wrap the theta-sketch parameters.  The initial values for the parameters are set to the
   * same defaults in the Apache Datasketches library.
   */
  private static class Parameters {
    private static final char PARAMETER_DELIMITER = ';';
    private static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';
    private static final String NOMINAL_ENTRIES_KEY = "nominalEntries";
    private static final String RESIZE_FACTOR_KEY = "resizeFactor";
    private static final String SAMPLING_PROBABILITY_KEY = "samplingProbability";
    private static final String INTERMEDIATE_ORDERING_KEY = "intermediateOrdering";
    private static final String ACCUMULATOR_THRESHOLD_KEY = "accumulatorThreshold";

    private int _resizeFactor = ResizeFactor.X8.getValue();
    private int _nominalEntries = ThetaUtil.DEFAULT_NOMINAL_ENTRIES;
    private int _accumulatorThreshold = DEFAULT_ACCUMULATOR_THRESHOLD;
    private boolean _intermediateOrdering = DEFAULT_INTERMEDIATE_ORDERING;
    private float _samplingProbability = 1.0F;

    Parameters(String parametersString) {
      StringUtils.deleteWhitespace(parametersString);
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePair : keyValuePairs) {
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1];
        if (key.equalsIgnoreCase(NOMINAL_ENTRIES_KEY)) {
          _nominalEntries = Integer.parseInt(value);
        } else if (key.equalsIgnoreCase(SAMPLING_PROBABILITY_KEY)) {
          _samplingProbability = Float.parseFloat(value);
        } else if (key.equalsIgnoreCase(RESIZE_FACTOR_KEY)) {
          _resizeFactor = Integer.parseInt(value);
        } else if (key.equalsIgnoreCase(INTERMEDIATE_ORDERING_KEY)) {
          _intermediateOrdering = Boolean.parseBoolean(value);
        } else if (key.equalsIgnoreCase(ACCUMULATOR_THRESHOLD_KEY)) {
          _accumulatorThreshold = Integer.parseInt(value);
        } else {
          throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }

    int getNominalEntries() {
      return _nominalEntries;
    }

    float getSamplingProbability() {
      return _samplingProbability;
    }

    boolean getIntermediateOrdering() {
      return _intermediateOrdering;
    }

    int getAccumulatorThreshold() {
      return _accumulatorThreshold;
    }

    ResizeFactor getResizeFactor() {
      return ResizeFactor.getRF(_resizeFactor);
    }
  }

  /**
   * Helper interface to evaluate the filter on the values.
   */
  private interface FilterEvaluator {

    /**
     * Evaluates the given values with the filter, returns {@code true} if the values pass the filter, {@code false}
     * otherwise.
     */
    boolean evaluate(boolean[] singleValues, DataType[] valueTypes, Object[] valueArrays, int index);
  }

  private static class AndFilterEvaluator implements FilterEvaluator {
    final List<FilterEvaluator> _children;

    private AndFilterEvaluator(List<FilterEvaluator> children) {
      _children = children;
    }

    @Override
    public boolean evaluate(boolean[] singleValues, DataType[] valueTypes, Object[] valueArrays, int index) {
      for (FilterEvaluator child : _children) {
        if (!child.evaluate(singleValues, valueTypes, valueArrays, index)) {
          return false;
        }
      }
      return true;
    }
  }

  private static class OrFilterEvaluator implements FilterEvaluator {
    final List<FilterEvaluator> _children;

    private OrFilterEvaluator(List<FilterEvaluator> children) {
      _children = children;
    }

    @Override
    public boolean evaluate(boolean[] singleValues, DataType[] valueTypes, Object[] valueArrays, int index) {
      for (FilterEvaluator child : _children) {
        if (child.evaluate(singleValues, valueTypes, valueArrays, index)) {
          return true;
        }
      }
      return false;
    }
  }

  private static class NotFilterEvaluator implements FilterEvaluator {
    final FilterEvaluator _child;

    private NotFilterEvaluator(FilterEvaluator child) {
      _child = child;
    }

    @Override
    public boolean evaluate(boolean[] singleValues, DataType[] valueTypes, Object[] valueArrays, int index) {
      return !_child.evaluate(singleValues, valueTypes, valueArrays, index);
    }
  }

  private static class PredicateFilterEvaluator implements FilterEvaluator {
    final Predicate _predicate;
    final int _expressionIndex;
    PredicateEvaluator _predicateEvaluator;

    private PredicateFilterEvaluator(Predicate predicate, int expressionIndex) {
      _predicate = predicate;
      _expressionIndex = expressionIndex;
    }

    @Override
    public boolean evaluate(boolean[] singleValues, DataType[] valueTypes, Object[] valueArrays, int index) {
      boolean singleValue = singleValues[_expressionIndex];
      DataType valueType = valueTypes[_expressionIndex];
      Object valueArray = valueArrays[_expressionIndex];
      if (_predicateEvaluator == null) {
        _predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(_predicate, null, valueType);
      }
      if (singleValue) {
        switch (valueType) {
          case INT:
            return _predicateEvaluator.applySV(((int[]) valueArray)[index]);
          case LONG:
            return _predicateEvaluator.applySV(((long[]) valueArray)[index]);
          case FLOAT:
            return _predicateEvaluator.applySV(((float[]) valueArray)[index]);
          case DOUBLE:
            return _predicateEvaluator.applySV(((double[]) valueArray)[index]);
          case STRING:
            return _predicateEvaluator.applySV(((String[]) valueArray)[index]);
          case BYTES:
            return _predicateEvaluator.applySV(((byte[][]) valueArray)[index]);
          default:
            throw new IllegalStateException();
        }
      } else {
        switch (valueType) {
          case INT:
            int[] intValues = ((int[][]) valueArray)[index];
            return _predicateEvaluator.applyMV(intValues, intValues.length);
          case LONG:
            long[] longValues = ((long[][]) valueArray)[index];
            return _predicateEvaluator.applyMV(longValues, longValues.length);
          case FLOAT:
            float[] floatValues = ((float[][]) valueArray)[index];
            return _predicateEvaluator.applyMV(floatValues, floatValues.length);
          case DOUBLE:
            double[] doubleValues = ((double[][]) valueArray)[index];
            return _predicateEvaluator.applyMV(doubleValues, doubleValues.length);
          case STRING:
            String[] stringValues = ((String[][]) valueArray)[index];
            return _predicateEvaluator.applyMV(stringValues, stringValues.length);
          default:
            throw new IllegalStateException();
        }
      }
    }
  }
}
