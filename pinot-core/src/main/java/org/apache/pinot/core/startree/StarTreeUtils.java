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
package org.apache.pinot.core.startree;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.objects.ObjectBooleanPair;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.plan.StarTreeProjectPlanNode;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("rawtypes")
public class StarTreeUtils {
  private StarTreeUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeUtils.class);

  /// Extracts the [AggregationFunctionColumnPair]s from the given [AggregationFunction]s. Returns
  /// `null` if any [AggregationFunction] cannot be represented as an [AggregationFunctionColumnPair]
  /// (e.g. has multiple arguments, argument is not column etc.).
  @Nullable
  public static AggregationFunctionColumnPair[] extractAggregationFunctionPairs(
      AggregationFunction[] aggregationFunctions) {
    return extractAggregationFunctionPairs(aggregationFunctions, false);
  }

  /// Extracts the [AggregationFunctionColumnPair]s from the given [AggregationFunction]s, resolving them against
  /// either a regular or a null-aware star-tree. Returns `null` if any [AggregationFunction] cannot be represented as
  /// an [AggregationFunctionColumnPair].
  ///
  /// The only pair that differs between the two is `COUNT`: a regular star-tree stores a single `count__*` of every
  /// row, while a null-aware star-tree stores a `count__column` holding the count of that column's non-null values.
  @Nullable
  public static AggregationFunctionColumnPair[] extractAggregationFunctionPairs(
      AggregationFunction[] aggregationFunctions, boolean nullHandlingEnabled) {
    int numAggregationFunctions = aggregationFunctions.length;
    AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
        new AggregationFunctionColumnPair[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunctionColumnPair aggregationFunctionColumnPair =
          AggregationFunctionUtils.getStoredFunctionColumnPair(aggregationFunctions[i], nullHandlingEnabled);
      if (aggregationFunctionColumnPair != null) {
        aggregationFunctionColumnPairs[i] = aggregationFunctionColumnPair;
      } else {
        return null;
      }
    }
    return aggregationFunctionColumnPairs;
  }

  /// Extracts a map from the column to a list of [CompositePredicateEvaluator]s for it. Returns `null` if
  /// the filter cannot be solved by the star-tree.
  ///
  /// A predicate can be simple (d1 > 10) or composite (d1 > 10 AND d2 < 50) or multi levelled
  /// (d1 > 50 AND (d2 > 10 OR NOT d2 > 35)).
  /// This method represents a list of CompositePredicates per dimension. For each dimension, all CompositePredicates in
  /// the list are implicitly ANDed together. Any OR and NOT predicates are nested within a CompositePredicate.
  ///
  /// A map from predicates to their evaluators is passed in to accelerate the computation.
  @Nullable
  public static Map<String, List<CompositePredicateEvaluator>> extractPredicateEvaluatorsMap(IndexSegment indexSegment,
      @Nullable FilterContext filter, List<Pair<Predicate, PredicateEvaluator>> predicateEvaluatorMapping) {
    if (filter == null) {
      return Map.of();
    }

    Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap = new HashMap<>();
    Queue<FilterContext> queue = new ArrayDeque<>();
    queue.add(filter);
    FilterContext filterNode;
    while ((filterNode = queue.poll()) != null) {
      switch (filterNode.getType()) {
        case AND:
          queue.addAll(filterNode.getChildren());
          break;
        case OR:
          Pair<String, CompositePredicateEvaluator> pair =
              isOrClauseValidForStarTree(indexSegment, filterNode, predicateEvaluatorMapping);
          if (pair == null) {
            return null;
          }
          // NOTE: Null identifier means always true
          if (pair.getLeft() != null) {
            predicateEvaluatorsMap.computeIfAbsent(pair.getLeft(), k -> new ArrayList<>()).add(pair.getRight());
          }
          break;
        case NOT:
          boolean negated = true;
          FilterContext negatedChild = filterNode.getChildren().get(0);
          while (true) {
            FilterContext.Type type = negatedChild.getType();
            if (type == FilterContext.Type.PREDICATE) {
              Predicate predicate = negatedChild.getPredicate();
              PredicateEvaluator predicateEvaluator =
                  getPredicateEvaluator(indexSegment, predicate, predicateEvaluatorMapping);
              // Do not use star-tree when the predicate cannot be solved with star-tree
              if (predicateEvaluator == null) {
                return null;
              }
              // Do not use star-tree when the predicate is always false
              if ((predicateEvaluator.isAlwaysTrue() && negated) || (predicateEvaluator.isAlwaysFalse() && !negated)) {
                return null;
              }
              // Skip adding always true predicate
              if ((predicateEvaluator.isAlwaysTrue() && !negated) || (predicateEvaluator.isAlwaysFalse() && negated)) {
                break;
              }
              predicateEvaluatorsMap.computeIfAbsent(predicate.getLhs().getIdentifier(), k -> new ArrayList<>())
                  .add(new CompositePredicateEvaluator(List.of(ObjectBooleanPair.of(predicateEvaluator, negated))));
              break;
            }
            if (type == FilterContext.Type.NOT) {
              negated = !negated;
              negatedChild = negatedChild.getChildren().get(0);
              continue;
            }
            // Do not allow nested AND/OR under NOT
            return null;
          }
          break;
        case PREDICATE:
          Predicate predicate = filterNode.getPredicate();
          PredicateEvaluator predicateEvaluator =
              getPredicateEvaluator(indexSegment, predicate, predicateEvaluatorMapping);
          // Do not use star-tree when the predicate cannot be solved with star-tree or is always false
          if (predicateEvaluator == null || predicateEvaluator.isAlwaysFalse()) {
            return null;
          }
          if (!predicateEvaluator.isAlwaysTrue()) {
            predicateEvaluatorsMap.computeIfAbsent(predicate.getLhs().getIdentifier(), k -> new ArrayList<>())
                .add(new CompositePredicateEvaluator(List.of(ObjectBooleanPair.of(predicateEvaluator, false))));
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return predicateEvaluatorsMap;
  }

  /// Returns whether the query is fit for star tree index.
  ///
  /// The query is fit for star tree index if the following conditions are met:
  ///
  /// - Star-tree contains all aggregation function column pairs
  /// - All predicate columns and group-by columns are star-tree dimensions
  public static boolean isFitForStarTree(StarTreeV2Metadata starTreeV2Metadata,
      List<Pair<AggregationFunction, AggregationFunctionColumnPair>> aggregations,
      @Nullable ExpressionContext[] groupByExpressions, Set<String> predicateColumns) {
    // Check aggregations
    for (Pair<AggregationFunction, AggregationFunctionColumnPair> aggregation : aggregations) {
      AggregationFunction function = aggregation.getLeft();
      AggregationFunctionColumnPair functionColumnPair = aggregation.getRight();
      AggregationSpec aggregationSpec = starTreeV2Metadata.getAggregationSpecs().get(functionColumnPair);
      if (aggregationSpec == null) {
        return false;
      }
      if (!function.canUseStarTree(aggregationSpec.getFunctionParameters())) {
        return false;
      }
    }

    Set<String> starTreeDimensions = new HashSet<>(starTreeV2Metadata.getDimensionsSplitOrder());

    // Check group-by expressions
    if (groupByExpressions != null) {
      Set<String> groupByColumns = new HashSet<>();
      for (ExpressionContext groupByExpression : groupByExpressions) {
        groupByExpression.getColumns(groupByColumns);
      }
      if (!starTreeDimensions.containsAll(groupByColumns)) {
        return false;
      }
    }

    // Check predicate columns
    return starTreeDimensions.containsAll(predicateColumns);
  }

  /// Evaluates whether the given OR clause is valid for StarTree processing.
  /// StarTree supports OR predicates on a single dimension only (d1 < 10 OR d1 > 50).
  ///
  /// @return The pair of single identifier and predicate evaluators applied to it if true; `null` if the OR
  ///         clause cannot be solved with star-tree; a pair of nulls if the OR clause always evaluates to true.
  @Nullable
  private static Pair<String, CompositePredicateEvaluator> isOrClauseValidForStarTree(IndexSegment indexSegment,
      FilterContext filter, List<Pair<Predicate, PredicateEvaluator>> predicateEvaluatorMapping) {
    assert filter.getType() == FilterContext.Type.OR;

    List<ObjectBooleanPair<Predicate>> predicates = new ArrayList<>();
    if (!extractOrClausePredicates(filter, predicates)) {
      return null;
    }

    String identifier = null;
    List<ObjectBooleanPair<PredicateEvaluator>> predicateEvaluators = new ArrayList<>();
    for (ObjectBooleanPair<Predicate> predicate : predicates) {
      PredicateEvaluator predicateEvaluator =
          getPredicateEvaluator(indexSegment, predicate.left(), predicateEvaluatorMapping);
      if (predicateEvaluator == null) {
        // The predicate cannot be solved with star-tree
        return null;
      }
      boolean negated = predicate.rightBoolean();
      // Use a pair of null values to represent always true
      if ((predicateEvaluator.isAlwaysTrue() && !negated) || (predicateEvaluator.isAlwaysFalse() && negated)) {
        return Pair.of(null, null);
      }
      // Skip the always false predicate
      if ((predicateEvaluator.isAlwaysTrue() && negated) || (predicateEvaluator.isAlwaysFalse() && !negated)) {
        continue;
      }
      String predicateIdentifier = predicate.left().getLhs().getIdentifier();
      if (identifier == null) {
        identifier = predicateIdentifier;
      } else {
        if (!identifier.equals(predicateIdentifier)) {
          // The predicates are applied to multiple columns
          return null;
        }
      }
      predicateEvaluators.add(ObjectBooleanPair.of(predicateEvaluator, negated));
    }
    // When all predicates are always false, do not use star-tree
    if (predicateEvaluators.isEmpty()) {
      return null;
    }
    return Pair.of(identifier, new CompositePredicateEvaluator(predicateEvaluators));
  }

  /// Extracts the predicates under the given OR clause, returns `false` if there is nested AND or NOT under OR
  /// clause.
  private static boolean extractOrClausePredicates(FilterContext filter,
      List<ObjectBooleanPair<Predicate>> predicates) {
    assert filter.getType() == FilterContext.Type.OR;

    for (FilterContext child : filter.getChildren()) {
      switch (child.getType()) {
        case AND:
          return false;
        case OR:
          if (!extractOrClausePredicates(child, predicates)) {
            return false;
          }
          break;
        case NOT:
          boolean negated = true;
          FilterContext negatedChild = child.getChildren().get(0);
          while (true) {
            FilterContext.Type type = negatedChild.getType();
            if (type == FilterContext.Type.PREDICATE) {
              predicates.add(ObjectBooleanPair.of(negatedChild.getPredicate(), negated));
              break;
            }
            if (type == FilterContext.Type.NOT) {
              negated = !negated;
              negatedChild = negatedChild.getChildren().get(0);
              continue;
            }
            // Do not allow nested AND/OR under NOT
            return false;
          }
          break;
        case PREDICATE:
          predicates.add(ObjectBooleanPair.of(child.getPredicate(), false));
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return true;
  }

  /// Returns the predicate evaluator for the given predicate, or `null` if the predicate cannot be solved with
  /// star-tree.
  @Nullable
  private static PredicateEvaluator getPredicateEvaluator(IndexSegment indexSegment, Predicate predicate,
      List<Pair<Predicate, PredicateEvaluator>> predicatesEvaluatorMapping) {
    ExpressionContext lhs = predicate.getLhs();
    if (lhs.getType() != ExpressionContext.Type.IDENTIFIER) {
      // Star-tree does not support non-identifier expression
      return null;
    }
    String column = lhs.getIdentifier();
    DataSource dataSource = indexSegment.getDataSourceNullable(column);
    if (dataSource == null) {
      // Star-tree does not support non-existent column
      return null;
    }
    Dictionary dictionary = dataSource.getDictionary();
    if (dictionary == null) {
      // Star-tree does not support non-dictionary encoded dimension
      return null;
    }
    switch (predicate.getType()) {
      // Do not use star-tree for the following predicates because:
      //   - REGEXP_LIKE: Need to scan the whole dictionary to gather the matching dictionary ids
      //   - TEXT_MATCH/IS_NULL/IS_NOT_NULL: No way to gather the matching dictionary ids
      // TODO: Support IS_NULL / IS_NOT_NULL on a null-aware star-tree.
      //   Nothing in the index prevents it: a null-aware star-tree stores nulls under a reserved dictionary id one
      //   past the column's last real id, so IS_NULL matches that id alone and IS_NOT_NULL matches every real id.
      //   Null rows form their own child node, and StarTreeFilterOperator already skips the star node for a
      //   predicated dimension, so nulls cannot leak in through it (at the cost of enumerating real children for
      //   IS_NOT_NULL). The gap is that FilterPlanNode answers both straight from the segment's null vector with a
      //   BitmapBasedFilterOperator and never builds a predicate evaluator, so there is no getMatchingDictIds() to
      //   call here. Supporting them needs a star-tree specific evaluator that knows the reserved id.
      case REGEXP_LIKE:
      case TEXT_MATCH:
      case IS_NULL:
      case IS_NOT_NULL:
        return null;
      default:
        break;
    }
    for (Pair<Predicate, PredicateEvaluator> pair : predicatesEvaluatorMapping) {
      if (pair.getKey() == predicate) {
        return toDictionaryBased(pair.getValue(), predicate, dataSource);
      }
    }
    return null;
  }

  /// Star-tree traversal reads dictionary ids; a raw-value evaluator (built when the forward index is RAW and no
  /// dict-consuming scan operator was available) would throw from `getMatchingDictIds` / `applySV(int)`. Rebuild
  /// against the segment dictionary when needed.
  @VisibleForTesting
  static PredicateEvaluator toDictionaryBased(PredicateEvaluator evaluator, Predicate predicate,
      DataSource dataSource) {
    if (evaluator.isDictionaryBased()) {
      return evaluator;
    }
    return PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
        dataSource.getDataSourceMetadata().getDataType(), null);
  }

  /// The star-tree a query was routed to, together with the [AggregationFunctionColumnPair]s resolved against it.
  ///
  /// The pairs have to travel with the operator because they depend on which star-tree was picked: a null-aware
  /// star-tree resolves `COUNT(column)` to `count__column`, while a regular one resolves it to `count__*`. The
  /// aggregation executors must read back the same columns that were projected.
  public static class StarTreeProjectPlan {
    private final BaseProjectOperator<?> _projectOperator;
    private final AggregationFunctionColumnPair[] _functionColumnPairs;

    public StarTreeProjectPlan(BaseProjectOperator<?> projectOperator,
        AggregationFunctionColumnPair[] functionColumnPairs) {
      _projectOperator = projectOperator;
      _functionColumnPairs = functionColumnPairs;
    }

    public BaseProjectOperator<?> getProjectOperator() {
      return _projectOperator;
    }

    public AggregationFunctionColumnPair[] getFunctionColumnPairs() {
      return _functionColumnPairs;
    }
  }

  /// Returns a [StarTreeProjectPlan] when the filter can be solved with star-tree, or `null` otherwise.
  ///
  /// A star-tree is only consistent with one null-handling mode. A regular star-tree folds nulls into the column's
  /// default null value and includes them in the pre-aggregation, matching null-handling-off semantics; a null-aware
  /// star-tree keeps nulls apart and excludes them, matching null-handling-on semantics. Queries are therefore routed
  /// to a star-tree built in the matching mode, except that a null-handling-on query may still fall back to a regular
  /// star-tree when none of the columns it touches actually contains a null value.
  @Nullable
  public static StarTreeProjectPlan createStarTreeBasedProjectOperator(IndexSegment indexSegment,
      QueryContext queryContext, AggregationFunction[] aggregationFunctions, @Nullable FilterContext filter,
      List<Pair<Predicate, PredicateEvaluator>> predicateEvaluators) {
    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees == null || queryContext.isSkipStarTree()) {
      return null;
    }

    Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap =
        extractPredicateEvaluatorsMap(indexSegment, filter, predicateEvaluators);
    if (predicateEvaluatorsMap == null) {
      return null;
    }

    ExpressionContext[] groupByExpressions =
        queryContext.getGroupByExpressions() != null ? queryContext.getGroupByExpressions()
            .toArray(new ExpressionContext[0]) : null;

    if (queryContext.isNullHandlingEnabled()) {
      // A null-aware star-tree pre-aggregates with exactly the semantics the query asks for
      StarTreeProjectPlan plan = createProjectPlan(indexSegment, queryContext, starTrees, true, aggregationFunctions,
          groupByExpressions, predicateEvaluatorsMap);
      if (plan != null) {
        return plan;
      }
    }
    return createProjectPlan(indexSegment, queryContext, starTrees, false, aggregationFunctions, groupByExpressions,
        predicateEvaluatorsMap);
  }

  /// Returns a [StarTreeProjectPlan] built on the first star-tree that both matches `nullAware` and fits the query,
  /// or `null` if there is none.
  ///
  /// Resolves the function-column pairs against the same mode, because a null-aware star-tree stores `COUNT` per
  /// column while a regular one stores a single count of every row, and the executors have to read back whichever
  /// was projected.
  @Nullable
  private static StarTreeProjectPlan createProjectPlan(IndexSegment indexSegment, QueryContext queryContext,
      List<StarTreeV2> starTrees, boolean nullAware, AggregationFunction[] aggregationFunctions,
      @Nullable ExpressionContext[] groupByExpressions,
      Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap) {
    // Only `COUNT` resolves differently between the two, and never to `null`, so a query that cannot be represented
    // as pairs at all fails here for either kind of star-tree
    AggregationFunctionColumnPair[] functionColumnPairs =
        extractAggregationFunctionPairs(aggregationFunctions, nullAware);
    if (functionColumnPairs == null) {
      return null;
    }
    // A regular star-tree folded nulls into the column's default value and counted them, so it can only answer a
    // null-handling-on query when nothing the query touches is actually null
    if (!nullAware && queryContext.isNullHandlingEnabled() && !hasNoNullValues(indexSegment, aggregationFunctions,
        functionColumnPairs, predicateEvaluatorsMap.keySet(), groupByExpressions)) {
      return null;
    }

    List<Pair<AggregationFunction, AggregationFunctionColumnPair>> aggregations =
        new ArrayList<>(aggregationFunctions.length);
    for (int i = 0; i < aggregationFunctions.length; i++) {
      aggregations.add(Pair.of(aggregationFunctions[i], functionColumnPairs[i]));
    }

    for (StarTreeV2 starTreeV2 : starTrees) {
      StarTreeV2Metadata metadata = starTreeV2.getMetadata();
      if (metadata.isNullHandlingEnabled() != nullAware) {
        continue;
      }
      if (isFitForStarTree(metadata, aggregations, groupByExpressions, predicateEvaluatorsMap.keySet())) {
        BaseProjectOperator<?> projectOperator =
            new StarTreeProjectPlanNode(queryContext, starTreeV2, functionColumnPairs, groupByExpressions,
                predicateEvaluatorsMap).run();
        return new StarTreeProjectPlan(projectOperator, functionColumnPairs);
      }
    }
    return null;
  }

  /// Returns whether none of the columns the query touches contains a null value in this segment, in which case a
  /// regular star-tree produces the same result as a null-aware one and can serve a null-handling-on query.
  private static boolean hasNoNullValues(IndexSegment indexSegment, AggregationFunction[] aggregationFunctions,
      AggregationFunctionColumnPair[] functionColumnPairs, Set<String> predicateColumns,
      @Nullable ExpressionContext[] groupByExpressions) {
    for (int i = 0; i < functionColumnPairs.length; i++) {
      AggregationFunctionColumnPair functionColumnPair = functionColumnPairs[i];
      if (functionColumnPair == AggregationFunctionColumnPair.COUNT_STAR) {
        // COUNT aggregation function returns a non-empty input expressions list only when null handling is enabled
        // and the input operand is a non-star identifier or function. Null handling is irrelevant for COUNT(*),
        // COUNT(literal) and COUNT(nonNullColumn).
        List<ExpressionContext> inputExpressions = aggregationFunctions[i].getInputExpressions();
        if (!inputExpressions.isEmpty() && inputExpressions.get(0).getType() == ExpressionContext.Type.IDENTIFIER
            && !hasNoNullValues(indexSegment, inputExpressions.get(0).getIdentifier(), "aggregation")) {
          return false;
        }
        continue;
      }

      if (!hasNoNullValues(indexSegment, functionColumnPair.getColumn(), "aggregation")) {
        return false;
      }
    }

    for (String column : predicateColumns) {
      if (!hasNoNullValues(indexSegment, column, "filter")) {
        return false;
      }
    }

    Set<String> groupByColumns = new HashSet<>();
    if (groupByExpressions != null) {
      for (ExpressionContext groupByExpression : groupByExpressions) {
        groupByExpression.getColumns(groupByColumns);
      }
    }
    for (String column : groupByColumns) {
      if (!hasNoNullValues(indexSegment, column, "group-by")) {
        return false;
      }
    }
    return true;
  }

  private static boolean hasNoNullValues(IndexSegment indexSegment, String column, String columnRole) {
    DataSource dataSource = indexSegment.getDataSourceNullable(column);
    if (dataSource == null) {
      LOGGER.debug("Cannot use star-tree index because {} column: '{}' does not exist", columnRole, column);
      return false;
    }
    if (dataSource.getNullValueVector() != null && !dataSource.getNullValueVector().getNullBitmap().isEmpty()) {
      LOGGER.debug("Cannot use star-tree index because {} column: '{}' has null values", columnRole, column);
      return false;
    }
    return true;
  }
}
