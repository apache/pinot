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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;


@SuppressWarnings("rawtypes")
public class StarTreeUtils {
  private StarTreeUtils() {
  }

  /**
   * Extracts the {@link AggregationFunctionColumnPair}s from the given {@link AggregationFunction}s. Returns
   * {@code null} if any {@link AggregationFunction} cannot be represented as an {@link AggregationFunctionColumnPair}
   * (e.g. has multiple arguments, argument is not column etc.).
   */
  @Nullable
  public static AggregationFunctionColumnPair[] extractAggregationFunctionPairs(
      AggregationFunction[] aggregationFunctions) {
    int numAggregationFunctions = aggregationFunctions.length;
    AggregationFunctionColumnPair[] aggregationFunctionColumnPairs =
        new AggregationFunctionColumnPair[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunctionColumnPair aggregationFunctionColumnPair =
          AggregationFunctionUtils.getAggregationFunctionColumnPair(aggregationFunctions[i]);
      if (aggregationFunctionColumnPair != null) {
        aggregationFunctionColumnPairs[i] = aggregationFunctionColumnPair;
      } else {
        return null;
      }
    }
    return aggregationFunctionColumnPairs;
  }

  /**
   * Extracts a map from the column to a list of {@link PredicateEvaluator}s for it. Returns {@code null} if the filter
   * cannot be solved by the star-tree.
   *
   * A predicate can be simple (d1 > 10) or composite (d1 > 10 AND d2 < 50) or multi levelled
   * (d1 > 50 AND (d2 > 10 OR d2 < 35)).
   * This method represents a list of CompositePredicates per dimension. For each dimension, all CompositePredicates in
   * the list are implicitly ANDed together. Any OR predicates are nested within a CompositePredicate.
   *
   * A map from predicates to their evaluators is passed in to accelerate the computation.
   */
  @Nullable
  public static Map<String, List<CompositePredicateEvaluator>> extractPredicateEvaluatorsMap(IndexSegment indexSegment,
      @Nullable FilterContext filter, List<Pair<Predicate, PredicateEvaluator>> predicateEvaluatorMapping) {
    if (filter == null) {
      return Collections.emptyMap();
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
          Pair<String, List<PredicateEvaluator>> pair =
              isOrClauseValidForStarTree(indexSegment, filterNode, predicateEvaluatorMapping);
          if (pair == null) {
            return null;
          }
          List<PredicateEvaluator> predicateEvaluators = pair.getRight();
          // NOTE: Empty list means always true
          if (!predicateEvaluators.isEmpty()) {
            predicateEvaluatorsMap.computeIfAbsent(pair.getLeft(), k -> new ArrayList<>())
                .add(new CompositePredicateEvaluator(predicateEvaluators));
          }
          break;
        case NOT:
          // TODO: Support NOT in star-tree
          return null;
        case PREDICATE:
          Predicate predicate = filterNode.getPredicate();
          PredicateEvaluator predicateEvaluator = getPredicateEvaluator(indexSegment, predicate,
              predicateEvaluatorMapping);
          // Do not use star-tree when the predicate cannot be solved with star-tree or is always false
          if (predicateEvaluator == null || predicateEvaluator.isAlwaysFalse()) {
            return null;
          }
          if (!predicateEvaluator.isAlwaysTrue()) {
            predicateEvaluatorsMap.computeIfAbsent(predicate.getLhs().getIdentifier(), k -> new ArrayList<>())
                .add(new CompositePredicateEvaluator(Collections.singletonList(predicateEvaluator)));
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return predicateEvaluatorsMap;
  }

  /**
   * Returns whether the query is fit for star tree index.
   * <p>The query is fit for star tree index if the following conditions are met:
   * <ul>
   *   <li>Star-tree contains all aggregation function column pairs</li>
   *   <li>All predicate columns and group-by columns are star-tree dimensions</li>
   * </ul>
   */
  public static boolean isFitForStarTree(StarTreeV2Metadata starTreeV2Metadata,
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs, @Nullable ExpressionContext[] groupByExpressions,
      Set<String> predicateColumns) {
    // Check aggregations
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      if (!starTreeV2Metadata.containsFunctionColumnPair(aggregationFunctionColumnPair)) {
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

  /**
   * Evaluates whether the given OR clause is valid for StarTree processing.
   * StarTree supports OR predicates on a single dimension only (d1 < 10 OR d1 > 50).
   *
   * @return The pair of single identifier and predicate evaluators applied to it if true; {@code null} if the OR clause
   *         cannot be solved with star-tree; empty predicate evaluator list if the OR clause always evaluates to true.
   */
  @Nullable
  private static Pair<String, List<PredicateEvaluator>> isOrClauseValidForStarTree(IndexSegment indexSegment,
      FilterContext filter, List<Pair<Predicate, PredicateEvaluator>> predicateEvaluatorMapping) {
    assert filter.getType() == FilterContext.Type.OR;

    List<Predicate> predicates = new ArrayList<>();
    if (!extractOrClausePredicates(filter, predicates)) {
      return null;
    }

    String identifier = null;
    List<PredicateEvaluator> predicateEvaluators = new ArrayList<>();
    for (Predicate predicate : predicates) {
      PredicateEvaluator predicateEvaluator = getPredicateEvaluator(indexSegment, predicate, predicateEvaluatorMapping);
      if (predicateEvaluator == null) {
        // The predicate cannot be solved with star-tree
        return null;
      }
      if (predicateEvaluator.isAlwaysTrue()) {
        // Use empty predicate evaluators to represent always true
        return Pair.of(null, Collections.emptyList());
      }
      if (!predicateEvaluator.isAlwaysFalse()) {
        String predicateIdentifier = predicate.getLhs().getIdentifier();
        if (identifier == null) {
          identifier = predicateIdentifier;
        } else {
          if (!identifier.equals(predicateIdentifier)) {
            // The predicates are applied to multiple columns
            return null;
          }
        }
        predicateEvaluators.add(predicateEvaluator);
      }
    }
    // When all predicates are always false, do not use star-tree
    if (predicateEvaluators.isEmpty()) {
      return null;
    }
    return Pair.of(identifier, predicateEvaluators);
  }

  /**
   * Extracts the predicates under the given OR clause, returns {@code false} if there is nested AND or NOT under OR
   * clause.
   * TODO: Support NOT in star-tree
   */
  private static boolean extractOrClausePredicates(FilterContext filter, List<Predicate> predicates) {
    assert filter.getType() == FilterContext.Type.OR;

    for (FilterContext child : filter.getChildren()) {
      switch (child.getType()) {
        case AND:
        case NOT:
          return false;
        case OR:
          if (!extractOrClausePredicates(child, predicates)) {
            return false;
          }
          break;
        case PREDICATE:
          predicates.add(child.getPredicate());
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return true;
  }

  /**
   * Returns the predicate evaluator for the given predicate, or {@code null} if the predicate cannot be solved with
   * star-tree.
   */
  @Nullable
  private static PredicateEvaluator getPredicateEvaluator(IndexSegment indexSegment, Predicate predicate,
      List<Pair<Predicate, PredicateEvaluator>> predicatesEvaluatorMapping) {
    ExpressionContext lhs = predicate.getLhs();
    if (lhs.getType() != ExpressionContext.Type.IDENTIFIER) {
      // Star-tree does not support non-identifier expression
      return null;
    }
    String column = lhs.getIdentifier();
    DataSource dataSource = indexSegment.getDataSource(column);
    Dictionary dictionary = dataSource.getDictionary();
    if (dictionary == null) {
      // Star-tree does not support non-dictionary encoded dimension
      return null;
    }
    switch (predicate.getType()) {
      // Do not use star-tree for the following predicates because:
      //   - REGEXP_LIKE: Need to scan the whole dictionary to gather the matching dictionary ids
      //   - TEXT_MATCH/IS_NULL/IS_NOT_NULL: No way to gather the matching dictionary ids
      case REGEXP_LIKE:
      case TEXT_MATCH:
      case IS_NULL:
      case IS_NOT_NULL:
        return null;
      default:
        break;
    }
    for (Pair<Predicate, PredicateEvaluator> pair : predicatesEvaluatorMapping) {
      if (pair.getKey().equals(predicate)) {
        return pair.getValue();
      }
    }
    return null;
  }
}
