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

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;


@SuppressWarnings("rawtypes")
public class StarTreeUtils {
  private StarTreeUtils() {
  }

  public static final String USE_STAR_TREE_KEY = "useStarTree";

  /**
   * Returns whether star-tree is disabled for the query.
   */
  public static boolean isStarTreeDisabled(QueryContext queryContext) {
    Map<String, String> debugOptions = queryContext.getDebugOptions();
    return debugOptions != null && "false".equalsIgnoreCase(debugOptions.get(USE_STAR_TREE_KEY));
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
   */
  @Nullable
  public static Map<String, List<PredicateEvaluator>> extractPredicateEvaluatorsMap(IndexSegment indexSegment,
      @Nullable FilterContext filter) {
    if (filter == null) {
      return Collections.emptyMap();
    }

    Map<String, List<PredicateEvaluator>> predicateEvaluatorsMap = new HashMap<>();
    Queue<FilterContext> queue = new LinkedList<>();
    queue.add(filter);
    FilterContext filterNode;
    while ((filterNode = queue.poll()) != null) {
      switch (filterNode.getType()) {
        case AND:
          queue.addAll(filterNode.getChildren());
          break;
        case OR:
          // Star-tree does not support OR filter
          return null;
        case PREDICATE:
          Predicate predicate = filterNode.getPredicate();
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
          }
          PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider
              .getPredicateEvaluator(predicate, dictionary, dataSource.getDataSourceMetadata().getDataType());
          if (predicateEvaluator.isAlwaysFalse()) {
            // Do not use star-tree if there is no matching record
            return null;
          }
          if (!predicateEvaluator.isAlwaysTrue()) {
            predicateEvaluatorsMap.computeIfAbsent(column, k -> new ArrayList<>()).add(predicateEvaluator);
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
   * Returns {@code true} if the given star-tree builder configs do not match the star-tree metadata, in which case the
   * existing star-trees need to be removed, {@code false} otherwise.
   */
  public static boolean shouldRemoveExistingStarTrees(List<StarTreeV2BuilderConfig> builderConfigs,
      List<StarTreeV2Metadata> metadataList) {
    int numStarTrees = builderConfigs.size();
    if (metadataList.size() != numStarTrees) {
      return true;
    }
    for (int i = 0; i < numStarTrees; i++) {
      StarTreeV2BuilderConfig builderConfig = builderConfigs.get(i);
      StarTreeV2Metadata metadata = metadataList.get(i);
      if (!builderConfig.getDimensionsSplitOrder().equals(metadata.getDimensionsSplitOrder())) {
        return true;
      }
      if (!builderConfig.getSkipStarNodeCreationForDimensions()
          .equals(metadata.getSkipStarNodeCreationForDimensions())) {
        return true;
      }
      if (!builderConfig.getFunctionColumnPairs().equals(metadata.getFunctionColumnPairs())) {
        return true;
      }
      if (builderConfig.getMaxLeafRecords() != metadata.getMaxLeafRecords()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Removes all the star-trees from the given segment.
   */
  public static void removeStarTrees(File indexDir)
      throws Exception {
    File segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(indexDir);

    // Remove the star-tree metadata
    PropertiesConfiguration metadataProperties =
        CommonsConfigurationUtils.fromFile(new File(segmentDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    metadataProperties.subset(StarTreeV2Constants.MetadataKey.STAR_TREE_SUBSET).clear();
    // Commons Configuration 1.10 does not support file path containing '%'.
    // Explicitly providing the output stream for the file bypasses the problem.
    try (FileOutputStream fileOutputStream = new FileOutputStream(metadataProperties.getFile())) {
      metadataProperties.save(fileOutputStream);
    }

    // Remove the index file and index map file
    FileUtils.forceDelete(new File(segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME));
    FileUtils.forceDelete(new File(segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME));
  }
}
