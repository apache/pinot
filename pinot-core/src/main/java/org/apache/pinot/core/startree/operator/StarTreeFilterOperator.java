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
package org.apache.pinot.core.startree.operator;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.segment.spi.index.startree.StarTree;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Filter operator based on star tree index.
 * <p>High-level algorithm:
 * <ul>
 *   <li>
 *     Traverse the filter tree and generate a map from column to a list of {@link CompositePredicateEvaluator}s applied
 *     to it
 *   </li>
 *   <li>
 *     Traverse the star tree index, try to match as many predicates as possible, add the matching documents into a
 *     bitmap, and keep track of the remaining predicate columns and group-by columns
 *     <ul>
 *       <li>
 *         If we have predicates on the current dimension, calculate matching dictionary ids, add nodes associated with
 *         the matching dictionary ids
 *       </li>
 *       <li>If we don't have predicate but have group-by on the current dimension, add all non-star nodes</li>
 *       <li>If no predicate or group-by on the current dimension, use star node if exists, or all non-star nodes</li>
 *       <li>If all predicates and group-by are matched, add the aggregated document</li>
 *       <li>If we have remaining predicates or group-by at leaf node, add the documents from start to end</li>
 *       <li>
 *         If we have remaining predicates at leaf node, store the column because we need separate
 *         {@link BaseFilterOperator}s for it
 *       </li>
 *       <li>Generate a {@link BitmapBasedFilterOperator} using the matching documents bitmap</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For each remaining predicate columns, use the list of {@link CompositePredicateEvaluator}s to generate separate
 *     {@link BaseFilterOperator}s for it
 *   </li>
 *   <li>Conjoin all {@link BaseFilterOperator}s with AND if we have multiple of them</li>
 * </ul>
 */
public class StarTreeFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_STARTREE_INDEX";

  /**
   * Helper class to wrap the result from traversing the star tree.
   */
  private static class StarTreeResult {
    final ImmutableRoaringBitmap _matchedDocIds;
    final Set<String> _remainingPredicateColumns;

    StarTreeResult(ImmutableRoaringBitmap matchedDocIds, Set<String> remainingPredicateColumns) {
      _matchedDocIds = matchedDocIds;
      _remainingPredicateColumns = remainingPredicateColumns;
    }
  }

  // If (number of matching dictionary ids * threshold) > (number of child nodes), use scan to traverse nodes instead of
  // binary search on each dictionary id
  private static final int USE_SCAN_TO_TRAVERSE_NODES_THRESHOLD = 10;

  private final QueryContext _queryContext;
  private final StarTreeV2 _starTreeV2;
  private final Map<String, List<CompositePredicateEvaluator>> _predicateEvaluatorsMap;
  private final Set<String> _groupByColumns;
  private final boolean _scanStarTreeNodes;

  boolean _resultEmpty = false;

  public StarTreeFilterOperator(QueryContext queryContext, StarTreeV2 starTreeV2,
      Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap, @Nullable Set<String> groupByColumns) {
    // This filter operator does not support AND/OR/NOT operations.
    super(0, NullMode.NONE_NULLABLE);
    _queryContext = queryContext;
    _starTreeV2 = starTreeV2;
    _predicateEvaluatorsMap = predicateEvaluatorsMap;
    _groupByColumns = groupByColumns != null ? groupByColumns : Collections.emptySet();
    _scanStarTreeNodes = QueryOptionsUtils.isScanStarTreeNodes(_queryContext.getQueryOptions());
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_resultEmpty) {
      return EmptyDocIdSet.getInstance();
    }
    return getFilterOperator().nextBlock().getBlockDocIdSet();
  }

  @Override
  public boolean isResultEmpty() {
    return _resultEmpty;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  /**
   * Helper method to get a filter operator that match the matchingDictIdsMap.
   * <ul>
   *   <li>First go over the star tree and try to match as many columns as possible</li>
   *   <li>For the remaining columns, use other indexes to match them</li>
   * </ul>
   */
  private BaseFilterOperator getFilterOperator() {
    StarTreeResult starTreeResult = traverseStarTree();

    // If star tree result is null, the result for the filter operator will be empty, early terminate
    if (starTreeResult == null) {
      return EmptyFilterOperator.getInstance();
    }

    int numDocs = _starTreeV2.getMetadata().getNumDocs();
    List<BaseFilterOperator> childFilterOperators =
        new ArrayList<>(1 + starTreeResult._remainingPredicateColumns.size());

    // Add the bitmap of matching documents from star tree
    childFilterOperators.add(new BitmapBasedFilterOperator(starTreeResult._matchedDocIds, false, numDocs));

    // Add remaining predicates
    for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
      List<CompositePredicateEvaluator> compositePredicateEvaluators =
          _predicateEvaluatorsMap.get(remainingPredicateColumn);
      DataSource dataSource = _starTreeV2.getDataSource(remainingPredicateColumn);
      for (CompositePredicateEvaluator compositePredicateEvaluator : compositePredicateEvaluators) {
        List<PredicateEvaluator> predicateEvaluators = compositePredicateEvaluator.getPredicateEvaluators();
        int numPredicateEvaluators = predicateEvaluators.size();
        if (numPredicateEvaluators == 1) {
          // Single predicate evaluator
          childFilterOperators.add(
              FilterOperatorUtils.getLeafFilterOperator(_queryContext, predicateEvaluators.get(0), dataSource,
                  numDocs));
        } else {
          // Predicate evaluators conjoined with OR
          List<BaseFilterOperator> orChildFilterOperators = new ArrayList<>(numPredicateEvaluators);
          for (PredicateEvaluator childPredicateEvaluator : predicateEvaluators) {
            orChildFilterOperators.add(
                FilterOperatorUtils.getLeafFilterOperator(_queryContext, childPredicateEvaluator, dataSource, numDocs));
          }
          childFilterOperators.add(
              FilterOperatorUtils.getOrFilterOperator(_queryContext, orChildFilterOperators, numDocs));
        }
      }
    }

    return FilterOperatorUtils.getAndFilterOperator(_queryContext, childFilterOperators, numDocs);
  }

  /**
   * Helper method to traverse the star tree, get matching documents and keep track of all the predicate columns that
   * are not matched. Returns {@code null} if no matching dictionary id found for a column (i.e. the result for the
   * filter operator is empty).
   */
  @Nullable
  private StarTreeResult traverseStarTree() {
    MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
    Set<String> globalRemainingPredicateColumns = null;

    StarTree starTree = _starTreeV2.getStarTree();
    List<String> dimensionNames = starTree.getDimensionNames();
    StarTreeNode starTreeRootNode = starTree.getRoot();

    // Track whether we have found a leaf node added to the queue. If we have found a leaf node, and traversed to the
    // level of the leave node, we can set globalRemainingPredicateColumns if not already set because we know the leaf
    // node won't split further on other predicate columns.
    boolean foundLeafNode = starTreeRootNode.isLeaf();

    // Use BFS to traverse the star tree
    Queue<StarTreeNode> queue = new ArrayDeque<>();
    queue.add(starTreeRootNode);
    int currentDimensionId = -1;
    Set<String> remainingPredicateColumns = new HashSet<>(_predicateEvaluatorsMap.keySet());
    Set<String> remainingGroupByColumns = new HashSet<>(_groupByColumns);
    if (foundLeafNode) {
      globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
    }
    IntSet matchingDictIds = null;
    StarTreeNode starTreeNode;
    while ((starTreeNode = queue.poll()) != null) {
      int dimensionId = starTreeNode.getDimensionId();
      if (dimensionId > currentDimensionId) {
        // Previous level finished
        String dimension = dimensionNames.get(dimensionId);
        remainingPredicateColumns.remove(dimension);
        remainingGroupByColumns.remove(dimension);
        if (foundLeafNode && globalRemainingPredicateColumns == null) {
          globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
        }
        matchingDictIds = null;
        currentDimensionId = dimensionId;
      }

      // If all predicate columns and group-by columns are matched, we can use aggregated document
      if (remainingPredicateColumns.isEmpty() && remainingGroupByColumns.isEmpty()) {
        matchingDocIds.add(starTreeNode.getAggregatedDocId());
        continue;
      }

      // For leaf node, because we haven't exhausted all predicate columns and group-by columns, we cannot use
      // the aggregated document. Add the range of documents for this node to the bitmap, and keep track of the
      // remaining predicate columns for this node
      if (starTreeNode.isLeaf()) {
        matchingDocIds.add((long) starTreeNode.getStartDocId(), starTreeNode.getEndDocId());
        continue;
      }

      // For non-leaf node, proceed to next level
      String childDimension = dimensionNames.get(dimensionId + 1);

      // Only read star-node when the dimension is not in the global remaining predicate columns or group-by columns
      // because we cannot use star-node in such cases
      StarTreeNode starNode = null;
      if ((globalRemainingPredicateColumns == null || !globalRemainingPredicateColumns.contains(childDimension))
          && !remainingGroupByColumns.contains(childDimension)) {
        starNode = starTreeNode.getChildForDimensionValue(StarTreeNode.ALL);
      }

      if (remainingPredicateColumns.contains(childDimension)) {
        // Have predicates on the next level, add matching nodes to the queue

        // Calculate the matching dictionary ids for the child dimension
        if (matchingDictIds == null) {
          matchingDictIds = getMatchingDictIds(_predicateEvaluatorsMap.get(childDimension));

          // If no matching dictionary id found, directly return null
          if (matchingDictIds.isEmpty()) {
            return null;
          }
        }

        int numMatchingDictIds = matchingDictIds.size();
        int numChildren = starTreeNode.getNumChildren();

        // If number of matching dictionary ids is large, use scan instead of binary search
        if (numMatchingDictIds * USE_SCAN_TO_TRAVERSE_NODES_THRESHOLD > numChildren || _scanStarTreeNodes) {
          Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();

          // When the star-node exists, and the number of matching dictionary ids is more than or equal to the
          // number of non-star child nodes, check if all the child nodes match the predicate, and use the
          // star-node if so
          if (starNode != null && numMatchingDictIds >= numChildren - 1) {
            List<StarTreeNode> matchingChildNodes = new ArrayList<>();
            boolean findLeafChildNode = false;
            while (childrenIterator.hasNext()) {
              StarTreeNode childNode = childrenIterator.next();
              if (matchingDictIds.contains(childNode.getDimensionValue())) {
                matchingChildNodes.add(childNode);
                findLeafChildNode |= childNode.isLeaf();
              }
            }
            if (matchingChildNodes.size() == numChildren - 1) {
              // All the child nodes (except for the star-node) match the predicate, use the star-node
              queue.add(starNode);
              foundLeafNode |= starNode.isLeaf();
            } else {
              // Some child nodes do not match the predicate, use the matching child nodes
              queue.addAll(matchingChildNodes);
              foundLeafNode |= findLeafChildNode;
            }
          } else {
            // Cannot use the star-node, use the matching child nodes
            while (childrenIterator.hasNext()) {
              StarTreeNode childNode = childrenIterator.next();
              if (matchingDictIds.contains(childNode.getDimensionValue())) {
                queue.add(childNode);
                foundLeafNode |= childNode.isLeaf();
              }
            }
          }
        } else {
          IntIterator iterator = matchingDictIds.iterator();
          while (iterator.hasNext()) {
            int matchingDictId = iterator.nextInt();
            StarTreeNode childNode = starTreeNode.getChildForDimensionValue(matchingDictId);

            // Child node might be null because the matching dictionary id might not exist under this branch
            if (childNode != null) {
              queue.add(childNode);
              foundLeafNode |= childNode.isLeaf();
            }
          }
        }
      } else {
        // No predicate on the next level

        if (starNode != null) {
          // Star-node exists, use it
          queue.add(starNode);
          foundLeafNode |= starNode.isLeaf();
        } else {
          // Star-node does not exist or cannot be used, add all non-star nodes to the queue
          Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
          while (childrenIterator.hasNext()) {
            StarTreeNode childNode = childrenIterator.next();
            if (childNode.getDimensionValue() != StarTreeNode.ALL) {
              queue.add(childNode);
              foundLeafNode |= childNode.isLeaf();
            }
          }
        }
      }
    }

    return new StarTreeResult(matchingDocIds,
        globalRemainingPredicateColumns != null ? globalRemainingPredicateColumns : Collections.emptySet());
  }

  /**
   * Helper method to get a set of matching dictionary ids from a list of composite predicate evaluators conjoined with
   * AND.
   * <p>When there are multiple composite predicate evaluators:
   * <ul>
   *   <li>
   *     We sort all composite predicate evaluators with priority: EQ > IN > RANGE > NOT_IN/NEQ > REGEXP_LIKE > multiple
   *     predicate evaluators conjoined with OR so that we process less dictionary ids.
   *   </li>
   *   <li>
   *     For the first composite predicate evaluator, we get all the matching dictionary ids, then apply them to other
   *     composite predicate evaluators to get the final set of matching dictionary ids.
   *   </li>
   * </ul>
   */
  private IntSet getMatchingDictIds(List<CompositePredicateEvaluator> compositePredicateEvaluators) {
    int numCompositePredicateEvaluators = compositePredicateEvaluators.size();
    if (numCompositePredicateEvaluators == 1) {
      return getMatchingDictIds(compositePredicateEvaluators.get(0));
    }

    // Sort the predicate evaluators so that we process less dictionary ids
    compositePredicateEvaluators.sort(new Comparator<CompositePredicateEvaluator>() {
      @Override
      public int compare(CompositePredicateEvaluator o1, CompositePredicateEvaluator o2) {
        return getPriority(o1) - getPriority(o2);
      }

      int getPriority(CompositePredicateEvaluator compositePredicateEvaluator) {
        List<PredicateEvaluator> predicateEvaluators = compositePredicateEvaluator.getPredicateEvaluators();
        if (predicateEvaluators.size() == 1) {
          switch (predicateEvaluators.get(0).getPredicateType()) {
            case EQ:
              return 1;
            case IN:
              return 2;
            case RANGE:
              return 3;
            case NOT_EQ:
            case NOT_IN:
              return 4;
            default:
              throw new UnsupportedOperationException();
          }
        } else {
          // Process OR at last
          return 5;
        }
      }
    });

    // Initialize matching dictionary ids with the first predicate evaluator
    IntSet matchingDictIds = getMatchingDictIds(compositePredicateEvaluators.get(0));

    // Process other predicate evaluators
    for (int i = 1; i < numCompositePredicateEvaluators; i++) {
      // We don't need to apply other predicate evaluators if all matching dictionary ids have already been removed
      if (matchingDictIds.isEmpty()) {
        return matchingDictIds;
      }
      CompositePredicateEvaluator compositePredicateEvaluator = compositePredicateEvaluators.get(i);
      IntIterator iterator = matchingDictIds.iterator();
      while (iterator.hasNext()) {
        if (!compositePredicateEvaluator.apply(iterator.nextInt())) {
          iterator.remove();
        }
      }
    }

    return matchingDictIds;
  }

  /**
   * Returns the matching dictionary ids for the given composite predicate evaluator.
   */
  private IntSet getMatchingDictIds(CompositePredicateEvaluator compositePredicateEvaluator) {
    IntSet matchingDictIds = new IntOpenHashSet();
    for (PredicateEvaluator predicateEvaluator : compositePredicateEvaluator.getPredicateEvaluators()) {
      for (int matchingDictId : predicateEvaluator.getMatchingDictIds()) {
        matchingDictIds.add(matchingDictId);
      }
    }
    return matchingDictIds;
  }
}
