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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.blocks.EmptyFilterBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
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
   * Helper class to wrap the information needed when traversing the star tree.
   */
  private static class SearchEntry {
    final StarTreeNode _starTreeNode;
    final Set<String> _remainingPredicateColumns;
    final Set<String> _remainingGroupByColumns;

    SearchEntry(StarTreeNode starTreeNode, Set<String> remainingPredicateColumns, Set<String> remainingGroupByColumns) {
      _starTreeNode = starTreeNode;
      _remainingPredicateColumns = remainingPredicateColumns;
      _remainingGroupByColumns = remainingGroupByColumns;
    }
  }

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

  private static final String OPERATOR_NAME = "StarTreeFilterOperator";
  // If (number of matching dictionary ids * threshold) > (number of child nodes), use scan to traverse nodes instead of
  // binary search on each dictionary id
  private static final int USE_SCAN_TO_TRAVERSE_NODES_THRESHOLD = 10;

  // Star-tree
  private final StarTreeV2 _starTreeV2;
  // Map from column to predicate evaluators
  private final Map<String, List<CompositePredicateEvaluator>> _predicateEvaluatorsMap;
  // Set of group-by columns
  private final Set<String> _groupByColumns;

  private final Map<String, String> _debugOptions;

  boolean _resultEmpty = false;

  public StarTreeFilterOperator(StarTreeV2 starTreeV2,
      Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap, Set<String> groupByColumns,
      @Nullable Map<String, String> debugOptions) {
    _starTreeV2 = starTreeV2;
    _predicateEvaluatorsMap = predicateEvaluatorsMap;
    _debugOptions = debugOptions;

    if (groupByColumns != null) {
      _groupByColumns = new HashSet<>(groupByColumns);
      // Remove columns with predicates from group-by columns because we won't use star node for that column
      _groupByColumns.removeAll(_predicateEvaluatorsMap.keySet());
    } else {
      _groupByColumns = Collections.emptySet();
    }
  }

  @Override
  public FilterBlock getNextBlock() {
    if (_resultEmpty) {
      return EmptyFilterBlock.getInstance();
    }
    return getFilterOperator().nextBlock();
  }

  @Override
  public boolean isResultEmpty() {
    return _resultEmpty;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public String getExplainPlanName() {
    return EXPLAIN_NAME;
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
          childFilterOperators
              .add(FilterOperatorUtils.getLeafFilterOperator(predicateEvaluators.get(0), dataSource, numDocs));
        } else {
          // Predicate evaluators conjoined with OR
          List<BaseFilterOperator> orChildFilterOperators = new ArrayList<>(numPredicateEvaluators);
          for (PredicateEvaluator childPredicateEvaluator : predicateEvaluators) {
            orChildFilterOperators
                .add(FilterOperatorUtils.getLeafFilterOperator(childPredicateEvaluator, dataSource, numDocs));
          }
          childFilterOperators
              .add(FilterOperatorUtils.getOrFilterOperator(orChildFilterOperators, numDocs, _debugOptions));
        }
      }
    }

    return FilterOperatorUtils.getAndFilterOperator(childFilterOperators, numDocs, _debugOptions);
  }

  /**
   * Helper method to traverse the star tree, get matching documents and keep track of all the predicate columns that
   * are not matched. Returns {@code null} if no matching dictionary id found for a column (i.e. the result for the
   * filter operator is empty).
   */
  @Nullable
  private StarTreeResult traverseStarTree() {
    MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
    Set<String> remainingPredicateColumns = new HashSet<>();
    Map<String, IntSet> matchingDictIdsMap = new HashMap<>();

    StarTree starTree = _starTreeV2.getStarTree();
    List<String> dimensionNames = starTree.getDimensionNames();
    StarTreeNode starTreeRootNode = starTree.getRoot();

    // Use BFS to traverse the star tree
    Queue<SearchEntry> queue = new LinkedList<>();
    queue.add(new SearchEntry(starTreeRootNode, _predicateEvaluatorsMap.keySet(), _groupByColumns));
    SearchEntry searchEntry;
    while ((searchEntry = queue.poll()) != null) {
      StarTreeNode starTreeNode = searchEntry._starTreeNode;

      // If all predicate columns and group-by columns are matched, we can use aggregated document
      if (searchEntry._remainingPredicateColumns.isEmpty() && searchEntry._remainingGroupByColumns.isEmpty()) {
        matchingDocIds.add(starTreeNode.getAggregatedDocId());
      } else {
        // For leaf node, because we haven't exhausted all predicate columns and group-by columns, we cannot use
        // the aggregated document. Add the range of documents for this node to the bitmap, and keep track of the
        // remaining predicate columns for this node
        if (starTreeNode.isLeaf()) {
          matchingDocIds.add((long) starTreeNode.getStartDocId(), starTreeNode.getEndDocId());
          remainingPredicateColumns.addAll(searchEntry._remainingPredicateColumns);
        } else {
          // For non-leaf node, proceed to next level
          String nextDimension = dimensionNames.get(starTreeNode.getChildDimensionId());

          // If we have predicates on next level, add matching nodes to the queue
          if (searchEntry._remainingPredicateColumns.contains(nextDimension)) {
            Set<String> newRemainingPredicateColumns = new HashSet<>(searchEntry._remainingPredicateColumns);
            newRemainingPredicateColumns.remove(nextDimension);

            IntSet matchingDictIds = matchingDictIdsMap.get(nextDimension);
            if (matchingDictIds == null) {
              matchingDictIds = getMatchingDictIds(_predicateEvaluatorsMap.get(nextDimension));

              // If no matching dictionary id found, directly return null
              if (matchingDictIds.isEmpty()) {
                return null;
              }

              matchingDictIdsMap.put(nextDimension, matchingDictIds);
            }

            int numMatchingDictIds = matchingDictIds.size();
            int numChildren = starTreeNode.getNumChildren();

            // If number of matching dictionary ids is large, use scan instead of binary search
            if (numMatchingDictIds * USE_SCAN_TO_TRAVERSE_NODES_THRESHOLD > numChildren) {
              Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
              while (childrenIterator.hasNext()) {
                StarTreeNode childNode = childrenIterator.next();
                if (matchingDictIds.contains(childNode.getDimensionValue())) {
                  queue.add(
                      new SearchEntry(childNode, newRemainingPredicateColumns, searchEntry._remainingGroupByColumns));
                }
              }
            } else {
              IntIterator iterator = matchingDictIds.iterator();
              while (iterator.hasNext()) {
                int matchingDictId = iterator.nextInt();
                StarTreeNode childNode = starTreeNode.getChildForDimensionValue(matchingDictId);

                // Child node might be null because the matching dictionary id might not exist under this branch
                if (childNode != null) {
                  queue.add(
                      new SearchEntry(childNode, newRemainingPredicateColumns, searchEntry._remainingGroupByColumns));
                }
              }
            }
          } else {
            // If we don't have predicate or group-by on next level, use star node if exists
            Set<String> newRemainingGroupByColumns;
            if (!searchEntry._remainingGroupByColumns.contains(nextDimension)) {
              StarTreeNode starNode = starTreeNode.getChildForDimensionValue(StarTreeNode.ALL);
              if (starNode != null) {
                queue.add(new SearchEntry(starNode, searchEntry._remainingPredicateColumns,
                    searchEntry._remainingGroupByColumns));
                continue;
              }
              newRemainingGroupByColumns = searchEntry._remainingGroupByColumns;
            } else {
              newRemainingGroupByColumns = new HashSet<>(searchEntry._remainingGroupByColumns);
              newRemainingGroupByColumns.remove(nextDimension);
            }

            // Add all non-star nodes to the queue if cannot use star node
            Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
            while (childrenIterator.hasNext()) {
              StarTreeNode childNode = childrenIterator.next();
              if (childNode.getDimensionValue() != StarTreeNode.ALL) {
                queue.add(
                    new SearchEntry(childNode, searchEntry._remainingPredicateColumns, newRemainingGroupByColumns));
              }
            }
          }
        }
      }
    }

    return new StarTreeResult(matchingDocIds, remainingPredicateColumns);
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
