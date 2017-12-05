/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.blocks.EmptyFilterBlock;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.core.startree.StarTreeNode;
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
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Filter operator based on star tree index.
 * <p>High-level algorithm:
 * <ul>
 *   <li>
 *     Traverse the filter tree and generate a map from column to a list of {@link PredicateEvaluator}s applied to it
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
 *     For each remaining predicate columns, use the list of {@link PredicateEvaluator}s to generate separate
 *     {@link BaseFilterOperator}s for it
 *   </li>
 *   <li>Conjoin all {@link BaseFilterOperator}s with AND if we have multiple of them</li>
 * </ul>
 */
public class StarTreeIndexBasedFilterOperator extends BaseFilterOperator {
  /**
   * Helper class to wrap the information needed when traversing the star tree.
   */
  private static class SearchEntry {
    StarTreeNode _starTreeNode;
    Set<String> _remainingPredicateColumns;
    Set<String> _remainingGroupByColumns;

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

  private static final String OPERATOR_NAME = "StarTreeIndexBasedFilterOperator";
  // If (number of matching dictionary ids * threshold) > (number of child nodes), use scan to traverse nodes instead of
  // binary search on each dictionary id
  private static final int USE_SCAN_TO_TRAVERSE_NODES_THRESHOLD = 10;

  private final IndexSegment _indexSegment;
  // Map from column to predicate evaluators
  private final Map<String, List<PredicateEvaluator>> _predicateEvaluatorsMap;
  // Map from column to matching dictionary ids
  private final Map<String, IntSet> _matchingDictIdsMap;
  // Set of group-by columns
  private final Set<String> _groupByColumns;

  boolean _resultEmpty = false;

  public StarTreeIndexBasedFilterOperator(IndexSegment indexSegment, BrokerRequest brokerRequest,
      FilterQueryTree rootFilterNode) {
    _indexSegment = indexSegment;
    _groupByColumns = RequestUtils.getAllGroupByColumns(brokerRequest.getGroupBy());

    if (rootFilterNode != null) {
      // Process the filter tree and get a map from column to a list of predicates applied to it
      Map<String, List<Predicate>> predicatesMap = processFilterTree(rootFilterNode);

      // Remove columns with predicates from group-by columns because we won't use star node for that column
      _groupByColumns.removeAll(predicatesMap.keySet());

      int numColumns = predicatesMap.size();
      _predicateEvaluatorsMap = new HashMap<>(numColumns);
      _matchingDictIdsMap = new HashMap<>(numColumns);

      // Initialize the predicate evaluators map
      for (Map.Entry<String, List<Predicate>> entry : predicatesMap.entrySet()) {
        String columnName = entry.getKey();
        List<Predicate> predicates = entry.getValue();
        List<PredicateEvaluator> predicateEvaluators = new ArrayList<>(predicates.size());

        DataSource dataSource = _indexSegment.getDataSource(columnName);
        for (Predicate predicate : predicates) {
          PredicateEvaluator predicateEvaluator =
              PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource);
          // If predicate is always evaluated false, the result for the filter operator will be empty, early terminate
          if (predicateEvaluator.isAlwaysFalse()) {
            _resultEmpty = true;
            return;
          }
          predicateEvaluators.add(predicateEvaluator);
        }
        _predicateEvaluatorsMap.put(columnName, predicateEvaluators);
      }
    } else {
      _predicateEvaluatorsMap = Collections.emptyMap();
      _matchingDictIdsMap = Collections.emptyMap();
    }
  }

  /**
   * Helper method to process the filter tree and get a map from column to a list of predicates applied to it.
   */
  private Map<String, List<Predicate>> processFilterTree(FilterQueryTree rootFilterNode) {
    Map<String, List<Predicate>> predicatesMap = new HashMap<>();
    Queue<FilterQueryTree> queue = new LinkedList<>();
    queue.add(rootFilterNode);

    while (!queue.isEmpty()) {
      FilterQueryTree filterNode = queue.remove();
      List<FilterQueryTree> children = filterNode.getChildren();
      if (children == null) {
        String columnName = filterNode.getColumn();
        Predicate predicate = Predicate.newPredicate(filterNode);
        List<Predicate> predicates = predicatesMap.get(columnName);
        if (predicates == null) {
          predicates = new ArrayList<>();
          predicatesMap.put(columnName, predicates);
        }
        predicates.add(predicate);
      } else {
        queue.addAll(children);
      }
    }

    return predicatesMap;
  }

  @Override
  public BaseFilterBlock getNextBlock() {
    if (_resultEmpty) {
      return EmptyFilterBlock.getInstance();
    }
    List<BaseFilterOperator> childFilterOperators = getChildFilterOperators();
    if (childFilterOperators.isEmpty()) {
      return EmptyFilterBlock.getInstance();
    } else if (childFilterOperators.size() == 1) {
      return childFilterOperators.get(0).nextBlock();
    } else {
      FilterOperatorUtils.reOrderFilterOperators(childFilterOperators);
      return new AndOperator(childFilterOperators).nextBlock();
    }
  }

  @Override
  public boolean isResultEmpty() {
    return _resultEmpty;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  /**
   * Helper method to get a list of child filter operators that match the matchingDictIdsMap.
   * <ul>
   *   <li>First go over the star tree and try to match as many columns as possible</li>
   *   <li>For the remaining columns, use other indexes to match them</li>
   * </ul>
   */
  private List<BaseFilterOperator> getChildFilterOperators() {
    StarTreeResult starTreeResult = traverseStarTree();

    // If star tree result is null, the result for the filter operator will be empty, early terminate
    if (starTreeResult == null) {
      return Collections.emptyList();
    }

    List<BaseFilterOperator> childFilterOperators =
        new ArrayList<>(1 + starTreeResult._remainingPredicateColumns.size());

    int startDocId = 0;
    // Inclusive end document id
    int endDocId = _indexSegment.getSegmentMetadata().getTotalDocs() - 1;

    // Add the bitmap of matching documents from star tree
    childFilterOperators.add(
        new BitmapBasedFilterOperator(new ImmutableRoaringBitmap[]{starTreeResult._matchedDocIds}, startDocId, endDocId,
            false));

    // Add remaining predicates
    for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
      List<PredicateEvaluator> predicateEvaluators = _predicateEvaluatorsMap.get(remainingPredicateColumn);
      DataSource dataSource = _indexSegment.getDataSource(remainingPredicateColumn);
      for (PredicateEvaluator predicateEvaluator : predicateEvaluators) {
        childFilterOperators.add(
            FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId));
      }
    }

    return childFilterOperators;
  }

  /**
   * Helper method to traverse the star tree, get matching documents and keep track of all the predicate columns that
   * are not matched.
   * <p>Return <code>null</code> if no matching dictionary id found for a column (i.e. the result for the filter
   * operator is empty).
   */
  private StarTreeResult traverseStarTree() {
    MutableRoaringBitmap matchedDocIds = new MutableRoaringBitmap();
    Set<String> remainingPredicateColumns = new HashSet<>();

    StarTree starTree = _indexSegment.getStarTree();
    List<String> dimensionNames = starTree.getDimensionNames();
    StarTreeNode starTreeRootNode = starTree.getRoot();

    // Use BFS to traverse the star tree
    Queue<SearchEntry> queue = new LinkedList<>();
    queue.add(new SearchEntry(starTreeRootNode, _predicateEvaluatorsMap.keySet(), _groupByColumns));
    while (!queue.isEmpty()) {
      SearchEntry searchEntry = queue.remove();
      StarTreeNode starTreeNode = searchEntry._starTreeNode;

      // If all predicate columns and group-by columns are matched, we can use aggregated document
      if (searchEntry._remainingPredicateColumns.isEmpty() && searchEntry._remainingGroupByColumns.isEmpty()) {
        matchedDocIds.add(starTreeNode.getAggregatedDocId());
      } else {
        // For leaf node, because we haven't exhausted all predicate columns and group-by columns, we cannot use
        // the aggregated document. Add the range of documents for this node to the bitmap, and keep track of the
        // remaining predicate columns for this node
        if (starTreeNode.isLeaf()) {
          matchedDocIds.add(starTreeNode.getStartDocId(), starTreeNode.getEndDocId());
          remainingPredicateColumns.addAll(searchEntry._remainingPredicateColumns);
        } else {
          // For non-leaf node, proceed to next level
          String nextDimension = dimensionNames.get(starTreeNode.getChildDimensionId());

          // If we have predicates on next level, add matching nodes to the queue
          if (searchEntry._remainingPredicateColumns.contains(nextDimension)) {
            Set<String> newRemainingPredicateColumns = new HashSet<>(searchEntry._remainingPredicateColumns);
            newRemainingPredicateColumns.remove(nextDimension);

            IntSet matchingDictIds = _matchingDictIdsMap.get(nextDimension);
            if (matchingDictIds == null) {
              matchingDictIds = getMatchingDictIds(_predicateEvaluatorsMap.get(nextDimension));

              // If no matching dictionary id found, directly return null
              if (matchingDictIds.isEmpty()) {
                return null;
              }

              _matchingDictIdsMap.put(nextDimension, matchingDictIds);
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

    return new StarTreeResult(matchedDocIds, remainingPredicateColumns);
  }

  /**
   * Helper method to get a set of matching dictionary ids from a list of predicate evaluators conjoined with AND.
   * <ul>
   *   <li>
   *     We sort all predicate evaluators with priority: EQ > IN > RANGE > NOT_IN/NEQ > REGEXP_LIKE so that we process
   *     less dictionary ids.
   *   </li>
   *   <li>
   *     For the first predicate evaluator, we get all the matching dictionary ids, then apply them to other predicate
   *     evaluators to get the final set of matching dictionary ids.
   *   </li>
   * </ul>
   */
  private IntSet getMatchingDictIds(List<PredicateEvaluator> predicateEvaluators) {
    // Sort the predicate evaluators so that we process less dictionary ids
    Collections.sort(predicateEvaluators, new Comparator<PredicateEvaluator>() {
      @Override
      public int compare(PredicateEvaluator o1, PredicateEvaluator o2) {
        return getPriority(o1) - getPriority(o2);
      }

      int getPriority(PredicateEvaluator predicateEvaluator) {
        switch (predicateEvaluator.getPredicateType()) {
          case EQ:
            return 1;
          case IN:
            return 2;
          case RANGE:
            return 3;
          case NOT_IN:
          case NEQ:
            return 4;
          case REGEXP_LIKE:
            return 5;
          default:
            throw new UnsupportedOperationException();
        }
      }
    });

    // Initialize matching dictionary ids with the first predicate evaluator
    IntSet matchingDictIds = new IntOpenHashSet();
    PredicateEvaluator firstPredicateEvaluator = predicateEvaluators.get(0);
    for (int matchingDictId : firstPredicateEvaluator.getMatchingDictIds()) {
      matchingDictIds.add(matchingDictId);
    }

    // Process other predicate evaluators
    int numPredicateEvaluators = predicateEvaluators.size();
    for (int i = 1; i < numPredicateEvaluators; i++) {
      // We don't need to apply other predicate evaluators if all matching dictionary ids have already been removed
      if (matchingDictIds.isEmpty()) {
        return matchingDictIds;
      }
      PredicateEvaluator predicateEvaluator = predicateEvaluators.get(i);
      IntIterator iterator = matchingDictIds.iterator();
      while (iterator.hasNext()) {
        if (!predicateEvaluator.applySV(iterator.nextInt())) {
          iterator.remove();
        }
      }
    }

    return matchingDictIds;
  }
}
