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
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.startree.StarTreeIndexNodeInterf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarTreeIndexOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexOperator.class);
  private static final String OPERATOR_NAME = "StarTreeIndexOperator";

  private final int numRawDocs;
  private IndexSegment segment;

  // Predicates map
  Map<String, PredicateEntry> predicatesMap;

  // Group by columns
  Set<String> groupByColumns;

  // Columns with predicate on them
  Set<String> predicateColumns;

  boolean emptyResult = false;
  private BrokerRequest brokerRequest;

  public StarTreeIndexOperator(IndexSegment segment, BrokerRequest brokerRequest) {
    this.segment = segment;
    numRawDocs = segment.getSegmentMetadata().getTotalRawDocs();
    this.brokerRequest = brokerRequest;
    predicateColumns = new HashSet<>();
    groupByColumns = new HashSet<>();
    predicatesMap = new HashMap<>();
    initPredicatesToEvaluate();
  }

  private void initPredicatesToEvaluate() {
    FilterQueryTree filterTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    // Find all filter columns
    if (filterTree != null) {
      if (filterTree.getChildren() != null && !filterTree.getChildren().isEmpty()) {
        for (FilterQueryTree childFilter : filterTree.getChildren()) {
          // Nested filters are not supported
          assert childFilter.getChildren() == null || childFilter.getChildren().isEmpty();
          processFilterTree(childFilter);
        }
      } else {
        processFilterTree(filterTree);
      }
    }
    // Group by columns, we cannot lose group by columns during traversal
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      groupByColumns.addAll(groupBy.getColumns());
    }
  }

  private void processFilterTree(FilterQueryTree childFilter) {
    String column = childFilter.getColumn();
    // Only equality predicates are supported
    Predicate predicate = Predicate.newPredicate(childFilter);
    DataSource dataSource = segment.getDataSource(column);
    Dictionary dictionary = dataSource.getDictionary();
    PredicateEntry predicateEntry = null;

    PredicateEvaluator predicateEvaluator =
        PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dataSource);

    // If dictionary does not have any values that satisfy the predicate, set emptyResults to
    // true.
    if (predicateEvaluator.alwaysFalse()) {
      emptyResult = true;
    }

    // Store this predicate, we will have to apply it later
    predicateEntry = new PredicateEntry(predicate, predicateEvaluator);
    predicateColumns.add(column);
    predicatesMap.put(column, predicateEntry);
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId blockId) {
    MutableRoaringBitmap finalResult = null;
    if (emptyResult) {
      finalResult = new MutableRoaringBitmap();
      final BitmapDocIdIterator bitmapDocIdIterator =
          new BitmapDocIdIterator(finalResult.getIntIterator());
      return createBaseFilterBlock(bitmapDocIdIterator);
    }

    List<BaseFilterOperator> matchingLeafOperators = buildMatchingLeafOperators();
    if (matchingLeafOperators.size() == 1) {
      BaseFilterOperator baseFilterOperator = (BaseFilterOperator) matchingLeafOperators.get(0);
      return baseFilterOperator.nextFilterBlock(blockId);
    } else {
      CompositeOperator compositeOperator = new CompositeOperator(matchingLeafOperators);
      return compositeOperator.nextFilterBlock(blockId);
    }
  }

  @Override
  public boolean isResultEmpty() {
    return false;
  }

  /**
   * Helper method to build a list of operators for matching leaf nodes.
   * - Finds all leaf nodes that match the predicates
   * - Iterates over all the matching leaf nodes, and generate a list of matching ranges
   * @return
   */
  private List<BaseFilterOperator> buildMatchingLeafOperators() {
    int totalDocsToScan = 0;
    int numExactlyMatched = 0;
    long start = System.currentTimeMillis();

    final MutableRoaringBitmap exactlyMatchedDocsBitmap = new MutableRoaringBitmap();
    Queue<SearchEntry> matchedEntries = findMatchingLeafNodes();

    // Iterate over the matching nodes. For each column, generate the list of ranges.
    List<BaseFilterOperator> matchingLeafOperators = new ArrayList<>();
    for (SearchEntry matchedEntry : matchedEntries) {
      BaseFilterOperator matchingLeafOperator = null;
      StarTreeIndexNodeInterf matchedLeafNode = matchedEntry.starTreeIndexnode;

      int startDocId = matchedLeafNode.getStartDocumentId();
      int endDocId = matchedLeafNode.getEndDocumentId();

      if (matchedEntry.remainingPredicateColumns.isEmpty()) {
        // No more filters to apply
        // Use aggregated doc for this leaf node if possible.
        int aggregatedDocumentId = matchedLeafNode.getAggregatedDocumentId();
        if (isValidAggregatedDocId(aggregatedDocumentId) && matchedEntry.remainingGroupByColumns.isEmpty()) {
          exactlyMatchedDocsBitmap.add(aggregatedDocumentId);
          numExactlyMatched = numExactlyMatched + 1;
        } else {
          // Have to scan all the documents under this leaf node
          exactlyMatchedDocsBitmap.add(startDocId, endDocId);
          numExactlyMatched += (endDocId - startDocId);
        }
      } else {
        Map<String, PredicateEntry> remainingPredicatesMap = computeRemainingPredicates(matchedEntry);
        List<BaseFilterOperator> filterOperators =
            createFilterOperatorsForRemainingPredicates(matchedEntry, remainingPredicatesMap);

        if (filterOperators.size() == 0) {
          // The predicates are applied, but we cannot use aggregated doc, as we might have lost
          // the group by dimensions, in the aggregated doc.
          exactlyMatchedDocsBitmap.add(startDocId, endDocId);
          numExactlyMatched += (endDocId - startDocId);
        } else if (filterOperators.size() == 1) {
          matchingLeafOperator = filterOperators.get(0);
        } else {
          matchingLeafOperator = new AndOperator(filterOperators);
        }
        if (matchingLeafOperator != null) {
          matchingLeafOperators.add(matchingLeafOperator);
        }
      }

      totalDocsToScan += (endDocId - startDocId);
      LOGGER.debug("{}", matchedLeafNode);
    }

    // Add an operator for exactlyMatchedDocs
    if (numExactlyMatched > 0) {
      matchingLeafOperators.add(createFilterOperator(exactlyMatchedDocsBitmap));
      totalDocsToScan += numExactlyMatched;
    }

    long end = System.currentTimeMillis();
    LOGGER.debug("Found {} matching leaves, took {} ms to create remaining filter operators. Total docs to scan:{}",
        matchedEntries.size(), (end - start), totalDocsToScan);
    return matchingLeafOperators;
  }

  /**
   * Returns true if aggregated doc id is valid, ie >= numRawDocs, false otherwise.
   *
   * This is a temporary fix to handle aggredated doc id's being de-serialized as 0 instead of -1
   * for older segments that do not have this field. This will be resolved once we implement the
   * on-disk format for star-tree along with its own serializer/deserializer, and versioning.
   *
   * @param aggregatedDocumentId
   * @return
   */
  private boolean isValidAggregatedDocId(int aggregatedDocumentId) {
    return (aggregatedDocumentId >= numRawDocs);
  }

  private BaseFilterOperator createFilterOperator(final MutableRoaringBitmap answer) {
    return new BaseFilterOperator() {
      private static final String OPERATOR_NAME = "Anonymous";

      @Override
      public String getOperatorName() {
        return OPERATOR_NAME;
      }

      @Override
      public boolean open() {
        return true;
      }

      @Override
      public boolean close() {
        return true;
      }

      @Override
      public BaseFilterBlock nextFilterBlock(BlockId blockId) {
        return createBaseFilterBlock(new BitmapDocIdIterator(answer.getIntIterator()));
      }

      @Override
      public boolean isResultEmpty() {
        return false;
      }
    };
  }

  /**
   * Builds a list of filter operators for a given matched leaf node for the given
   * set of predicates.
   * @param matchedEntry
   * @param remainingPredicatesMap
   * @return
   */
  private List<BaseFilterOperator> createFilterOperatorsForRemainingPredicates(SearchEntry matchedEntry,
      Map<String, PredicateEntry> remainingPredicatesMap) {
    int startDocId = matchedEntry.starTreeIndexnode.getStartDocumentId();
    int endDocId = matchedEntry.starTreeIndexnode.getEndDocumentId();

    List<BaseFilterOperator> childOperators = new ArrayList<>();
    for (String column : remainingPredicatesMap.keySet()) {
      PredicateEntry predicateEntry = remainingPredicatesMap.get(column);

      // predicateEntry could be null if column appeared only in groupBy
      if (predicateEntry != null) {
        BaseFilterOperator childOperator =
            createChildOperator(startDocId, endDocId - 1, column, predicateEntry);
        childOperators.add(childOperator);
      }
    }
    return childOperators;
  }

  /**
   * Helper method to compute remaining predicates from remainingPredicateColumns of
   * the given search entry.
   *
   * @param entry Search entry for which to compute the remaining predicates.
   * @return
   */
  private Map<String, PredicateEntry> computeRemainingPredicates(SearchEntry entry) {
    Map<String, PredicateEntry> remainingPredicatesMap = new HashMap<>();
    for (String column : entry.remainingPredicateColumns) {
      PredicateEntry predicateEntry = predicatesMap.get(column);
      remainingPredicatesMap.put(column, predicateEntry);
    }
    return remainingPredicatesMap;
  }

  private BaseFilterOperator createChildOperator(int startDocId, int endDocId, String column,
      PredicateEntry predicateEntry) {
    DataSource dataSource = segment.getDataSource(column);
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    BaseFilterOperator childOperator;
    Predicate predicate = predicateEntry.predicate;
    if (dataSourceMetadata.hasInvertedIndex()) {
      if (dataSourceMetadata.isSorted()) {
        childOperator = new SortedInvertedIndexBasedFilterOperator(predicate, dataSource, startDocId, endDocId);
      } else {
        childOperator = new BitmapBasedFilterOperator(predicate, dataSource, startDocId, endDocId);
      }
    } else {
      childOperator = new ScanBasedFilterOperator(predicate, dataSource, startDocId, endDocId);
    }
    return childOperator;
  }

  private BaseFilterBlock createBaseFilterBlock(final BitmapDocIdIterator bitmapDocIdIterator) {
    return new BaseFilterBlock() {

      @Override
      public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
        return new FilterBlockDocIdSet() {

          @Override
          public BlockDocIdIterator iterator() {
            return bitmapDocIdIterator;
          }

          @Override
          public <T> T getRaw() {
            return null;
          }

          @Override
          public void setStartDocId(int startDocId) {
            // no-op
          }

          @Override
          public void setEndDocId(int endDocId) {
            // no-op
          }

          @Override
          public long getNumEntriesScannedInFilter() {
            return 0L;
          }

          @Override
          public int getMinDocId() {
            return 0;
          }

          @Override
          public int getMaxDocId() {
            return segment.getSegmentMetadata().getTotalDocs() - 1;
          }
        };
      }

      @Override
      public BlockId getId() {
        return new BlockId(0);
      }
    };
  }

  private Queue<SearchEntry> findMatchingLeafNodes() {
    Queue<SearchEntry> matchedEntries = new LinkedList<>();
    Queue<SearchEntry> searchQueue = new LinkedList<>();
    List<String> dimensionNames = segment.getStarTree().getDimensionNames();

    SearchEntry startEntry = new SearchEntry();
    startEntry.starTreeIndexnode = segment.getStarTree().getRoot();
    startEntry.remainingPredicateColumns = new HashSet<>(predicatesMap.keySet());
    startEntry.remainingGroupByColumns = new HashSet<>(groupByColumns);
    searchQueue.add(startEntry);

    while (!searchQueue.isEmpty()) {
      SearchEntry searchEntry = searchQueue.remove();
      StarTreeIndexNodeInterf current = searchEntry.starTreeIndexnode;
      HashSet<String> remainingPredicateColumns = searchEntry.remainingPredicateColumns;
      HashSet<String> remainingGroupByColumns = searchEntry.remainingGroupByColumns;
      // Check if its leaf, or if there are no remaining predicates/groupbycolumns, and node has valid aggregated docId
      if (current.isLeaf() || (remainingPredicateColumns.isEmpty() && remainingGroupByColumns.isEmpty()) &&
          isValidAggregatedDocId(current.getAggregatedDocumentId())) {
        // reached leaf
        matchedEntries.add(searchEntry);
        continue;
      }
      // Find next set of nodes to search
      String nextDimension = dimensionNames.get(current.getChildDimensionName());

      HashSet<String> newRemainingPredicateColumns = new HashSet<>();
      newRemainingPredicateColumns.addAll(remainingPredicateColumns);
      HashSet<String> newRemainingGroupByColumns = new HashSet<>();
      newRemainingGroupByColumns.addAll(remainingGroupByColumns);

      addMatchingChildrenToQueue(searchQueue, current, nextDimension, newRemainingPredicateColumns,
          newRemainingGroupByColumns);
    }
    return matchedEntries;
  }

  /**
   * Helper method to add matching children into the search queue.
   * - If predicate can be applied (i.e. equality predicate that is eligible), add the child
   * satisfying the predicate into the queue.
   * - If predicate cannot be applied (either inEligible or nonEquality), add all children to the
   * queue.
   * - If no predicate on the column, add the star-child to the queue
   * @param searchQueue
   * @param node
   * @param column
   * @param remainingPredicateColumns
   * @param remainingGroupByColumns
   */
  private void addMatchingChildrenToQueue(Queue<SearchEntry> searchQueue, StarTreeIndexNodeInterf node,
      String column, HashSet<String> remainingPredicateColumns,
      HashSet<String> remainingGroupByColumns) {

    if (predicateColumns.contains(column)) {
      // Check if there is exact match filter on this column
      PredicateEntry predicateEntry = predicatesMap.get(column);

      remainingPredicateColumns.remove(column);
      remainingGroupByColumns.remove(column);

      int[] matchingDictionaryIds = predicateEntry.predicateEvaluator.getMatchingDictionaryIds();
      for (int matchingDictionaryId : matchingDictionaryIds) {
        StarTreeIndexNodeInterf child = node.getChildForDimensionValue(matchingDictionaryId);
        if (child != null) {
          addNodeToSearchQueue(searchQueue, child, remainingPredicateColumns, remainingGroupByColumns);
        }
      }
    } else {
      int nextValueId;
      if (groupByColumns.contains(column) || predicatesMap.containsKey(column)
          || (node.getChildForDimensionValue(StarTreeIndexNodeInterf.ALL) == null)) {
        Iterator<? extends StarTreeIndexNodeInterf> childrenIterator = node.getChildrenIterator();

        while (childrenIterator.hasNext()) {
          StarTreeIndexNodeInterf child = childrenIterator.next();
          if (child.getDimensionValue() != StarTreeIndexNodeInterf.ALL) {
            remainingPredicateColumns.remove(column);
            remainingGroupByColumns.remove(column);
            addNodeToSearchQueue(searchQueue, child, remainingPredicateColumns,
                remainingGroupByColumns);
          }
        }
      } else {
        // Since we have a star node and no group by on this column we can take lose this dimension
        // by taking star node path
        nextValueId = StarTreeIndexNodeInterf.ALL;
        addNodeToSearchQueue(searchQueue, node.getChildForDimensionValue(nextValueId), remainingPredicateColumns,
            remainingGroupByColumns);
      }
    }
  }

  /**
   * Helper method to add the given node the the provided queue.
   * @param searchQueue
   * @param node
   * @param predicateColumns
   * @param groupByColumns
   */
  private void addNodeToSearchQueue(Queue<SearchEntry> searchQueue, StarTreeIndexNodeInterf node,
      HashSet<String> predicateColumns, HashSet<String> groupByColumns) {
    SearchEntry newEntry = new SearchEntry();
    newEntry.starTreeIndexnode = node;
    newEntry.remainingPredicateColumns = predicateColumns;
    newEntry.remainingGroupByColumns = groupByColumns;
    searchQueue.add(newEntry);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  class SearchEntry {
    StarTreeIndexNodeInterf starTreeIndexnode;
    HashSet<String> remainingPredicateColumns;
    HashSet<String> remainingGroupByColumns;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(starTreeIndexnode);
      sb.append("\t").append(remainingPredicateColumns);
      return sb.toString();
    }
  }

  class PredicateEntry {
    Predicate predicate;
    private PredicateEvaluator predicateEvaluator;

    public PredicateEntry(Predicate predicate, PredicateEvaluator predicateEvaluator) {
      this.predicate = predicate;
      this.predicateEvaluator = predicateEvaluator;
    }
  }
}
