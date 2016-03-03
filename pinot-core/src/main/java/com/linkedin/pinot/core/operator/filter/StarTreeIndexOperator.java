/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.startree.StarTreeIndexNode;


public class StarTreeIndexOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexOperator.class);

  private IndexSegment segment;
  //predicates that can be applied on the star tree
  Map<String, PredicateEntry> eligibleEqualityPredicatesMap;
  //predicates that cannot be applied on star tree index because it appears in group by clause
  Map<String, PredicateEntry> inEligiblePredicatesMap;
  //predicates that cannot be applied because they are not of type Equality
  Map<String, PredicateEntry> nonEqualityPredicatesMap;

  boolean emptyResult = false;
  private BrokerRequest brokerRequest;

  public StarTreeIndexOperator(IndexSegment segment, BrokerRequest brokerRequest) {
    this.segment = segment;
    this.brokerRequest = brokerRequest;
    eligibleEqualityPredicatesMap = new HashMap<>();
    inEligiblePredicatesMap = new HashMap<>();
    nonEqualityPredicatesMap = new HashMap<>();
    initPredicatesToEvaluate();
  }

  private void initPredicatesToEvaluate() {
    FilterQueryTree filterTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    //find all filter columns
    if (filterTree != null) {
      if (filterTree.getChildren() != null && !filterTree.getChildren().isEmpty()) {
        for (FilterQueryTree childFilter : filterTree.getChildren()) {
          //nested filters are not supported
          assert childFilter.getChildren() == null || childFilter.getChildren().isEmpty();
          processFilterTree(childFilter);
        }
      } else {
        processFilterTree(filterTree);
      }
    }
    //group by columns, we cannot lose group by columns during traversal
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      for (String groupByCol : groupBy.getColumns()) {
        //remove if there was a filter predicate set on this column in the query
        if (eligibleEqualityPredicatesMap.containsKey(groupByCol)) {
          inEligiblePredicatesMap.put(groupByCol, eligibleEqualityPredicatesMap.get(groupByCol));
          eligibleEqualityPredicatesMap.remove(groupByCol);
        } else {
          //there is no predicate but we cannot lose this dimension while traversing
          inEligiblePredicatesMap.put(groupByCol, null);
        }
      }
    }
  }

  private void processFilterTree(FilterQueryTree childFilter) {
    String column = childFilter.getColumn();
    //only equality predicates are supported
    Predicate predicate = Predicate.newPredicate(childFilter);
    Dictionary dictionary = segment.getDataSource(column).getDictionary();
    if (childFilter.getOperator() == FilterOperator.EQUALITY) {
      EqPredicate eqPredicate = (EqPredicate) predicate;
      //computing dictionaryId allows us early termination and avoids multiple looks up during tree traversal
      int dictId = dictionary.indexOf(eqPredicate.getEqualsValue());
      if (dictId < 0) {
        //empty result
        emptyResult = true;
      }
      eligibleEqualityPredicatesMap.put(column, new PredicateEntry(predicate, dictId));
    } else {
      // If dictionary does not have any values that satisfy the predicate, set emptyResults to true.
      PredicateEvaluator predicateEvaluator =
          PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dictionary);
      if (predicateEvaluator.alwaysFalse()) {
        emptyResult = true;
      }
      //store this predicate, we will have to apply it later
      nonEqualityPredicatesMap.put(column, new PredicateEntry(predicate, -1));
    }
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
    long start, end;
    MutableRoaringBitmap finalResult = null;
    if (emptyResult) {
      finalResult = new MutableRoaringBitmap();
      final BitmapDocIdIterator bitmapDocIdIterator = new BitmapDocIdIterator(finalResult.getIntIterator());
      return createBaseFilterBlock(bitmapDocIdIterator);
    }
    start = System.currentTimeMillis();
    Queue<SearchEntry> matchedEntries = findMatchingLeafNodes();
    //iterate over the matching nodes. For each column, generate the list of ranges.
    List<Operator> matchingLeafOperators = new ArrayList<>();
    int totalDocsToScan = 0;
    for (SearchEntry matchedEntry : matchedEntries) {
      Operator matchingLeafOperator;
      int startDocId = matchedEntry.starTreeIndexnode.getStartDocumentId();
      int endDocId = matchedEntry.starTreeIndexnode.getEndDocumentId();

      List<Operator> filterOperators = createFilterOperatorsForRemainingPredicates(matchedEntry);

      if (filterOperators.size() == 0) {
        matchingLeafOperator = createFilterOperator(startDocId, endDocId);
      } else if (filterOperators.size() == 1) {
        matchingLeafOperator = filterOperators.get(0);
      } else {
        matchingLeafOperator = new AndOperator(filterOperators);
      }
      matchingLeafOperators.add(matchingLeafOperator);
      totalDocsToScan += (endDocId - startDocId);
      LOGGER.debug("{}", matchedEntry.starTreeIndexnode);
    }
    end = System.currentTimeMillis();
    LOGGER.debug("Found {} matching leaves, took {} ms to create remaining filter operators. Total docs to scan:{}",
        matchedEntries.size(), (end - start), totalDocsToScan);

    if (matchingLeafOperators.size() == 1) {
      BaseFilterOperator baseFilterOperator = (BaseFilterOperator) matchingLeafOperators.get(0);
      return baseFilterOperator.nextFilterBlock(blockId);
    } else {
      CompositeOperator compositeOperator = new CompositeOperator(matchingLeafOperators);
      return compositeOperator.nextFilterBlock(blockId);
    }
  }

  private BaseFilterOperator createFilterOperator(int startDocId, int endDocId) {
    final MutableRoaringBitmap answer = new MutableRoaringBitmap();
    answer.add(startDocId, endDocId);
    return new BaseFilterOperator() {

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
    };
  }

  private List<Operator> createFilterOperatorsForRemainingPredicates(SearchEntry matchedEntry) {
    int startDocId = matchedEntry.starTreeIndexnode.getStartDocumentId();
    int endDocId = matchedEntry.starTreeIndexnode.getEndDocumentId();
    List<Operator> childOperators = new ArrayList<>();
    Map<String, PredicateEntry> remainingPredicatesMap = new HashMap<>();
    for (String column : matchedEntry.remainingPredicates) {
      PredicateEntry predicateEntry = eligibleEqualityPredicatesMap.get(column);
      remainingPredicatesMap.put(column, predicateEntry);
    }
    //apply predicate that were not applied because of group by
    //this should mostly empty unless query has a column in both filter and group by
    remainingPredicatesMap.putAll(inEligiblePredicatesMap);
    remainingPredicatesMap.putAll(nonEqualityPredicatesMap);
    for (String column : remainingPredicatesMap.keySet()) {
      PredicateEntry predicateEntry = remainingPredicatesMap.get(column);
      //predicateEntry could be null if column appeared  only in groupBy
      if (predicateEntry != null) {
        BaseFilterOperator childOperator = createChildOperator(startDocId, endDocId - 1, column, predicateEntry);
        childOperators.add(childOperator);
      }
    }
    return childOperators;
  }

  private BaseFilterOperator createChildOperator(int startDocId, int endDocId, String column,
      PredicateEntry predicateEntry) {
    DataSource dataSource = segment.getDataSource(column);
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    BaseFilterOperator childOperator;
    if (dataSourceMetadata.hasInvertedIndex()) {
      if (dataSourceMetadata.isSorted()) {
        childOperator = new SortedInvertedIndexBasedFilterOperator(dataSource, startDocId, endDocId);
      } else {
        childOperator = new BitmapBasedFilterOperator(dataSource, startDocId, endDocId);
      }
    } else {
      childOperator = new ScanBasedFilterOperator(dataSource, startDocId, endDocId);
    }
    childOperator.setPredicate(predicateEntry.predicate);
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
            //no-op
          }

          @Override
          public void setEndDocId(int endDocId) {
            //no-op
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
    HashBiMap<String, Integer> dimensionIndexToNameMapping = segment.getStarTree().getDimensionNameToIndexMap();
    SearchEntry startEntry = new SearchEntry();
    startEntry.starTreeIndexnode = segment.getStarTree().getRoot();
    startEntry.remainingPredicates = new HashSet<>(eligibleEqualityPredicatesMap.keySet());
    searchQueue.add(startEntry);
    while (!searchQueue.isEmpty()) {
      SearchEntry searchEntry = searchQueue.remove();
      StarTreeIndexNode current = searchEntry.starTreeIndexnode;
      HashSet<String> remainingColumnsToFilter = searchEntry.remainingPredicates;
      //check if its leaf
      if (current.isLeaf()) {
        //reached leaf
        matchedEntries.add(searchEntry);
        continue;
      }
      //find next set of nodes to search
      String nextDimension = dimensionIndexToNameMapping.inverse().get(current.getChildDimensionName());
      HashSet<String> newRemainingPredicates = new HashSet<>();
      newRemainingPredicates.addAll(remainingColumnsToFilter);
      newRemainingPredicates.remove(nextDimension);
      if (!(inEligiblePredicatesMap.containsKey(nextDimension)
          || nonEqualityPredicatesMap.containsKey(nextDimension))) {
        //check if there is exact match filter on this column
        int nextValueId;
        if (eligibleEqualityPredicatesMap.containsKey(nextDimension)) {
          nextValueId = eligibleEqualityPredicatesMap.get(nextDimension).dictionaryId;
        } else {
          nextValueId = StarTreeIndexNode.all();
        }
        SearchEntry newEntry = new SearchEntry();
        newEntry.starTreeIndexnode = current.getChildren().get(nextValueId);
        if (newEntry.starTreeIndexnode != null) {
          newEntry.remainingPredicates = newRemainingPredicates;
          searchQueue.add(newEntry);
        }
      } else {
        //if there is a group by
        for (Map.Entry<Integer, StarTreeIndexNode> entry : current.getChildren().entrySet()) {
          if (entry.getKey() != StarTreeIndexNode.all()) {
            SearchEntry newEntry = new SearchEntry();
            newEntry.starTreeIndexnode = entry.getValue();
            newEntry.remainingPredicates = newRemainingPredicates;
            searchQueue.add(newEntry);
          }
        }
      }
    }
    return matchedEntries;
  }

  class SearchEntry {
    StarTreeIndexNode starTreeIndexnode;
    HashSet<String> remainingPredicates;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(starTreeIndexnode);
      sb.append("\t").append(remainingPredicates);
      return sb.toString();
    }
  }

  class PredicateEntry {
    Predicate predicate;
    int dictionaryId;

    public PredicateEntry(Predicate predicate, int dictionaryId) {
      this.predicate = predicate;
      this.dictionaryId = dictionaryId;
    }
  }
}
