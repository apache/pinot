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

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.docvaliterators.UnSortedSingleValueIterator;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.startree.StarTreeIndexNode;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;


public class StarTreeIndexOperator extends BaseFilterOperator {

  private IndexSegment segment;
  Map<String, PathEntry> pathValuesToTraverse;
  boolean emptyResult = false;

  public StarTreeIndexOperator(IndexSegment segment, BrokerRequest brokerRequest) {
    this.segment = segment;
    pathValuesToTraverse = computeTraversePath(segment, brokerRequest);
  }

  private HashMap<String, PathEntry> computeTraversePath(IndexSegment segment, BrokerRequest brokerRequest) {
    FilterQueryTree filterTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    HashMap<String, PathEntry> pathMap = new HashMap<>();
    for (String column : segment.getColumnNames()) {
      FieldType fieldType = segment.getDataSource(column).getDataSourceMetadata().getFieldType();
      //star tree index does not support filters on metric columns
      if (!fieldType.equals(FieldType.METRIC)) {
        PathEntry pathEntry = new PathEntry(null, StarTreeIndexNode.all());
        pathMap.put(column, pathEntry);
      }
    }

    //find all filter columns
    if (filterTree != null) {
      traverseFilterTree(filterTree, pathMap);
    }
    //group by columns, we cannot lose group by columns during traversal
    GroupBy groupBy = brokerRequest.getGroupBy();
    if (groupBy != null) {
      for (String groupByCol : groupBy.getColumns()) {
        //remove if there was no filter set on this column in the query
        if (pathMap.get(groupByCol).dictionaryId != StarTreeIndexNode.all()) {
          pathMap.remove(groupByCol);
        }
      }
    }
    return pathMap;
  }

  private void traverseFilterTree(FilterQueryTree filterTree, HashMap<String, PathEntry> pathMap) {
    if (filterTree.getChildren() != null && !filterTree.getChildren().isEmpty()) {
      for (FilterQueryTree child : filterTree.getChildren()) {
        traverseFilterTree(child, pathMap);
      }
    } else {
      String column = filterTree.getColumn();
      if (filterTree.getOperator() == FilterOperator.EQUALITY) {
        Dictionary dictionary = segment.getDataSource(column).getDictionary();
        int dictId = dictionary.indexOf(filterTree.getValue().get(0));
        if (dictId < 0) {
          dictId = Constants.EOF;
        }
        pathMap.put(column, new PathEntry(filterTree, dictId));
      } else {
        pathMap.remove(column);
      }
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
    MutableRoaringBitmap finalResult = null;
    if (pathValuesToTraverse.values().contains(Constants.EOF)) {
      finalResult = new MutableRoaringBitmap();
    } else {
      Queue<SearchEntry> matchedEntries = findMatchingLeafNodes();
      //iterate over the matching nodes. For each column, generate the list of ranges.

      for (SearchEntry matchedEntry : matchedEntries) {
        MutableRoaringBitmap answer = new MutableRoaringBitmap();
        int startDocId = matchedEntry.starTreeIndexnode.getStartDocumentId();
        int endDocId = matchedEntry.starTreeIndexnode.getEndDocumentId();
        answer.add(startDocId, endDocId);

        if (!matchedEntry.remainingColumnsToFilter.isEmpty()) {
          for (String column : matchedEntry.remainingColumnsToFilter) {
            PathEntry pathEntry = pathValuesToTraverse.get(column);
            Integer dictId = pathEntry.dictionaryId;
            if (dictId == StarTreeIndexNode.all()) {
              continue;
            }
            DataSource dataSource = segment.getDataSource(column);
            DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
            if (dataSourceMetadata.hasInvertedIndex()) {
              if (dataSourceMetadata.isSorted()) {
                IntPair minMaxRange = dataSource.getInvertedIndex().getMinMaxRangeFor(dictId);
                MutableRoaringBitmap sortedRangeBitmap = new MutableRoaringBitmap();
                sortedRangeBitmap.add(minMaxRange.getLeft(), minMaxRange.getRight() + 1);//end is exclusive in bitmap but getMinMaxRange returns inclusive
                answer.and(sortedRangeBitmap);
              } else {
                ImmutableRoaringBitmap bitmap = dataSource.getInvertedIndex().getImmutable(dictId);
                answer.and(bitmap);
              }
            } else {
              ScanBasedFilterOperator operator = new ScanBasedFilterOperator(dataSource, startDocId, endDocId -1);
              EqPredicate predicate = new EqPredicate(column, pathEntry.tree.getValue());
              operator.setPredicate(predicate);
              BlockDocIdIterator iterator = operator.getNextBlock().getBlockDocIdSet().iterator();
              ScanBasedDocIdIterator scanBasedDocIdIterator = (ScanBasedDocIdIterator) iterator;
              MutableRoaringBitmap scanAnswer = scanBasedDocIdIterator.applyAnd(answer);
              answer.and(scanAnswer);
            }
          }
        }
        if (finalResult == null) {
          finalResult = answer;
        } else {
          finalResult.or(answer);
        }

      }
    }
    final BitmapDocIdIterator bitmapDocIdIterator = new BitmapDocIdIterator(finalResult.getIntIterator());
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
            return segment.getSegmentMetadata().getTotalDocs();
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
    startEntry.remainingColumnsToFilter = new HashSet<>(pathValuesToTraverse.keySet());
    searchQueue.add(startEntry);
    while (!searchQueue.isEmpty()) {
      SearchEntry searchEntry = searchQueue.remove();
      StarTreeIndexNode current = searchEntry.starTreeIndexnode;
      HashSet<String> remainingColumnsToFilter = searchEntry.remainingColumnsToFilter;
      //check if its leaf
      if (current.isLeaf()) {
        //reached leaf
        matchedEntries.add(searchEntry);
        continue;
      }
      //find next set of nodes to search
      String nextDimension = dimensionIndexToNameMapping.inverse().get(current.getChildDimensionName());
      HashSet<String> newRemainingColumnsToFilter = new HashSet<>();
      newRemainingColumnsToFilter.addAll(remainingColumnsToFilter);
      newRemainingColumnsToFilter.remove(nextDimension);
      if (remainingColumnsToFilter.contains(nextDimension)) {
        //if there is exact match filter on this column or no filter. If there was a group by on this dimension, we cannot lose it
        int nextValueId = pathValuesToTraverse.get(nextDimension).dictionaryId;
        SearchEntry newEntry = new SearchEntry();
        newEntry.starTreeIndexnode = current.getChildren().get(nextValueId);
        if (newEntry.starTreeIndexnode != null) {
          newEntry.remainingColumnsToFilter = newRemainingColumnsToFilter;
          searchQueue.add(newEntry);
        }
      } else {
        //if there is a group by
        for (Map.Entry<Integer, StarTreeIndexNode> entry : current.getChildren().entrySet()) {
          if (entry.getKey() != StarTreeIndexNode.all()) {
            SearchEntry newEntry = new SearchEntry();
            newEntry.starTreeIndexnode = entry.getValue();
            newEntry.remainingColumnsToFilter = newRemainingColumnsToFilter;
            searchQueue.add(newEntry);
          }
        }
      }
    }
    return matchedEntries;
  }

  class SearchEntry {
    StarTreeIndexNode starTreeIndexnode;
    HashSet<String> remainingColumnsToFilter;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(starTreeIndexnode);
      sb.append("\t").append(remainingColumnsToFilter);
      return sb.toString();
    }
  }

  class PathEntry {
    FilterQueryTree tree;
    int dictionaryId;

    public PathEntry(FilterQueryTree tree, int dictionaryId) {
      super();
      this.tree = tree;
      this.dictionaryId = dictionaryId;
    }
  }
}
