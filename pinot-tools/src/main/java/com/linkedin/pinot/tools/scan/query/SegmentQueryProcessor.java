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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


class SegmentQueryProcessor {
  private BrokerRequest _brokerRequest;
  private File _segmentDir;

  SegmentQueryProcessor(BrokerRequest brokerRequest, File segmentDir) {
    _brokerRequest = brokerRequest;
    _segmentDir = segmentDir;
  }

  public ResultTable process(String query)
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDir);
    IndexSegmentImpl indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(_segmentDir, ReadMode.heap);

    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(_brokerRequest);
    List<Integer> filteredDocIds = filterDocIds(indexSegment, metadata, filterQueryTree);

    ResultTable result = null;
    if (_brokerRequest.isSetAggregationsInfo()) {
      // Aggregation only
      if (!_brokerRequest.isSetGroupBy()) {
        Aggregation aggregation =
            new Aggregation(indexSegment, metadata, filteredDocIds, _brokerRequest.getAggregationsInfo());
        return aggregation.run();
      } else { // Aggregation GroupBy
        AggregationGroupBy aggregationGroupBy =
            new AggregationGroupBy(indexSegment, metadata, filteredDocIds, _brokerRequest.getAggregationsInfo());
        return aggregationGroupBy.run();
      }
    }

    // Only Selection
    if (_brokerRequest.isSetSelections()) {
      Selection selection =
          new Selection(indexSegment, metadata, filteredDocIds, _brokerRequest.getSelections().getSelectionColumns());
      result = selection.run();
    }

    return result;
  }

  private List<Integer> filterDocIds(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata,
      FilterQueryTree filterQueryTree)
      throws Exception {
    // If there's no filter predicate, return all docsIds.
    if (filterQueryTree == null) {
      int totalDocs = indexSegment.getTotalDocs();

      List<Integer> filteredDocIds = new ArrayList<>(indexSegment.getTotalDocs());
      for (int i = 0; i < totalDocs; i++) {
        filteredDocIds.add(i);
      }
      return filteredDocIds;
    }

    return doFilter(filterQueryTree, indexSegment, metadata);
  }

  private List<Integer> doFilter(FilterQueryTree filterQueryTree, IndexSegmentImpl indexSegment,
      SegmentMetadataImpl metadata) {
    final List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    final boolean isLeaf = (childFilters == null) || childFilters.isEmpty();

    if (isLeaf) {
      FilterOperator filterType = filterQueryTree.getOperator();
      String column = filterQueryTree.getColumn();
      final List<String> value = filterQueryTree.getValue();
      return getMatchingDocIds(indexSegment, metadata, filterType, column, value);
    }

    List<Integer> result = doFilter(childFilters.get(0), indexSegment, metadata);
    for (int i = 1; i < childFilters.size(); ++i) {
      result = combine(result, doFilter(childFilters.get(i), indexSegment, metadata), filterQueryTree.getOperator());
    }
    return result;
  }

  private List<Integer> combine(List<Integer> operand1, List<Integer> operand2, FilterOperator operator) {
    List<Integer> result = new ArrayList<>();
    Set<Integer> set = new HashSet<>();

    switch (operator) {
      case AND:
        set.addAll(operand1);
        for (Integer docId : operand2) {
          if (set.contains(docId)) {
            result.add(docId);
          }
        }
        break;

      case OR:
        set.addAll(operand1);
        set.addAll(operand2);
        result.addAll(set);
        break;

      default:
        throw new RuntimeException("Unsupported combine operator");
    }

    return result;
  }

  List<Integer> getMatchingDocIds(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata,
      FilterOperator filterType, String column, List<String> value) {
    Dictionary dictionaryReader = indexSegment.getDictionaryFor(column);
    PredicateFilter predicateFilter = null;

    switch (filterType) {
      case EQUALITY:
        predicateFilter = new EqualsPredicateFilter(dictionaryReader, value.get(0));
        break;

      case NOT:
        predicateFilter = new NotPredicateFilter(dictionaryReader, value.get(0));
        break;

      case IN:
        predicateFilter = new InPredicateFilter(dictionaryReader, value);
        break;

      case NOT_IN:
        predicateFilter = new NotInPredicateFilter(dictionaryReader, value);
        break;

      case RANGE:
      case REGEX:
      default:
        throw new UnsupportedOperationException("Unsupported filterType:" + filterType);
    }

    return evaluatePredicate(column, indexSegment, metadata, predicateFilter);
  }

  private List<Integer> evaluatePredicate(String column, IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata,
      PredicateFilter predicateFilter) {
    List<Integer> result = new ArrayList<>();
    if (metadata.getColumnMetadataFor(column).isSingleValue()) {
      BlockSingleValIterator bvIter =
          (BlockSingleValIterator) indexSegment.getDataSource(column).getNextBlock().getBlockValueSet().iterator();

      int docId = 0;
      while (bvIter.hasNext()) {
        if (predicateFilter.apply(bvIter.nextIntVal())) {
          result.add(docId);
        }
        ++docId;
      }
    } else {
      int maxNumMultiValues = metadata.getColumnMetadataFor(column).getMaxNumberOfMultiValues();
      BlockMultiValIterator bvIter =
          (BlockMultiValIterator) indexSegment.getDataSource(column).getNextBlock().getBlockValueSet().iterator();

      int docId = 0;
      while (bvIter.hasNext()) {
        int[] dictIds = new int[maxNumMultiValues];
        int numMVValues = bvIter.nextIntVal(dictIds);

        dictIds = Arrays.copyOf(dictIds, numMVValues);
        if (predicateFilter.apply(dictIds)) {
          result.add(docId);
        }
        ++docId;
      }
    }

    return result;
  }
}