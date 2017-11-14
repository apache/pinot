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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class SegmentQueryProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentQueryProcessor.class);

  private File _segmentDir;
  private Set<String> _mvColumns;
  private Map<String, int[]> _mvColumnArrayMap;

  private final SegmentMetadataImpl _metadata;
  private final IndexSegmentImpl _indexSegment;

  private final String _tableName;
  private final String _segmentName;
  private final int _totalDocs;

  SegmentQueryProcessor(File segmentDir)
      throws Exception {
    _segmentDir = segmentDir;

    _indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(_segmentDir, ReadMode.mmap);
    _metadata = new SegmentMetadataImpl(_segmentDir);
    _tableName = _metadata.getTableName();
    _segmentName = _metadata.getName();

    _totalDocs = _metadata.getTotalDocs();

    _mvColumns = new HashSet<>();
    _mvColumnArrayMap = new HashMap<>();

    for (ColumnMetadata columnMetadata : _metadata.getColumnMetadataMap().values()) {
      String column = columnMetadata.getColumnName();

      if (!columnMetadata.isSingleValue()) {
        _mvColumns.add(column);
      }
      _mvColumnArrayMap.put(column, new int[columnMetadata.getMaxNumberOfMultiValues()]);
    }
  }

  public void close() {
    _metadata.close();
    _indexSegment.destroy();
  }

  public ResultTable process(BrokerRequest brokerRequest)
      throws Exception {
    if (pruneSegment(brokerRequest)) {
      return null;
    }

    LOGGER.debug("Processing segment: {}", _segmentName);
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    List<Integer> filteredDocIds = filterDocIds(filterQueryTree, null);

    ResultTable result = null;
    if (brokerRequest.isSetAggregationsInfo()) {
      // Aggregation only
      if (!brokerRequest.isSetGroupBy()) {
        Aggregation aggregation =
            new Aggregation(_indexSegment, _metadata, filteredDocIds, brokerRequest.getAggregationsInfo(), null, 10);
        result = aggregation.run();
      } else { // Aggregation GroupBy
        GroupBy groupBy = brokerRequest.getGroupBy();
        Aggregation aggregation =
            new Aggregation(_indexSegment, _metadata, filteredDocIds, brokerRequest.getAggregationsInfo(),
                groupBy.getColumns(), groupBy.getTopN());
        result = aggregation.run();
      }
    } else {// Only Selection
      if (brokerRequest.isSetSelections()) {
        List<String> columns = brokerRequest.getSelections().getSelectionColumns();
        if (columns.contains("*")) {
          columns = Arrays.asList(_indexSegment.getColumnNames());
        }
        List<Pair> selectionColumns = new ArrayList<>();
        Set<String> columSet = new HashSet<>();

        // Collect a unique list of columns, in case input has duplicates.
        for (String column : columns) {
          if (!columSet.contains(column)) {
            selectionColumns.add(new Pair(column, null));
            columSet.add(column);
          }
        }
        Selection selection = new Selection(_indexSegment, _metadata, filteredDocIds, selectionColumns);
        result = selection.run();
      }
    }

    result.setNumDocsScanned(filteredDocIds.size());
    result.setTotalDocs(_totalDocs);
    return result;
  }

  private boolean pruneSegment(BrokerRequest brokerRequest) {
    // Check if segment belongs to the table being queried.
    if (!_tableName.equals(brokerRequest.getQuerySource().getTableName())) {
      LOGGER.debug("Skipping segment {} from different table {}", _segmentName, _tableName);
      return true;
    }

    // Check if any column in the query does not exist in the segment.
    Set<String> allColumns = _metadata.getAllColumns();
    if (brokerRequest.isSetAggregationsInfo()) {
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        Map<String, String> aggregationParams = aggregationInfo.getAggregationParams();

        for (String column : aggregationParams.values()) {
          if (column != null && !column.isEmpty() && !column.equals("*") && !allColumns.contains(column)) {
            LOGGER.debug("Skipping segment '{}', as it does not have column '{}'", _metadata.getName(), column);
            return true;
          }
        }

        GroupBy groupBy = brokerRequest.getGroupBy();
        if (groupBy != null) {
          for (String column : groupBy.getColumns()) {
            if (!allColumns.contains(column)) {
              LOGGER.debug("Skipping segment '{}', as it does not have column '{}'", _metadata.getName(), column);
              return true;
            }
          }
        }
      }
    } else {
      if (brokerRequest.isSetSelections()) {
        for (String column : brokerRequest.getSelections().getSelectionColumns()) {
          if (!allColumns.contains(column)) {
            LOGGER.debug("Skipping segment '{}', as it does not have column '{}'", _metadata.getName(), column);
            return true;
          }
        }
      }
    }
    return false;
  }

  private List<Integer> filterDocIds(FilterQueryTree filterQueryTree, List<Integer> inputDocIds) {
    // If no filter predicate, return the input without filtering.
    if (filterQueryTree == null) {
      List<Integer> allDocs = new ArrayList<>(_totalDocs);
      for (int i = 0; i < _totalDocs; ++i) {
        allDocs.add(i);
      }
      return allDocs;
    }

    final List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    final boolean isLeaf = (childFilters == null) || childFilters.isEmpty();

    if (isLeaf) {
      FilterOperator filterType = filterQueryTree.getOperator();
      String column = filterQueryTree.getColumn();
      final List<String> value = filterQueryTree.getValue();
      return getMatchingDocIds(inputDocIds, filterType, column, value);
    }

    List<Integer> result = filterDocIds(childFilters.get(0), inputDocIds);
    final FilterOperator operator = filterQueryTree.getOperator();
    for (int i = 1; i < childFilters.size(); ++i) {
//      List<Integer> childResult = operator.equals(FilterOperator.AND) ? filterDocIds(childFilters.get(i), result)
//          : filterDocIds(childFilters.get(i), inputDocIds);
      List<Integer> childResult = filterDocIds(childFilters.get(i), inputDocIds);
      result = combine(result, childResult, operator);
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

  List<Integer> getMatchingDocIds(List<Integer> inputDocIds, FilterOperator filterType, String column,
      List<String> value) {
    Dictionary dictionaryReader = _indexSegment.getDictionaryFor(column);
    PredicateFilter predicateFilter;

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
        predicateFilter = new RangePredicateFilter(dictionaryReader, value);
        break;

      case REGEXP_LIKE:
      default:
        throw new UnsupportedOperationException("Unsupported filterType:" + filterType);
    }

    return evaluatePredicate(inputDocIds, column, predicateFilter);
  }

  private List<Integer> evaluatePredicate(List<Integer> inputDocIds, String column, PredicateFilter predicateFilter) {
    List<Integer> result = new ArrayList<>();
    if (!_mvColumns.contains(column)) {
      BlockSingleValIterator bvIter =
          (BlockSingleValIterator) _indexSegment.getDataSource(column).nextBlock().getBlockValueSet().iterator();

      int i = 0;
      while (bvIter.hasNext() && (inputDocIds == null || i < inputDocIds.size())) {
        int docId = (inputDocIds != null) ? inputDocIds.get(i++) : i++;
        bvIter.skipTo(docId);
        if (predicateFilter.apply(bvIter.nextIntVal())) {
          result.add(docId);
        }
      }
    } else {
      BlockMultiValIterator bvIter =
          (BlockMultiValIterator) _indexSegment.getDataSource(column).nextBlock().getBlockValueSet().iterator();

      int i = 0;
      while (bvIter.hasNext() && (inputDocIds == null || i < inputDocIds.size())) {
        int docId = (inputDocIds != null) ? inputDocIds.get(i++) : i++;
        bvIter.skipTo(docId);

        int[] dictIds = _mvColumnArrayMap.get(column);
        int numMVValues = bvIter.nextIntVal(dictIds);

        if (predicateFilter.apply(dictIds, numMVValues)) {
          result.add(docId);
        }
      }
    }
    return result;
  }

  public String getSegmentName() {
    return _segmentName;
  }
}