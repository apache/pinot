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

import com.linkedin.pinot.common.data.FieldSpec;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.linkedin.pinot.tools.scan.query.Utils.getNextMultiValue;
import static com.linkedin.pinot.tools.scan.query.Utils.getNextSingleValue;


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
      if (!_brokerRequest.isSetGroupBy()) {
        // Only Aggregation
        List<String> projColumns = null;
        Projection projection = new Projection(indexSegment, metadata, filteredDocIds, projColumns);
        ResultTable projectionResult = projection.run();
        String[] aggColumns = null;
        String[] aggFunctions = null;
        result.aggregate(projectionResult, _brokerRequest.getAggregationsInfo());
      } else {
        // Aggregation GroupBy
        List<String> projColumns = null;
        Projection projection = new Projection(indexSegment, metadata, filteredDocIds, projColumns);
        ResultTable projectionResult = projection.run();
        String[] groupByColumns = null;
        String[] aggColumns = null;
        String[] aggFunctions = null;

        result.groupByAggregation(projectionResult, groupByColumns, aggColumns, aggFunctions);
      }
    }

    // Only Selection
    if (_brokerRequest.isSetSelections()) {
      Projection projection =
          new Projection(indexSegment, metadata, filteredDocIds, _brokerRequest.getSelections().getSelectionColumns());
      result = projection.run();
    }

    return result;
  }

  private List<Integer> filterDocIds(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata,
      FilterQueryTree filterQueryTree)
      throws Exception {
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
      return evaluatePredicate(indexSegment, metadata, filterType, column, value);
    }

    List<Integer> result = new ArrayList<>();
    for (FilterQueryTree childFilter : childFilters) {
      result = combine(result, doFilter(childFilter, indexSegment, metadata), filterQueryTree.getOperator());
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

  List<Integer> evaluatePredicate(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata,
      FilterOperator filterType, String column, List<String> value) {
    switch (filterType) {
      case EQUALITY:
        return getEquals(indexSegment, metadata, column, value);

      case RANGE:
      case REGEX:
      case NOT:
      case NOT_IN:
      case IN:
      default:
        throw new UnsupportedOperationException("Unsupported filterType:" + filterType);
    }
  }

  private List<Integer> getEquals(IndexSegmentImpl indexSegment, SegmentMetadataImpl metadata, String column,
      List<String> eqValue) {
    FieldSpec.DataType dataType = metadata.getColumnMetadataFor(column).getDataType();

    int docId = 0;
    List<Integer> result = new ArrayList<>();
    Dictionary dictionaryReader = indexSegment.getDictionaryFor(column);

    if (metadata.getColumnMetadataFor(column).isSingleValue()) {
      BlockSingleValIterator bvIter =
          (BlockSingleValIterator) indexSegment.getDataSource(column).getNextBlock().getBlockValueSet().iterator();

      while (bvIter.hasNext()) {
        Object value = getNextSingleValue(bvIter, dataType, dictionaryReader);

        if (value.equals(eqValue.get(0))) {
          result.add(docId);
        }
        ++docId;
      }
    } else {
      BlockMultiValIterator bvIter =
          (BlockMultiValIterator) indexSegment.getDataSource(column).getNextBlock().getBlockValueSet().iterator();

      while (bvIter.hasNext()) {
        int maxNumMultiValue = metadata.getColumnMetadataFor(column).getMaxNumberOfMultiValues();
        Object[] value = getNextMultiValue(bvIter, dataType, dictionaryReader, maxNumMultiValue);

        for (int i = 0; i < value.length; ++i) {
          if (value.equals(eqValue.get(0))) {
            result.add(docId);
            break;
          }
        }
        ++docId;
      }
    }

    return result;
  }
}
