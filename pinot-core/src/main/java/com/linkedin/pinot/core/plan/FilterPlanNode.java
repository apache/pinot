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
package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.AndOperator;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.BitmapBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.filter.OrOperator;
import com.linkedin.pinot.core.operator.filter.ScanBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.SortedInvertedIndexBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexOperator;
import com.linkedin.pinot.core.realtime.RealtimeSegment;


/**
 */
public class FilterPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilterPlanNode.class);
  private final BrokerRequest _brokerRequest;
  private final IndexSegment _segment;

  public FilterPlanNode(IndexSegment segment, BrokerRequest brokerRequest) {
    _segment = segment;
    _brokerRequest = brokerRequest;
  }

  @Override
  public Operator run() {
    long start = System.currentTimeMillis();
    Operator operator;
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(_brokerRequest);
    if (_segment.getSegmentMetadata().hasStarTree() && RequestUtils.isFitForStarTreeIndex(_segment.getSegmentMetadata(),
        filterQueryTree, _brokerRequest.getAggregationsInfo())) {
      operator = new StarTreeIndexOperator(_segment, _brokerRequest);
    } else {
      operator = constructPhysicalOperator(filterQueryTree);
    }
    long end = System.currentTimeMillis();
    LOGGER.debug("FilterPlanNode.run took:{}", (end - start));
    return operator;
  }

  private Operator constructPhysicalOperator(FilterQueryTree filterQueryTree) {
    Operator ret = null;

    if (null == filterQueryTree) {
      return new MatchEntireSegmentOperator(_segment.getSegmentMetadata().getTotalRawDocs());
    }

    final List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
    final boolean isLeaf = (childFilters == null) || childFilters.isEmpty();

    if (!isLeaf) {
      List<Operator> operators = new ArrayList<Operator>();
      for (final FilterQueryTree query : childFilters) {
        Operator childOperator = constructPhysicalOperator(query);
        operators.add(childOperator);
      }
      final FilterOperator filterType = filterQueryTree.getOperator();
      switch (filterType) {
        case AND:
          reorder(operators);
          ret = new AndOperator(operators);
          break;
        case OR:
          reorder(operators);
          ret = new OrOperator(operators);
          break;
        default:
          throw new UnsupportedOperationException(
              "Not support filter type - " + filterType + " with children operators");
      }
    } else {
      final FilterOperator filterType = filterQueryTree.getOperator();
      final String column = filterQueryTree.getColumn();
      Predicate predicate = Predicate.newPredicate(filterQueryTree);

      DataSource ds;
      ds = _segment.getDataSource(column);
      DataSourceMetadata dataSourceMetadata = ds.getDataSourceMetadata();
      BaseFilterOperator baseFilterOperator;
      int startDocId = 0;
      int endDocId = _segment.getSegmentMetadata().getTotalRawDocs() - 1; //end is inclusive
      if (dataSourceMetadata.hasInvertedIndex()) {
        // jfim: ScanBasedFilterOperator is broken for realtime segments for now
        // range evaluation based on inv index is inefficient, so do this only if is NOT range.
        if (!filterType.equals(FilterOperator.RANGE) || _segment instanceof RealtimeSegment) {
          if (dataSourceMetadata.isSingleValue() && dataSourceMetadata.isSorted()) {
            // if the column is sorted use sorted inverted index based implementation
            baseFilterOperator = new SortedInvertedIndexBasedFilterOperator(ds, startDocId, endDocId);
          } else {
            baseFilterOperator = new BitmapBasedFilterOperator(ds, startDocId, endDocId);
          }
        } else {
          baseFilterOperator = new ScanBasedFilterOperator(ds, startDocId, endDocId);
        }
      } else {
        baseFilterOperator = new ScanBasedFilterOperator(ds, startDocId, endDocId);
      }
      baseFilterOperator.setPredicate(predicate);
      ret = baseFilterOperator;
    }
    return ret;
  }

  /**
   * Re orders operators, puts Sorted -> Inverted and then Raw scan. TODO: With Inverted, we can
   * further optimize based on cardinality
   * @param operators
   */
  private void reorder(List<Operator> operators) {

    final Map<Operator, Integer> operatorPriorityMap = new HashMap<Operator, Integer>();
    for (Operator operator : operators) {
      Integer priority = Integer.MAX_VALUE;
      if (operator instanceof SortedInvertedIndexBasedFilterOperator) {
        priority = 0;
      } else if (operator instanceof AndOperator) {
        priority = 1;
      } else if (operator instanceof BitmapBasedFilterOperator) {
        priority = 2;
      } else if (operator instanceof ScanBasedFilterOperator) {
        priority = 3;
      } else if (operator instanceof OrOperator) {
        priority = 4;
      }
      operatorPriorityMap.put(operator, priority);
    }

    Comparator<? super Operator> comparator = new Comparator<Operator>() {
      @Override
      public int compare(Operator o1, Operator o2) {
        return Integer.compare(operatorPriorityMap.get(o1), operatorPriorityMap.get(o2));
      }
    };
    Collections.sort(operators, comparator);
  }

  @Override
  public void showTree(String prefix) {
    final String treeStructure = prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 0: "
        + _brokerRequest.getFilterQuery();
    LOGGER.debug(treeStructure);
  }
}
