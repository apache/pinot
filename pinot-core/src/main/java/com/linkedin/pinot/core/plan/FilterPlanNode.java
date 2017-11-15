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
package com.linkedin.pinot.core.plan;

import com.google.common.annotations.VisibleForTesting;
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
import com.linkedin.pinot.core.operator.filter.EmptyFilterOperator;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.filter.OrOperator;
import com.linkedin.pinot.core.operator.filter.ScanBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.SortedInvertedIndexBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexOperator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
        filterQueryTree, _brokerRequest)) {
      operator = new StarTreeIndexOperator(_segment, _brokerRequest);
    } else {
      operator = constructPhysicalOperator(filterQueryTree, _segment);
    }
    long end = System.currentTimeMillis();
    LOGGER.debug("FilterPlanNode.run took:{}", (end - start));
    return operator;
  }

  /**
   * Helper method to build the operator tree from the filter query tree.
   * @param filterQueryTree
   * @param segment Index segment
   * @return Filter Operator created
   */
  @VisibleForTesting
  public static BaseFilterOperator constructPhysicalOperator(FilterQueryTree filterQueryTree, IndexSegment segment) {
    if (filterQueryTree == null) {
      return new MatchEntireSegmentOperator(segment.getSegmentMetadata().getTotalRawDocs());
    }

    // For non-leaf node, recursively create the child filter operators
    FilterOperator filterType = filterQueryTree.getOperator();
    if (filterType == FilterOperator.AND || filterType == FilterOperator.OR) {
      List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
      List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
      if (filterType == FilterOperator.AND) {
        for (FilterQueryTree childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, segment);
          if (childFilterOperator.isResultEmpty()) {
            return EmptyFilterOperator.getInstance();
          }
          childFilterOperators.add(childFilterOperator);
        }
        reorder(childFilterOperators);
        return new AndOperator(childFilterOperators);
      } else {
        for (FilterQueryTree childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, segment);
          if (!childFilterOperator.isResultEmpty()) {
            childFilterOperators.add(childFilterOperator);
          }
        }
        if (childFilterOperators.isEmpty()) {
          return EmptyFilterOperator.getInstance();
        } else if (childFilterOperators.size() == 1) {
          return childFilterOperators.get(0);
        } else {
          reorder(childFilterOperators);
          return new OrOperator(childFilterOperators);
        }
      }
    } else {
      Predicate predicate = Predicate.newPredicate(filterQueryTree);
      DataSource dataSource = segment.getDataSource(filterQueryTree.getColumn());
      PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dataSource);
      if (predicateEvaluator.isAlwaysFalse()) {
        return EmptyFilterOperator.getInstance();
      }

      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
      int startDocId = 0;
      // TODO: make it exclusive
      // NOTE: end is inclusive
      int endDocId = segment.getSegmentMetadata().getTotalRawDocs() - 1;

      // Use inverted index if the filter type is not RANGE or REGEXP_LIKE for efficiency
      if (dataSourceMetadata.hasInvertedIndex() && (filterType != FilterOperator.RANGE) && (filterType
          != FilterOperator.REGEXP_LIKE)) {
        if (dataSourceMetadata.isSorted()) {
          return new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
        } else {
          return new BitmapBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
        }
      } else {
        return new ScanBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
      }
    }
  }

  /**
   * Re orders operators, puts Sorted -> Inverted and then Raw scan. TODO: With Inverted, we can
   * further optimize based on cardinality
   * @param operators
   */
  private static void reorder(List<BaseFilterOperator> operators) {

    final Map<Operator, Integer> operatorPriorityMap = new HashMap<>();
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
