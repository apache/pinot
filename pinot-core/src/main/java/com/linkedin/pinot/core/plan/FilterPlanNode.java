/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.AndFilterOperator;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.EmptyFilterOperator;
import com.linkedin.pinot.core.operator.filter.FilterOperatorUtils;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.filter.OrFilterOperator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FilterPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilterPlanNode.class);

  private final IndexSegment _segment;
  private final BrokerRequest _brokerRequest;
  private final boolean _enableScanReorderOptimization;

  public FilterPlanNode(IndexSegment segment, BrokerRequest brokerRequest) {
    _segment = segment;
    _brokerRequest = brokerRequest;
    _enableScanReorderOptimization = FilterOperatorUtils.enableScanReorderOptimization(brokerRequest.getDebugOptions());
  }

  @Override
  public BaseFilterOperator run() {
    FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(_brokerRequest);
    return constructPhysicalOperator(rootFilterNode);
  }

  /**
   * Helper method to build the filter operator from the filter query tree.
   */
  private BaseFilterOperator constructPhysicalOperator(@Nullable FilterQueryTree filterNode) {
    if (filterNode == null) {
      return new MatchEntireSegmentOperator(_segment.getSegmentMetadata().getTotalRawDocs());
    }

    // For non-leaf node, recursively create the child filter operators
    FilterOperator filterType = filterNode.getOperator();
    if (filterType == FilterOperator.AND || filterType == FilterOperator.OR) {
      List<FilterQueryTree> childFilters = filterNode.getChildren();
      List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
      if (filterType == FilterOperator.AND) {
        for (FilterQueryTree childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter);
          if (childFilterOperator.isResultEmpty()) {
            return EmptyFilterOperator.getInstance();
          }
          childFilterOperators.add(childFilterOperator);
        }
        FilterOperatorUtils.reorderFilterOperators(childFilterOperators, _enableScanReorderOptimization);
        return new AndFilterOperator(childFilterOperators);
      } else {
        for (FilterQueryTree childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter);
          if (!childFilterOperator.isResultEmpty()) {
            childFilterOperators.add(childFilterOperator);
          }
        }
        if (childFilterOperators.isEmpty()) {
          return EmptyFilterOperator.getInstance();
        } else if (childFilterOperators.size() == 1) {
          return childFilterOperators.get(0);
        } else {
          return new OrFilterOperator(childFilterOperators);
        }
      }
    } else {
      Predicate predicate = Predicate.newPredicate(filterNode);
      DataSource dataSource = _segment.getDataSource(filterNode.getColumn());
      PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource);
      int startDocId = 0;
      // TODO: make it exclusive
      // NOTE: end is inclusive
      int endDocId = _segment.getSegmentMetadata().getTotalRawDocs() - 1;
      return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
    }
  }

  @Override
  public void showTree(String prefix) {
    final String treeStructure = prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 0: "
        + _brokerRequest.getFilterQuery();
    LOGGER.debug(treeStructure);
  }
}
