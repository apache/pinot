/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.predicate.Predicate;
import org.apache.pinot.core.common.predicate.PredicateFactory;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
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
  public BaseFilterOperator run() {
    FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(_brokerRequest);
    return constructPhysicalOperator(rootFilterNode, _segment, _brokerRequest.getDebugOptions());
  }

  /**
   * Helper method to build the operator tree from the filter query tree.
   */
  private static BaseFilterOperator constructPhysicalOperator(FilterQueryTree filterQueryTree, IndexSegment segment,
      @Nullable Map<String, String> debugOptions) {
    int numDocs = segment.getSegmentMetadata().getTotalRawDocs();
    if (filterQueryTree == null) {
      return new MatchAllFilterOperator(numDocs);
    }

    // For non-leaf node, recursively create the child filter operators
    FilterOperator filterType = filterQueryTree.getOperator();
    if (filterType == FilterOperator.AND || filterType == FilterOperator.OR) {
      // Non-leaf filter operator
      List<FilterQueryTree> childFilters = filterQueryTree.getChildren();
      List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
      if (filterType == FilterOperator.AND) {
        // AND operator
        for (FilterQueryTree childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, segment, debugOptions);
          if (childFilterOperator.isResultEmpty()) {
            // Return empty filter operator if any of the child filter operator's result is empty
            return EmptyFilterOperator.getInstance();
          } else if (!childFilterOperator.isResultMatchingAll()) {
            // Remove child filter operators that match all records
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getAndFilterOperator(childFilterOperators, numDocs, debugOptions);
      } else {
        // OR operator
        for (FilterQueryTree childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, segment, debugOptions);
          if (childFilterOperator.isResultMatchingAll()) {
            // Return match all filter operator if any of the child filter operator matches all records
            return new MatchAllFilterOperator(numDocs);
          } else if (!childFilterOperator.isResultEmpty()) {
            // Remove child filter operators whose result is empty
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getOrFilterOperator(childFilterOperators, numDocs, debugOptions);
      }
    } else {
      // Leaf filter operator
      Predicate predicate = PredicateFactory.getPredicate(filterQueryTree);

      TransformExpressionTree expression = filterQueryTree.getExpression();
      if (expression.getExpressionType() == TransformExpressionTree.ExpressionType.FUNCTION) {

        return new ExpressionFilterOperator(segment, expression, predicate);
      } else {
        DataSource dataSource = segment.getDataSource(filterQueryTree.getColumn());
        PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider
            .getPredicateEvaluator(predicate, dataSource.getDictionary(),
                dataSource.getDataSourceMetadata().getDataType());
        return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
    }
  }

  @Override
  public void showTree(String prefix) {
    final String treeStructure =
        prefix + "Filter Plan Node\n" + prefix + "Operator: Filter\n" + prefix + "Argument 0: " + _brokerRequest
            .getFilterQuery();
    LOGGER.debug(treeStructure);
  }
}
