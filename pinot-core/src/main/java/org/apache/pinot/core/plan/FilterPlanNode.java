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
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.TextMatchPredicate;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.TextMatchFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;


public class FilterPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public FilterPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public BaseFilterOperator run() {
    FilterQueryTree rootFilterNode = RequestUtils.generateFilterQueryTree(_queryContext.getBrokerRequest());
    return constructPhysicalOperator(rootFilterNode, _indexSegment, _queryContext.getDebugOptions());
  }

  /**
   * Helper method to build the operator tree from the filter query tree.
   */
  private static BaseFilterOperator constructPhysicalOperator(FilterQueryTree filterQueryTree, IndexSegment segment,
      @Nullable Map<String, String> debugOptions) {
    int numDocs = segment.getSegmentMetadata().getTotalDocs();
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
      Predicate predicate = Predicate.newPredicate(filterQueryTree);

      TransformExpressionTree expression = filterQueryTree.getExpression();
      if (expression.getExpressionType() == TransformExpressionTree.ExpressionType.FUNCTION) {
        // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (IS_NULL,
        //       IS_NOT_NULL, TEXT_MATCH)
        return new ExpressionFilterOperator(segment, expression, predicate);
      }

      // Single column filter
      String column = filterQueryTree.getColumn();
      DataSource dataSource = segment.getDataSource(column);
      Predicate.Type predicateType = predicate.getType();
      switch (predicateType) {
        // Specialize predicate types without PredicateEvaluator
        case IS_NULL:
          NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
          if (nullValueVector != null) {
            return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), false, numDocs);
          } else {
            return EmptyFilterOperator.getInstance();
          }
        case IS_NOT_NULL:
          nullValueVector = dataSource.getNullValueVector();
          if (nullValueVector != null) {
            return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), true, numDocs);
          } else {
            return new MatchAllFilterOperator(numDocs);
          }
        case TEXT_MATCH:
          return new TextMatchFilterOperator(dataSource.getInvertedIndex(),
              ((TextMatchPredicate) predicate).getSearchQuery(), numDocs);
        default:
          PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider
              .getPredicateEvaluator(predicate, dataSource.getDictionary(),
                  dataSource.getDataSourceMetadata().getDataType());
          return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs);
      }
    }
  }
}
