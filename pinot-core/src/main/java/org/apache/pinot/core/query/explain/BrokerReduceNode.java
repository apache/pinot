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

package org.apache.pinot.core.query.explain;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * BrokerReduceNode for the output of EXPLAIN PLAN queries
 */
public class BrokerReduceNode implements ExplainPlanTreeNode {

  private String _name = "BROKER_REDUCE";
  private String _havingFilter; // not sure if we want a tree structure
  private String _sort;
  private int _limit;
  private Set<String> _postAggregations = new HashSet<>();
  private ExplainPlanTreeNode[] _childNodes = new ExplainPlanTreeNode[1];

  public BrokerReduceNode(QueryContext queryContext, TableConfig tableConfig) {
    _havingFilter = queryContext.getHavingFilter() != null ? queryContext.getHavingFilter().toString() : null;
    _sort = queryContext.getOrderByExpressions() != null ? queryContext.getOrderByExpressions().toString() : null;
    _limit = queryContext.getLimit();
    if (QueryContextUtils.isAggregationQuery(queryContext) && queryContext.getGroupByExpressions() == null) {
      // queries with 1 or more aggregation only functions always returns at most 1 row
      _limit = 1;
    }
    Set<String> regularTransforms = new HashSet<>();
    QueryContextUtils.generateTransforms(queryContext, _postAggregations, regularTransforms);
    _childNodes[0] = new ServerCombineNode(queryContext, regularTransforms, tableConfig);
  }

  @Override
  public ExplainPlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_name).append('(');
    if (_havingFilter != null) {
      stringBuilder.append("havingFilter").append(':').append(_havingFilter).append(',');
    }
    if (_sort != null) {
      stringBuilder.append("sort").append(':').append(_sort).append(',');
    }
    stringBuilder.append("limit:").append(_limit);
    if (!_postAggregations.isEmpty()) {
      stringBuilder.append(",postAggregations:");
      int count = 0;
      for (String func : _postAggregations) {
        if (count == _postAggregations.size() - 1) {
          stringBuilder.append(func);
        } else {
          stringBuilder.append(func).append(", ");
        }
        count++;
      }
    }
    return stringBuilder.append(')').toString();
  }
}
