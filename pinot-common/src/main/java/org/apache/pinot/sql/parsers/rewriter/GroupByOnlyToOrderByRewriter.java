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
package org.apache.pinot.sql.parsers.rewriter;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class GroupByOnlyToOrderByRewriter implements QueryRewriter {
  /**
   * Rewrite aggregate group by only query to an order by query based on the group by expressions. Converting group by
   * queries to order by queries can improve the accuracy and provide deterministic query results.
   * Caveat: This can result in group by queries becoming more expensive due to ordering
   *
   * E.g.
   * ```
   *   SELECT col1, col2, max(col3) FROM foo GROUP BY col1, col2 => SELECT col1, col2, max(col3) FROM foo GROUP BY
   *          col1, col2 ORDER BY col1, col2
   *   SELECT col1, avg(col2) FROM foo GROUP BY col1 => SELECT col1, avg(col2) FROM foo GROUP BY col1 ORDER BY col1
   * ```
   * @param pinotQuery
   */
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    if (pinotQuery.getOrderByListSize() > 0) {
      // No need to add order-by if it is already present
      return pinotQuery;
    }

    boolean hasAggregation = false;
    for (Expression select : pinotQuery.getSelectList()) {
      if (CalciteSqlParser.isAggregateExpression(select)) {
        hasAggregation = true;
      }
    }

    if (hasAggregation && pinotQuery.getGroupByListSize() > 0) {
      // Convert the group-by expressions into order-by expressions with ascending ordering. This is done to get
      // better accuracy and deterministic results for group-by aggregate queries without order-by
      List<Expression> orderByExpr = new ArrayList<>();
      for (Expression expression : pinotQuery.getGroupByList()) {
        Expression orderByExpression = RequestUtils.getFunctionExpression("asc");
        orderByExpression.getFunctionCall().addToOperands(expression);
        orderByExpr.add(orderByExpression);
      }
      pinotQuery.setOrderByList(orderByExpr);
    }
    return pinotQuery;
  }
}
