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
import java.util.Map;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/// Orders a GROUP BY query without an ORDER BY on all its group-by expressions, ascending, when the servers then merge
/// it in the sorted combine: without HAVING, and with a `LIMIT` below `sortAggregateLimitThreshold`, as in
/// `QueryContext.shouldSortAggregateUnderSafeTrim()`.
///
/// Without an ORDER BY, a server stops adding groups once it holds enough of them for the result, so two servers can
/// keep different groups, and a group the broker returns can miss the values of the servers that dropped it. Ordered on
/// the group keys, every segment, server and the broker keep the smallest keys, so the servers keep the same groups and
/// the result is the same on every run. Without HAVING the trim is safe: each segment keeps only `LIMIT` groups, and
/// each returned group is complete as long as no segment reaches `numGroupsLimit`.
///
/// Not rewritten:
/// - a query that already has an ORDER BY;
/// - a GROUPING SETS, ROLLUP or CUBE query: the servers keep all its groups, so none of them misses values;
/// - a query with HAVING, or with a `LIMIT` at or above `sortAggregateLimitThreshold`: the servers would not take the
///   sorted combine, and in the other combines the ordered query keeps more groups than the query as written and sorts
///   them, which costs more memory and time.
///
/// This rewriter is not in [QueryRewriterFactory#DEFAULT_QUERY_REWRITERS_CLASS_NAMES]. The single-stage broker applies
/// it to the query it sends to the servers, so materialized view definitions and the multi-stage engine are unchanged.
///
/// E.g.
/// SELECT col1, SUM(col2) FROM foo GROUP BY col1 LIMIT 10
///   --> SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY col1 LIMIT 10
///
/// This rewriter is stateless and thread-safe; each invocation mutates its own query.
public class OrderByGroupKeysRewriter implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    if (pinotQuery.getGroupByListSize() == 0 || pinotQuery.getOrderByListSize() > 0
        || pinotQuery.getGroupingSets() != null || pinotQuery.getHavingExpression() != null
        || !isLimitBelowSortAggregateLimitThreshold(pinotQuery)) {
      return pinotQuery;
    }
    List<Expression> groupByList = pinotQuery.getGroupByList();
    List<Expression> orderByList = new ArrayList<>(groupByList.size());
    for (Expression groupBy : groupByList) {
      // Copied, as the parser builds a written ORDER BY, so that a later rewrite of one list does not change the other
      orderByList.add(RequestUtils.getFunctionExpression(CalciteSqlParser.ASC, groupBy.deepCopy()));
    }
    pinotQuery.setOrderByList(orderByList);
    return pinotQuery;
  }

  /// Reads `sortAggregateLimitThreshold` as the servers read it. An invalid value leaves the query unchanged, for the
  /// servers to reject.
  private static boolean isLimitBelowSortAggregateLimitThreshold(PinotQuery pinotQuery) {
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    Integer sortAggregateLimitThreshold;
    try {
      sortAggregateLimitThreshold =
          queryOptions != null ? QueryOptionsUtils.getSortAggregateLimitThreshold(queryOptions) : null;
    } catch (IllegalArgumentException e) {
      return false;
    }
    return pinotQuery.getLimit() < (sortAggregateLimitThreshold != null ? sortAggregateLimitThreshold
        : Server.DEFAULT_SORT_AGGREGATE_LIMIT_THRESHOLD);
  }
}
