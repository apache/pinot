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


/// Completes the order of a GROUP BY query on its group-by expressions, so that groups the query ranks alike still have
/// one order: every server and the broker then keep the same groups, and the result is the same on every run.
///
/// A query with an ORDER BY gets the group-by expressions it does not order on appended, ascending and in the GROUP BY
/// order; its own terms keep their place, direction and null ordering. HAVING and `sortAggregateLimitThreshold` do
/// not change this. Whether the servers may keep only `LIMIT` groups is decided apart from it, by an ORDER BY on
/// exactly the group-by expressions without HAVING (`QueryContext.isUnsafeTrim()`): a query ordered on an aggregate or
/// with HAVING is trimmed as before, while one ordered on some of its group-by expressions without HAVING is then
/// ordered on all of them and may be trimmed to `LIMIT`, its combine picked by the threshold.
///
/// A query without an ORDER BY is ordered on all its group-by expressions, ascending, when the servers then merge it in
/// the sorted combine: without HAVING, and with a `LIMIT` below `sortAggregateLimitThreshold`, as in
/// `QueryContext.shouldSortAggregateUnderSafeTrim()`. Without an ORDER BY, a server stops adding groups once it holds
/// enough of them for the result, so two servers can keep different groups, and a group the broker returns can miss the
/// values of the servers that dropped it. Ordered on the group keys, every segment, server and the broker keep the
/// smallest keys. Without HAVING the trim is safe: each segment keeps only `LIMIT` groups, and each returned group is
/// complete as long as no segment reaches `numGroupsLimit`. With HAVING, or a `LIMIT` at or above the threshold, the
/// servers would not take the sorted combine, and in the other combines the ordered query keeps more groups than the
/// query as written and sorts them, which costs more memory and time, so such a query is left as written.
///
/// A GROUPING SETS, ROLLUP or CUBE query is left as written: its visible group-by expressions do not tell apart every
/// group it returns, and without an ORDER BY the servers keep all its groups, so none of them misses values.
///
/// This rewriter is not in [QueryRewriterFactory#DEFAULT_QUERY_REWRITERS_CLASS_NAMES]. The single-stage broker applies
/// it to the query it sends to the servers, so materialized view definitions and the multi-stage engine are unchanged.
///
/// E.g.
/// SELECT col1, col2, SUM(col3) FROM foo GROUP BY col1, col2 ORDER BY SUM(col3) DESC, col1 DESC LIMIT 10
///   --> SELECT col1, col2, SUM(col3) FROM foo GROUP BY col1, col2 ORDER BY SUM(col3) DESC, col1 DESC, col2 LIMIT 10
/// SELECT col1, SUM(col2) FROM foo GROUP BY col1 LIMIT 10
///   --> SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY col1 LIMIT 10
///
/// This rewriter is stateless and thread-safe; each invocation mutates its own query.
public class OrderByGroupKeysRewriter implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    if (pinotQuery.getGroupByListSize() == 0 || pinotQuery.getGroupingSets() != null) {
      return pinotQuery;
    }
    if (pinotQuery.getOrderByListSize() > 0) {
      completeOrderBy(pinotQuery);
    } else if (pinotQuery.getHavingExpression() == null && isLimitBelowSortAggregateLimitThreshold(pinotQuery)) {
      List<Expression> groupByList = pinotQuery.getGroupByList();
      List<Expression> orderByList = new ArrayList<>(groupByList.size());
      for (Expression groupBy : groupByList) {
        orderByList.add(ascending(groupBy));
      }
      pinotQuery.setOrderByList(orderByList);
    }
    // TODO: Also order a query without ORDER BY that has HAVING or a LIMIT at or above the threshold. It needs a
    //       separate combine algorithm for a deterministic ordering, since ordered it costs much more in the existing
    //       combines than as written.
    return pinotQuery;
  }

  /// Appends to the query's ORDER BY each group-by expression it does not order on, in the GROUP BY order.
  private static void completeOrderBy(PinotQuery pinotQuery) {
    List<Expression> orderByList = new ArrayList<>(pinotQuery.getOrderByList());
    List<Expression> ordered = new ArrayList<>(orderByList.size());
    for (Expression orderBy : orderByList) {
      ordered.add(CalciteSqlParser.removeOrderByFunctions(orderBy));
    }
    boolean appended = false;
    for (Expression groupBy : pinotQuery.getGroupByList()) {
      if (!ordered.contains(groupBy)) {
        ordered.add(groupBy);
        orderByList.add(ascending(groupBy));
        appended = true;
      }
    }
    if (appended) {
      pinotQuery.setOrderByList(orderByList);
    }
  }

  /// An ascending ORDER BY term on a copy of the group-by expression, as the parser builds a written ORDER BY, so that
  /// a later rewrite of one list does not change the other.
  private static Expression ascending(Expression groupBy) {
    return RequestUtils.getFunctionExpression(CalciteSqlParser.ASC, groupBy.deepCopy());
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
