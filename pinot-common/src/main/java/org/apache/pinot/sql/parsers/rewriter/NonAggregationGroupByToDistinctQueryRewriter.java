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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Rewrite non-aggregation group-by query to distinct query.
 * The query can be rewritten only if select expression set and group-by expression set are the same.
 *
 * E.g.
 * SELECT col1, col2 FROM foo GROUP BY col1, col2 --> SELECT DISTINCT col1, col2 FROM foo
 * SELECT col1, col2 FROM foo GROUP BY col2, col1 --> SELECT DISTINCT col1, col2 FROM foo
 * SELECT col1 + col2 FROM foo GROUP BY col1 + col2 --> SELECT DISTINCT col1 + col2 FROM foo
 * SELECT col1 AS c1 FROM foo GROUP BY col1 --> SELECT DISTINCT col1 AS c1 FROM foo
 * SELECT col1, col1 AS c1, col2 FROM foo GROUP BY col1, col2 --> SELECT DISTINCT col1, col1 AS ci, col2 FROM foo
 *
 * Unsupported queries:
 * SELECT col1 FROM foo GROUP BY col1, col2 (not equivalent to SELECT DISTINCT col1 FROM foo)
 * SELECT col1 + col2 FROM foo GROUP BY col1, col2 (not equivalent to SELECT col1 + col2 FROM foo)
 */
public class NonAggregationGroupByToDistinctQueryRewriter implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    if (pinotQuery.getGroupByListSize() == 0) {
      return pinotQuery;
    }
    for (Expression select : pinotQuery.getSelectList()) {
      if (CalciteSqlParser.isAggregateExpression(select)) {
        return pinotQuery;
      }
    }
    if (pinotQuery.getOrderByList() != null) {
      for (Expression orderBy : pinotQuery.getOrderByList()) {
        if (CalciteSqlParser.isAggregateExpression(orderBy)) {
          return pinotQuery;
        }
      }
    }

    // This rewriter is applied after AliasApplier, so all the alias in group-by are already replaced with expressions
    Set<Expression> selectExpressions = new HashSet<>();
    for (Expression select : pinotQuery.getSelectList()) {
      Function function = select.getFunctionCall();
      if (function != null && function.getOperator().equals("as")) {
        selectExpressions.add(function.getOperands().get(0));
      } else {
        selectExpressions.add(select);
      }
    }
    Set<Expression> groupByExpressions = new HashSet<>(pinotQuery.getGroupByList());
    if (selectExpressions.equals(groupByExpressions)) {
      Expression distinct = RequestUtils.getFunctionExpression("distinct");
      distinct.getFunctionCall().setOperands(pinotQuery.getSelectList());
      pinotQuery.setSelectList(Collections.singletonList(distinct));
      pinotQuery.setGroupByList(null);
      return pinotQuery;
    } else {
      throw new SqlCompilationException(String.format(
          "For non-aggregation group-by query, select expression set and group-by expression set should be the same. "
              + "Found select: %s, group-by: %s", selectExpressions, groupByExpressions));
    }
  }
}
