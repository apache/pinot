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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class NonAggregationGroupByToDistinctQueryRewriter implements QueryRewriter {
  /**
   * Rewrite non-aggregate group by query to distinct query.
   * E.g.
   * ```
   *   SELECT col1+col2*5 FROM foo GROUP BY col1, col2 => SELECT distinct col1+col2*5 FROM foo
   *   SELECT col1, col2 FROM foo GROUP BY col1, col2 => SELECT distinct col1, col2 FROM foo
   * ```
   * @param pinotQuery
   */
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    boolean hasAggregation = false;
    for (Expression select : pinotQuery.getSelectList()) {
      if (CalciteSqlParser.isAggregateExpression(select)) {
        hasAggregation = true;
      }
    }
    if (pinotQuery.getOrderByList() != null) {
      for (Expression orderBy : pinotQuery.getOrderByList()) {
        if (CalciteSqlParser.isAggregateExpression(orderBy)) {
          hasAggregation = true;
        }
      }
    }
    if (!hasAggregation && pinotQuery.getGroupByListSize() > 0) {
      Set<String> selectIdentifiers = CalciteSqlParser.extractIdentifiers(pinotQuery.getSelectList(), true);
      Set<String> groupByIdentifiers = CalciteSqlParser.extractIdentifiers(pinotQuery.getGroupByList(), true);
      if (groupByIdentifiers.containsAll(selectIdentifiers)) {
        Expression distinctExpression = RequestUtils.getFunctionExpression("DISTINCT");
        for (Expression select : pinotQuery.getSelectList()) {
          if (CalciteSqlParser.isAsFunction(select)) {
            Function asFunc = select.getFunctionCall();
            distinctExpression.getFunctionCall().addToOperands(asFunc.getOperands().get(0));
          } else {
            distinctExpression.getFunctionCall().addToOperands(select);
          }
        }
        pinotQuery.setSelectList(Arrays.asList(distinctExpression));
        pinotQuery.setGroupByList(Collections.emptyList());
      } else {
        selectIdentifiers.removeAll(groupByIdentifiers);
        throw new SqlCompilationException(String.format(
            "For non-aggregation group by query, all the identifiers in select clause should be in groupBys. Found "
                + "identifier: %s", Arrays.toString(selectIdentifiers.toArray(new String[0]))));
      }
    }
    return pinotQuery;
  }
}
