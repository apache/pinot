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
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/// Removes literal ORDER BY keys, which impose no ordering constraint. Run after constant folding and alias/ordinal
/// resolution. This rewriter is stateless and thread-safe; each invocation mutates its own query.
public class OrderByLiteralRemover implements QueryRewriter {
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      List<Expression> orderByExpressions = new ArrayList<>(orderByList.size());
      for (Expression expression : orderByList) {
        if (!CalciteSqlParser.removeOrderByFunctions(expression).isSetLiteral()) {
          orderByExpressions.add(expression);
        }
      }
      pinotQuery.setOrderByList(orderByExpressions.isEmpty() ? null : orderByExpressions);
    }
    return pinotQuery;
  }
}
