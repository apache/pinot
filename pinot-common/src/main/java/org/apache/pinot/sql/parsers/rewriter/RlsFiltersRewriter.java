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

import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;

/**
 * A query rewriter that applies Row-Level Security (RLS) filters to Pinot queries.
 *
 * <p>This rewriter examines query options for table-specific row filters and automatically
 * applies them to the query's WHERE clause. The RLS filters are retrieved from the query
 * options using the prefixed table name as the key.
 *
 * <p>The rewriter performs the following operations:
 * <ul>
 *   <li>Extracts the raw table name from the query's data source</li>
 *   <li>Looks up RLS filters from query options using the table name</li>
 *   <li>Parses the filter string into an Expression object</li>
 *   <li>Combines the RLS filter with any existing WHERE clause using AND logic</li>
 * </ul>
 *
 * <p>If no query options are present, no RLS filters are found for the table, or the
 * filter string is empty, the query is returned unchanged.
 *
 * @see QueryRewriter
 */
public class RlsFiltersRewriter implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    if (MapUtils.isEmpty(queryOptions)) {
      return pinotQuery;
    }
    String tableName = pinotQuery.getDataSource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    String rowFilters = RlsUtils.getRlsFilterForTable(queryOptions, rawTableName);

    if (Strings.isEmpty(rowFilters)) {
      return pinotQuery;
    }

    Expression expression = CalciteSqlParser.compileToExpression(rowFilters);
    Expression existingFilterExpression = pinotQuery.getFilterExpression();
    if (existingFilterExpression != null) {
      Expression combinedFilterExpression =
          RequestUtils.getFunctionExpression(FilterKind.AND.name(), List.of(expression, existingFilterExpression));
      pinotQuery.setFilterExpression(combinedFilterExpression);
    } else {
      pinotQuery.setFilterExpression(expression);
    }
    return pinotQuery;
  }
}
