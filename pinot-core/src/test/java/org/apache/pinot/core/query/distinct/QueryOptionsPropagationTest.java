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
package org.apache.pinot.core.query.distinct;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class QueryOptionsPropagationTest {

  @Test
  public void shouldPropagateQueryOptionsIntoQueryContext()
      throws Exception {
    String sql = "SELECT DISTINCT intCol FROM myTable LIMIT 10";
    String payload = "{\"sql\":\"" + sql + "\","
        + "\"queryOptions\":\"maxRowsInDistinct=7;numRowsWithoutChangeInDistinct=11\"}";
    SqlNodeAndOptions sqlNodeAndOptions = RequestUtils.parseQuery(sql, JsonUtils.stringToJsonNode(payload));
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);

    assertEquals(queryContext.getQueryOptions().get(QueryOptionKey.MAX_ROWS_IN_DISTINCT), "7");
    assertEquals(queryContext.getQueryOptions().get(QueryOptionKey.NUM_ROWS_WITHOUT_CHANGE_IN_DISTINCT), "11");
    assertEquals(QueryOptionsUtils.getMaxRowsInDistinct(queryContext.getQueryOptions()).intValue(), 7);
    assertEquals(QueryOptionsUtils.getNumRowsWithoutChangeInDistinct(queryContext.getQueryOptions()).intValue(), 11);
  }
}
