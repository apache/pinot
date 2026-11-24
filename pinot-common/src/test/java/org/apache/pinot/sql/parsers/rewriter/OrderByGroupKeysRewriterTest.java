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

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class OrderByGroupKeysRewriterTest {
  private static final QueryRewriter QUERY_REWRITER = new OrderByGroupKeysRewriter();

  @Test
  public void testQueryRewrite() {
    testQueryRewrite("SELECT col1, SUM(col2) FROM foo GROUP BY col1",
        "SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY col1");
    // Every group-by expression, in the GROUP BY order
    testQueryRewrite("SELECT col1, col2, COUNT(*) FROM foo GROUP BY col2, col1 LIMIT 10",
        "SELECT col1, col2, COUNT(*) FROM foo GROUP BY col2, col1 ORDER BY col2, col1 LIMIT 10");
    // A group-by expression the query does not select
    testQueryRewrite("SELECT COUNT(*) FROM foo GROUP BY col1", "SELECT COUNT(*) FROM foo GROUP BY col1 ORDER BY col1");
    testQueryRewrite("SELECT DATETRUNC('DAY', ts) AS day, MAX(col2) FROM foo GROUP BY day LIMIT 10 OFFSET 20",
        "SELECT DATETRUNC('DAY', ts) AS day, MAX(col2) FROM foo GROUP BY day ORDER BY DATETRUNC('DAY', ts) "
            + "LIMIT 10 OFFSET 20");
    // The largest LIMIT below the default sortAggregateLimitThreshold (10,000)
    testQueryRewrite("SELECT col1, SUM(col2) FROM foo GROUP BY col1 LIMIT 9999",
        "SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY col1 LIMIT 9999");
    // A LIMIT below the sortAggregateLimitThreshold of the query
    testQueryRewrite("SET sortAggregateLimitThreshold = 100001; SELECT col1, SUM(col2) FROM foo GROUP BY col1 "
            + "LIMIT 100000",
        "SET sortAggregateLimitThreshold = 100001; SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY col1 "
            + "LIMIT 100000");
    // OrderByLiteralRemover drops a literal ORDER BY key, so the query compiles without an ORDER BY
    testQueryRewrite("SELECT col1, COUNT(*) FROM foo GROUP BY col1 ORDER BY 'a'",
        "SELECT col1, COUNT(*) FROM foo GROUP BY col1 ORDER BY col1");
    // A query without options, as one not built by the parser can be, against the default threshold
    PinotQuery withoutOptions =
        CalciteSqlParser.compileToPinotQuery("SELECT col1, SUM(col2) FROM foo GROUP BY col1 LIMIT 10");
    withoutOptions.setQueryOptions(null);
    PinotQuery expected =
        CalciteSqlParser.compileToPinotQuery("SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY col1 LIMIT 10");
    expected.setQueryOptions(null);
    assertEquals(QUERY_REWRITER.rewrite(withoutOptions), expected);
  }

  private void testQueryRewrite(String original, String expected) {
    assertEquals(QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(original)),
        CalciteSqlParser.compileToPinotQuery(expected));
  }

  @Test
  public void testSkipQueryRewrite() {
    testSkipQueryRewrite("SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY col1 DESC");
    testSkipQueryRewrite("SELECT col1, SUM(col2) FROM foo GROUP BY col1 ORDER BY SUM(col2) DESC LIMIT 10");
    testSkipQueryRewrite("SELECT col1, col2, SUM(col3) FROM foo GROUP BY col1, col2 ORDER BY col2");
    testSkipQueryRewrite("SELECT col1, col2, SUM(col3) FROM foo GROUP BY ROLLUP(col1, col2)");
    testSkipQueryRewrite("SELECT col1, col2, SUM(col3) FROM foo GROUP BY CUBE(col1, col2)");
    testSkipQueryRewrite("SELECT col1, col2, SUM(col3) FROM foo GROUP BY GROUPING SETS ((col1, col2), (col1), ())");
    // A GROUP BY without an aggregation compiles into a DISTINCT, which has no GROUP BY
    testSkipQueryRewrite("SELECT col1 FROM foo GROUP BY col1");
    testSkipQueryRewrite("SELECT DISTINCT col1 FROM foo");
    testSkipQueryRewrite("SELECT SUM(col1) FROM foo");
    testSkipQueryRewrite("SELECT * FROM foo");
    // The servers would not take the sorted combine
    testSkipQueryRewrite("SELECT col1, SUM(col2) FROM foo GROUP BY col1 HAVING SUM(col2) > 10");
    testSkipQueryRewrite("SELECT col1, SUM(col2) FROM foo GROUP BY col1 LIMIT 10000");
    testSkipQueryRewrite(
        "SET sortAggregateLimitThreshold = 100; SELECT col1, SUM(col2) FROM foo GROUP BY col1 LIMIT 100");
    // Left for the servers to reject
    testSkipQueryRewrite("SET sortAggregateLimitThreshold = 0; SELECT col1, SUM(col2) FROM foo GROUP BY col1");
  }

  private void testSkipQueryRewrite(String query) {
    assertEquals(QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(query)),
        CalciteSqlParser.compileToPinotQuery(query));
  }

  @Test
  public void testOrderByDoesNotShareTheGroupByExpressions() {
    String query = "SELECT DATETRUNC('DAY', ts), COUNT(*) FROM foo GROUP BY DATETRUNC('DAY', ts)";
    PinotQuery pinotQuery = QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(query));
    pinotQuery.getGroupByList().get(0).getFunctionCall().getOperands()
        .set(1, RequestUtils.getIdentifierExpression("otherTs"));
    assertEquals(pinotQuery.getOrderByList(),
        CalciteSqlParser.compileToPinotQuery(query + " ORDER BY DATETRUNC('DAY', ts)").getOrderByList());
  }
}
