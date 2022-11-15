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

import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class NonAggregationGroupByToDistinctQueryRewriterTest {
  private static final QueryRewriter QUERY_REWRITER = new NonAggregationGroupByToDistinctQueryRewriter();

  @Test
  public void testQueryRewrite() {
    testQueryRewrite("SELECT A FROM myTable GROUP BY A", "SELECT DISTINCT A FROM myTable");
    testQueryRewrite("SELECT col1, col2 FROM foo GROUP BY col1, col2", "SELECT DISTINCT col1, col2 FROM foo");
    testQueryRewrite("SELECT col1, col2 FROM foo GROUP BY col2, col1", "SELECT DISTINCT col1, col2 FROM foo");
    testQueryRewrite("SELECT col1+col2*5 FROM foo GROUP BY col1+col2*5", "SELECT DISTINCT col1+col2*5 FROM foo");
    testQueryRewrite("SELECT col1 as col2, col1 as col3 FROM foo GROUP BY col1",
        "SELECT DISTINCT col1 as col2, col1 as col3 FROM foo");
    testQueryRewrite("SELECT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo GROUP BY a,b,c",
        "SELECT DISTINCT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo");
    testQueryRewrite("SELECT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo GROUP BY c, b, a",
        "SELECT DISTINCT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo");
    testQueryRewrite(
        "SELECT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo GROUP BY col1, col2, CONCAT(col3,col4,'')",
        "SELECT DISTINCT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo");
  }

  private void testQueryRewrite(String original, String expected) {
    assertEquals(QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(original)),
        CalciteSqlParser.compileToPinotQuery(expected));
  }

  @Test
  public void testUnsupportedQueries() {
    testUnsupportedQuery("SELECT col1 FROM foo GROUP BY col1, col2");
    testUnsupportedQuery("SELECT col1, col2 FROM foo GROUP BY col1");
    testUnsupportedQuery("SELECT col1 + col2 FROM foo GROUP BY col1, col2");
  }

  private void testUnsupportedQuery(String query) {
    assertThrows(SqlCompilationException.class,
        () -> QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(query)));
  }
}
