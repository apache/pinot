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


public class ArgMinMaxRewriterTest {
  private static final QueryRewriter QUERY_REWRITER = new ArgMinMaxRewriter();

  @Test
  public void testQueryRewrite() {
    testQueryRewrite("SELECT ARG_MIN(col1,col2), ARG_MIN(col1,col3) FROM myTable",
        "SELECT PINOT_CHILD_AGGREGATION_ARG_MIN(0,col2,col1,col2), "
            + "PINOT_CHILD_AGGREGATION_ARG_MIN(0,col3,col1,col3),"
            + "PINOT_PARENT_AGGREGATION_ARG_MIN(0,1,col1,col2,col3) FROM myTable");

    testQueryRewrite("SELECT ARG_MIN(col1,col2), ARG_MIN(col1,col2) FROM myTable",
        "SELECT PINOT_CHILD_AGGREGATION_ARG_MIN(0,col2,col1,col2),"
            + "PINOT_PARENT_AGGREGATION_ARG_MIN(0,1,col1,col2) FROM myTable");

    testQueryRewrite("SELECT ARG_MIN(col1,col2,col5), ARG_MIN(col1,col2,col6), ARG_MAX(col1,col2,col6) "
            + "FROM myTable",
        "SELECT PINOT_CHILD_AGGREGATION_ARG_MIN(0,col5,col1,col2,col5), "
            + "PINOT_CHILD_AGGREGATION_ARG_MIN(0,col6,col1,col2,col6), "
            + "PINOT_CHILD_AGGREGATION_ARG_MAX(0,col6,col1,col2,col6),"
            + "PINOT_PARENT_AGGREGATION_ARG_MIN(0,2,col1,col2,col6,col5),"
            + "PINOT_PARENT_AGGREGATION_ARG_MAX(0,2,col1,col2,col6) FROM myTable");
  }

  private void testQueryRewrite(String original, String expected) {
    assertEquals(QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(original)),
        CalciteSqlParser.compileToPinotQuery(expected));
  }

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
