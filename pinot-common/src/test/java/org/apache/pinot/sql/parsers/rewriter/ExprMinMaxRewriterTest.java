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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ExprMinMaxRewriterTest {
  private static final QueryRewriter QUERY_REWRITER = new ExprMinMaxRewriter();

  @Test
  public void testQueryRewrite() {
    testQueryRewrite("SELECT EXPR_MIN(col2,col1), EXPR_MIN(col3,col1) FROM myTable",
        "SELECT CHILD_EXPR_MIN(0,col2,col2,col1), "
            + "CHILD_EXPR_MIN(0,col3,col3,col1),"
            + "PARENT_EXPR_MIN(0,1,col1,col2,col3) FROM myTable");

    testQueryRewrite("SELECT EXPR_MIN(col2,col1), EXPR_MIN(col2,col1) FROM myTable",
        "SELECT CHILD_EXPR_MIN(0,col2,col2,col1),"
            + "PARENT_EXPR_MIN(0,1,col1,col2) FROM myTable");

    testQueryRewrite("SELECT EXPR_MIN(col5,col1,col2), EXPR_MIN(col6,col1,col2), EXPR_MAX(col6,col1,col2) "
            + "FROM myTable",
        "SELECT CHILD_EXPR_MIN(0,col5,col5,col1,col2), "
            + "CHILD_EXPR_MIN(0,col6,col6,col1,col2), "
            + "CHILD_EXPR_MAX(0,col6,col6,col1,col2),"
            + "PARENT_EXPR_MIN(0,2,col1,col2,col6,col5),"
            + "PARENT_EXPR_MAX(0,2,col1,col2,col6) FROM myTable");
  }

  @Test
  public void testQueryRewriteWithOrderBy() {
    testQueryRewrite("SELECT EXPR_MIN(col5,col1,col2), EXPR_MIN(col6,col1,col3),"
            + "EXPR_MIN(col6,col3,col1) FROM myTable GROUP BY col3 "
            + "ORDER BY col3 DESC",
        "SELECT CHILD_EXPR_MIN(0,col5,col5,col1,col2), "
            + "CHILD_EXPR_MIN(1,col6,col6,col1,col3),"
            + "CHILD_EXPR_MIN(2,col6,col6,col3,col1),"
            + "PARENT_EXPR_MIN(1,2,col1,col3,col6),"
            + "PARENT_EXPR_MIN(0,2,col1,col2,col5),"
            + "PARENT_EXPR_MIN(2,2,col3,col1,col6)"
            + "FROM myTable GROUP BY col3 ORDER BY col3 DESC");

    testQueryRewrite("SELECT EXPR_MIN(col5,col1,col2), EXPR_MAX(col5,col1,col2) FROM myTable GROUP BY col3 "
            + "ORDER BY ADD(co1, co3) DESC",
        "SELECT CHILD_EXPR_MIN(0,col5,col5,col1,col2),"
            + "CHILD_EXPR_MAX(0,col5,col5,col1,col2),"
            + "PARENT_EXPR_MIN(0,2,col1,col2,col5), "
            + "PARENT_EXPR_MAX(0,2,col1,col2,col5) "
            + "FROM myTable GROUP BY col3 ORDER BY ADD(co1, co3) DESC");
  }

  private void testQueryRewrite(String original, String expected) {
    assertEquals(QUERY_REWRITER.rewrite(CalciteSqlParser.compileToPinotQuery(original)),
        CalciteSqlParser.compileToPinotQuery(expected));
  }
}
