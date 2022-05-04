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
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NonAggregationGroupByToDistinctQueryRewriterTest {

  private static final QueryRewriter QUERY_REWRITER = new NonAggregationGroupByToDistinctQueryRewriter();

  @Test
  public void testQuery1() {
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT A FROM myTable GROUP BY A");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinct");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "A");
  }

  @Test
  // SELECT col1+col2*5 FROM foo GROUP BY col1, col2 => SELECT distinct col1+col2*5 FROM foo
  public void testQuery2() {
    final PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("SELECT col1+col2*5 FROM foo GROUP BY col1, col2");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinct");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col2");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 5L);
  }

  @Test
  // SELECT col1, col2 FROM foo GROUP BY col1, col2 => SELECT distinct col1, col2 FROM foo
  public void testQuery3() {
    final PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("SELECT col1, col2 FROM foo GROUP BY col1, col2 ");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinct");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "col2");
  }

  @Test
  // SELECT col1 as col2 FROM foo GROUP BY col1 => SELECT distinct col1 as col2 FROM foo
  public void testQuery4() {
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT col1 as col2 FROM foo GROUP BY col1");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinct");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "col2");
  }

  @Test
  // SELECT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo GROUP BY a,b,c => SELECT DISTINCT col1 as a,
  // col2 as b, concat(col3, col4, '') as c FROM foo
  public void testQuery5() {
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo GROUP BY a,b,c");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinct");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "a");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col2");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "b");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "concat");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col3");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(1).getIdentifier().getName(), "col4");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(2).getLiteral().getStringValue(), "");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "c");
  }

  @Test
  // SELECT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo GROUP BY col1, col2, col3, col4 => SELECT
  // DISTINCT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo
  public void testQuery6() {
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT col1 as a, col2 as b, concat(col3, col4, '') as c FROM foo GROUP BY col1, col2, col3, col4");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinct");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "a");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col2");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "b");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "concat");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col3");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(1).getIdentifier().getName(), "col4");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(2).getLiteral().getStringValue(), "");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(2).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "c");
  }
}
