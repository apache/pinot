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


public class GroupByOnlyToOrderByRewriterTest {

  private static final QueryRewriter QUERY_REWRITER = new GroupByOnlyToOrderByRewriter();

  @Test
  public void testQuery1() {
    // SELECT A, max(B) FROM myTable GROUP BY A
    // -> SELECT A, max(B) FROM myTable GROUP BY A ORDER BY A
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT A, max(B) FROM myTable GROUP BY A");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "A");
  }

  @Test
  public void testQuery2() {
    // Negative test - ensure that non-aggregate queries aren't rewritten to add order by
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT A, B FROM myTable GROUP BY A, B");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 0);
  }

  @Test
  public void testQuery3() {
    // SELECT A, max(B), C FROM myTable GROUP BY A, C
    // -> SELECT A, max(B), C FROM myTable GROUP BY A, C ORDER BY A, C
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT A, max(B), C FROM myTable GROUP BY A, C");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "A");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C");
  }

  @Test
  public void testQuery4() {
    // SELECT concat(A, 'test'), avg(B) FROM myTable GROUP BY concat(A, 'test')
    // -> SELECT concat(A, 'test'), avg(B) FROM myTable GROUP BY concat(A, 'test') ORDER BY concat(A, 'test')
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT concat(A, 'test'), avg(B) FROM myTable GROUP BY concat(A, 'test')");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "concat");
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().size(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(0).getIdentifier().getName(), "A");
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(1).getLiteral().getStringValue(), "test");
  }

  @Test
  public void testQuery5() {
    // SELECT A, concat(B, '-', 'test'), count(C) FROM myTable GROUP BY A, concat(B, '-', 'test')
    // -> SELECT A, concat(B, '-', 'test'), count(C) FROM myTable GROUP BY A, concat(B, '-', 'test')
    //    ORDER BY A, concat(B, '-', 'test')
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT A, concat(B, '-', 'test'), count(C) FROM myTable GROUP BY A, concat(B, '-', 'test')");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "A");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "concat");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall()
            .getOperands().size(), 3);
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(0).getIdentifier().getName(), "B");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(1).getLiteral().getStringValue(), "-");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(2).getLiteral().getStringValue(), "test");
  }

  @Test
  public void testQuery6() {
    // SELECT A, count(B) FROM myTable GROUP BY A ORDER BY A, count(C)
    // Should not be modified
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT A, count(B) FROM myTable GROUP BY A ORDER BY A, count(C)");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "A");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "count");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().size(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(0).getIdentifier().getName(), "C");
  }

  @Test
  public void testQuery7() {
    // SELECT A, concat(B, '-', 'test'), count(C) FROM myTable GROUP BY A, concat(B, '-', 'test') ORDER BY count(C) DESC
    // Should not be modified
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT A, concat(B, '-', 'test'), count(C) FROM myTable GROUP BY A, concat(B, '-', 'test') ORDER BY "
            + "count(C) DESC");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "desc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "count");
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().size(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(0).getIdentifier().getName(), "C");
  }

  @Test
  public void testQuery8() {
    // SELECT max(A), min(B), C FROM testTable WHERE D = 1 GROUP BY C HAVING max(A) > 2
    // -> SELECT max(A), min(B), C FROM testTable WHERE D = 1 GROUP BY C HAVING max(A) > 2 ORDER BY C
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT max(A), min(B), C FROM testTable WHERE D = 1 GROUP BY C HAVING max(A) > 2");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C");
    Assert.assertNotNull(pinotQuery.getHavingExpression());
    Assert.assertEquals(pinotQuery.getHavingExpression().getFunctionCall().getOperator(), "GREATER_THAN");
    Assert.assertEquals(pinotQuery.getHavingExpression().getFunctionCall().getOperands().size(), 2);
  }

  @Test
  public void testQuery9() {
    // SELECT max(A), min(B), C FROM testTable WHERE D = 1 GROUP BY C HAVING max(A) > 2 ORDER BY max(A) DESC
    // Should not be modified
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT max(A), min(B), C "
        + "FROM testTable WHERE D = 1 GROUP BY C HAVING max(A) > 2 ORDER BY max(A) DESC");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "desc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "max");
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().size(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().get(0).getIdentifier().getName(), "A");
    Assert.assertNotNull(pinotQuery.getHavingExpression());
    Assert.assertEquals(pinotQuery.getHavingExpression().getFunctionCall().getOperator(), "GREATER_THAN");
    Assert.assertEquals(pinotQuery.getHavingExpression().getFunctionCall().getOperands().size(), 2);
  }

  @Test
  public void testQuery10() {
    // SELECT SUM('A'), MAX(B) FROM myTable GROUP BY 'A', B
    // -> SELECT SUM('A'), MAX(B) FROM myTable GROUP BY 'A', B ORDER BY 'A', B
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT SUM('A'), MAX(B) FROM myTable GROUP BY 'A', B");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().size(), 1);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getLiteral().getStringValue(), "A");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().size(), 1);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "B");
  }

  @Test
  public void testQuery11() {
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
    "SELECT SUM(volume) FROM callistoInteractions WHERE \"time\" >= 26194020 AND \"time\" < 26326560 AND pageUrn "
        + "IN (123) GROUP BY dateTimeConvert(\"time\",'1:MINUTES:EPOCH','1:MINUTES:EPOCH','1:MINUTES') LIMIT 100000");
    QUERY_REWRITER.rewrite(pinotQuery);
    Assert.assertNotNull(pinotQuery.getOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().size(), 1);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "datetimeconvert");
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall()
        .getOperands().size(), 4);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(0).getIdentifier().getName(), "time");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(1).getLiteral().getStringValue(), "1:MINUTES:EPOCH");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(2).getLiteral().getStringValue(), "1:MINUTES:EPOCH");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(3).getLiteral().getStringValue(), "1:MINUTES");
  }
}
