/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.pql.parsers;

import com.linkedin.pinot.common.request.FilterOperator;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.pql.parsers.pql2.ast.TopAstNode;


/**
 * Some tests for the PQL 2 compiler.
 */
public class Pql2CompilerTest {
  @Test
  public void testQuotedStrings() {
    Pql2Compiler compiler = new Pql2Compiler();

    // Two single quotes in a single quoted string
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = 'Martha''s Vineyard'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha's Vineyard");

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = 'Martha\"\"s Vineyard'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha\"\"s Vineyard");

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = \"Martha\"\"s Vineyard\"");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha\"s Vineyard");

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where origin = \"Martha''s Vineyard\"");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha''s Vineyard");
  }

  @Test
  public void testDuplicateClauses() {
    Pql2Compiler compiler = new Pql2Compiler();
    assertCompilationFails(compiler, "select top 5 count(*) from a top 8");
    assertCompilationFails(compiler, "select count(*) from a where a = 1 limit 5 where b = 2");
    assertCompilationFails(compiler, "select count(*) from a group by b limit 5 group by b");
    assertCompilationFails(compiler, "select count(*) from a having sum(a) = 8 limit 5 having sum(a) = 9");
    assertCompilationFails(compiler, "select count(*) from a order by b limit 5 order by c");
    assertCompilationFails(compiler, "select count(*) from a limit 5 limit 5");
  }

  @Test
  public void testTopZero() throws Exception {
    Pql2Compiler compiler  = new Pql2Compiler();
    testTopZeroFor(compiler, "select count(*) from someTable where c = 5 group by X top 0", TopAstNode.DEFAULT_TOP_N, false);
    testTopZeroFor(compiler, "select count(*) from someTable where c = 5 group by X top 1", 1, false);
    testTopZeroFor(compiler, "select count(*) from someTable where c = 5 group by X top -1", TopAstNode.DEFAULT_TOP_N, true);
  }

  private void assertCompilationFails(Pql2Compiler compiler, String query) {
    try {
      compiler.compileToBrokerRequest(query);
    } catch (Pql2CompilationException e) {
      // Expected
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  private void testTopZeroFor(Pql2Compiler compiler, String s, final int expectedTopN, boolean parseException) throws Exception {
    BrokerRequest req;
    try {
      req = compiler.compileToBrokerRequest(s);
    } catch (Pql2CompilationException e) {
      if (parseException) {
        return;
      }
      throw e;
    }
    Assert.assertTrue(req.isSetGroupBy());
    GroupBy groupBy = req.getGroupBy();
    Assert.assertTrue(groupBy.isSetTopN());
    Assert.assertEquals(expectedTopN, groupBy.getTopN());
  }

  @Test
  public void testRejectInvalidLexerToken() {
    assertCompilationFails(new Pql2Compiler(), "select foo from bar where baz ?= 2");
    assertCompilationFails(new Pql2Compiler(), "select foo from bar where baz =! 2");
  }

  @Test
  public void testRejectInvalidParses() {
    assertCompilationFails(new Pql2Compiler(), "select foo from bar where baz < > 2");
    assertCompilationFails(new Pql2Compiler(), "select foo from bar where baz ! = 2");
  }

  @Test
  public void testParseExceptionHasCharacterPosition() {
    Pql2Compiler compiler = new Pql2Compiler();
    final String query = "select foo from bar where baz ? 2";

    try {
      compiler.compileToBrokerRequest(query);
    } catch (Pql2CompilationException e) {
      // Expected
      Assert.assertTrue(e.getMessage().startsWith("1:30: "), "Compilation exception should contain line and character for error message. Error message is " + e.getMessage());
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  @Test
  public void testCStyleInequalityOperator() {
    Pql2Compiler compiler = new Pql2Compiler();

    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where name != 'Brussels sprouts'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getOperator(), FilterOperator.NOT);
  }

  @Test
  public void testCompilationWithHaving() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "select avg(age) as avg_age from person group by address_city having avg(age)=20");
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getOperator(), FilterOperator.EQUALITY);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getAggregationInfo().getAggregationType(), "avg");
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getAggregationInfo().getAggregationParams().get("column"),
        "age");
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getValue().get(0), "20");
    brokerRequest = compiler.compileToBrokerRequest(
        "select count(*) as count from sell group by price having count(*) > 100 AND count(*)<200");
    Assert.assertEquals(brokerRequest.getHavingFilterSubQueryMap().getFilterQueryMap().size(), 3);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getOperator(), FilterOperator.AND);
    brokerRequest = compiler.compileToBrokerRequest(
        "select count(*) as count, avg(price) as avgprice from sell having count(*) > 0 OR (avg(price) < 45 AND count(*) > 22)");
    Assert.assertEquals(brokerRequest.getHavingFilterSubQueryMap().getFilterQueryMap().size(), 5);

    brokerRequest = compiler.compileToBrokerRequest(
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having count(*) in (375,5005,1099)");
    Assert.assertEquals(brokerRequest.getHavingFilterSubQueryMap().getFilterQueryMap().size(), 1);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getOperator(), FilterOperator.IN);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getValue().size(), 1);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getValue().get(0).contains("375"), true);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getValue().get(0).contains("5005"), true);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getValue().get(0).contains("1099"), true);
    brokerRequest = compiler.compileToBrokerRequest(
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having count(*) not in (375,5005,1099)");
    Assert.assertEquals(brokerRequest.getHavingFilterSubQueryMap().getFilterQueryMap().size(), 1);
    Assert.assertEquals(brokerRequest.getHavingFilterQuery().getOperator(), FilterOperator.NOT_IN);
  }

  @Test
  public void testQueryOptions() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where name != 'Brussels sprouts'");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 0);
    Assert.assertNull(brokerRequest.getQueryOptions());

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where name != 'Brussels sprouts' OPTION (delicious=yes)");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 1);
    Assert.assertTrue(brokerRequest.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getQueryOptions().get("delicious"), "yes");

    brokerRequest = compiler.compileToBrokerRequest(
        "select * from vegetables where name != 'Brussels sprouts' OPTION (delicious=yes, foo=1234, bar='potato')");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 3);
    Assert.assertTrue(brokerRequest.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("bar"), "potato");
  }
}
