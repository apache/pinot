/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.reduce;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.core.query.reduce.BetweenComparison;
import com.linkedin.pinot.core.query.reduce.EqualComparison;
import com.linkedin.pinot.core.query.reduce.GreaterEqualComparison;
import com.linkedin.pinot.core.query.reduce.GreaterThanComparison;
import com.linkedin.pinot.core.query.reduce.HavingClauseComparisonTree;
import com.linkedin.pinot.core.query.reduce.InAndNotInComparison;
import com.linkedin.pinot.core.query.reduce.LessEqualComparison;
import com.linkedin.pinot.core.query.reduce.LessThanComparison;
import com.linkedin.pinot.core.query.reduce.NotEqualComparison;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HavingClauseComparisonTests {
  @Test
  public void testBetweenComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT avg(DepDelay) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having avg(DepDelay) BETWEEN 100 AND 200");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof BetweenComparison, true);
    BetweenComparison betweenComparison = (BetweenComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(betweenComparison.getFunctionExpression(), "avg_DepDelay");
    Assert.assertEquals(betweenComparison.getLeftValue(), 100.0);
    Assert.assertEquals(betweenComparison.getRightValue(), 200.0);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("avg_DepDelay", 99.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("avg_DepDelay", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("avg_DepDelay", 200);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("avg_DepDelay", 150);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("avg_DepDelay", 201);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
  }

  @Test
  public void testEqualComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having count(*) = 200");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof EqualComparison, true);
    EqualComparison equalComparison = (EqualComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(equalComparison.getFunctionExpression(), "count_star");
    Assert.assertEquals(equalComparison.getRightValue(), 200.0);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("count_star", 99.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 200);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 150);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 201);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
  }

  @Test
  public void testGreaterEqualComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT min(ArrivalDelay) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having min(ArrivalDelay) >= 500");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof GreaterEqualComparison, true);
    GreaterEqualComparison greaterEqualComparison =
        (GreaterEqualComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(greaterEqualComparison.getFunctionExpression(), "min_ArrivalDelay");
    Assert.assertEquals(greaterEqualComparison.getRightValue(), 500.0);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("min_ArrivalDelay", 499.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("min_ArrivalDelay", 500);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("min_ArrivalDelay", 500.01);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("min_ArrivalDelay", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("min_ArrivalDelay", -2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
  }

  @Test
  public void testGreaterComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT min(ArrivalDelay) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having min(ArrivalDelay) > 500");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof GreaterThanComparison, true);
    GreaterThanComparison greaterThanComparison =
        (GreaterThanComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(greaterThanComparison.getFunctionExpression(), "min_ArrivalDelay");
    Assert.assertEquals(greaterThanComparison.getRightValue(), 500.0);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("min_ArrivalDelay", 499.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("min_ArrivalDelay", 500);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("min_ArrivalDelay", 500.01);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("min_ArrivalDelay", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("min_ArrivalDelay", -2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
  }

  @Test
  public void testLessEqualComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT max(ArrivalDelay) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having max(ArrivalDelay) <= 500");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof LessEqualComparison, true);
    LessEqualComparison lessEqualComparison = (LessEqualComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(lessEqualComparison.getFunctionExpression(), "max_ArrivalDelay");
    Assert.assertEquals(lessEqualComparison.getRightValue(), 500.0);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("max_ArrivalDelay", 499.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", 500);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", 500.01);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("max_ArrivalDelay", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", -2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
  }

  @Test
  public void testLessLessThanComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT max(ArrivalDelay) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having max(ArrivalDelay) < 500");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof LessThanComparison, true);
    LessThanComparison lessThanComparison = (LessThanComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(lessThanComparison.getFunctionExpression(), "max_ArrivalDelay");
    Assert.assertEquals(lessThanComparison.getRightValue(), 500.0);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("max_ArrivalDelay", 499.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", 500);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("max_ArrivalDelay", 500.01);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("max_ArrivalDelay", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", -2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
  }

  @Test
  public void testNotEqualComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT max(ArrivalDelay) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having max(ArrivalDelay) <> 500");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof NotEqualComparison, true);
    NotEqualComparison notEqualComparison = (NotEqualComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(notEqualComparison.getFunctionExpression(), "max_ArrivalDelay");
    Assert.assertEquals(notEqualComparison.getRightValue(), 500.0);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("max_ArrivalDelay", 499.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", 500);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("max_ArrivalDelay", 500.01);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("max_ArrivalDelay", -2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
  }

  @Test
  public void testInAndNotINComparison() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having count(*) in (1000,2000,3000,4000,5000)");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof InAndNotInComparison, true);
    InAndNotInComparison inAndNotInComparison =
        (InAndNotInComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(inAndNotInComparison.getFunctionExpression(), "count_star");
    for (double predicateValue : inAndNotInComparison.getValues()) {
      Assert.assertEquals(
          predicateValue == 1000.0 || predicateValue == 2000.0 || predicateValue == 3000.0 || predicateValue == 4000.0
              || predicateValue == 5000.0, true);
    }
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("count_star", 999.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 500);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", -2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 6000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 1000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 3000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 4000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 5000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    //Test Not in using the same query
    brokerRequest = compiler.compileToBrokerRequest(
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having count(*) not in (1000,2000,3000,4000,5000)");
    havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), null);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction() instanceof InAndNotInComparison, true);
    inAndNotInComparison = (InAndNotInComparison) havingClauseComparisonTree.getComparisonFunction();
    Assert.assertEquals(inAndNotInComparison.getFunctionExpression(), "count_star");
    for (double predicateValue : inAndNotInComparison.getValues()) {
      Assert.assertEquals(
          predicateValue == 1000.0 || predicateValue == 2000.0 || predicateValue == 3000.0 || predicateValue == 4000.0
              || predicateValue == 5000.0, true);
    }
    singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("count_star", 999.99);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 500);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 100);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", -2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 6000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("count_star", 1000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 2000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 3000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 4000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("count_star", 5000);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
  }

  @Test
  public void testComplexHavingPredicate() {
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(
        "SELECT avg(DepDelay) FROM mytable WHERE DaysSinceEpoch >= 16312 group by Carrier having (avg(DepDelay) BETWEEN 100 AND 200 "
            + "AND count(*) >= 100) or (max(ArrivalDelay) <= 500 AND min(ArrivalDelay) > 200)");
    HavingClauseComparisonTree havingClauseComparisonTree =
        HavingClauseComparisonTree.buildHavingClauseComparisonTree(brokerRequest.getHavingFilterQuery(),
            brokerRequest.getHavingFilterSubQueryMap());
    Assert.assertEquals(havingClauseComparisonTree.getFilterOperator(), FilterOperator.OR);
    Assert.assertEquals(havingClauseComparisonTree.getComparisonFunction(), null);
    List<HavingClauseComparisonTree> subComparisonTree = havingClauseComparisonTree.getSubComparisonTree();
    Assert.assertEquals(subComparisonTree.get(0).getFilterOperator(), FilterOperator.AND);
    Assert.assertEquals(subComparisonTree.get(0).getComparisonFunction(), null);
    Assert.assertEquals(subComparisonTree.get(1).getFilterOperator(), FilterOperator.AND);
    Assert.assertEquals(subComparisonTree.get(1).getComparisonFunction(), null);
    List<HavingClauseComparisonTree> subSubComparisonTree = subComparisonTree.get(0).getSubComparisonTree();
    Assert.assertEquals(subSubComparisonTree.get(0).getFilterOperator(), null);
    Assert.assertEquals(subSubComparisonTree.get(0).getComparisonFunction() instanceof BetweenComparison, true);
    Assert.assertEquals(subSubComparisonTree.get(1).getFilterOperator(), null);
    Assert.assertEquals(subSubComparisonTree.get(1).getComparisonFunction() instanceof GreaterEqualComparison, true);
    subSubComparisonTree = subComparisonTree.get(1).getSubComparisonTree();
    Assert.assertEquals(subSubComparisonTree.get(0).getFilterOperator(), null);
    Assert.assertEquals(subSubComparisonTree.get(0).getComparisonFunction() instanceof LessEqualComparison, true);
    Assert.assertEquals(subSubComparisonTree.get(1).getFilterOperator(), null);
    Assert.assertEquals(subSubComparisonTree.get(1).getComparisonFunction() instanceof GreaterThanComparison, true);
    Map<String, Comparable> singleGroupAggResults = new TreeMap<String, Comparable>(String.CASE_INSENSITIVE_ORDER);
    singleGroupAggResults.put("avg_DepDelay", 200);
    singleGroupAggResults.put("count_star", 100);
    singleGroupAggResults.put("min_ArrivalDelay", 300);
    singleGroupAggResults.put("max_ArrivalDelay", 450);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("avg_DepDelay", 201);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), true);
    singleGroupAggResults.put("avg_DepDelay", 201);
    singleGroupAggResults.put("min_ArrivalDelay", 199);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
    singleGroupAggResults.put("avg_DepDelay", 200);
    singleGroupAggResults.put("count_star", 99);
    singleGroupAggResults.put("min_ArrivalDelay", 300);
    singleGroupAggResults.put("max_ArrivalDelay", 501);
    Assert.assertEquals(havingClauseComparisonTree.isThisGroupPassPredicates(singleGroupAggResults), false);
  }
}
