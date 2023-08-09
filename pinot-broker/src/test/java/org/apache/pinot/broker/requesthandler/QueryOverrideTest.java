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
package org.apache.pinot.broker.requesthandler;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class QueryOverrideTest {
  private static final int QUERY_LIMIT = 1000;

  @Test
  public void testLimitOverride() {
    // Selections
    testLimitOverride("SELECT * FROM vegetables LIMIT 999", 999);
    testLimitOverride("select * from vegetables limit 1000", 1000);
    testLimitOverride("SeLeCt * FrOm vegetables LiMit 1001", 1000);
    testLimitOverride("sElEcT * fRoM vegetables lImIt 10000", 1000);

    // Group-bys
    testLimitOverride("SELECT COUNT(*) FROM vegetables GROUP BY a LIMIT 999", 999);
    testLimitOverride("select count(*) from vegetables group by a limit 1000", 1000);
    testLimitOverride("SeLeCt CoUnT(*) FrOm vegetables GrOuP By a LiMit 1001", 1000);
    testLimitOverride("sElEcT cOuNt(*) fRoM vegetables gRoUp bY a lImIt 10000", 1000);
  }

  private void testLimitOverride(String query, int expectedLimit) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.handleQueryLimitOverride(pinotQuery, QUERY_LIMIT);
    assertEquals(pinotQuery.getLimit(), expectedLimit);
  }

  @Test
  public void testDistinctCountOverride() {
    String query = "SELECT DISTINCT_COUNT(col1) FROM myTable";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.handleSegmentPartitionedDistinctCountOverride(pinotQuery, ImmutableSet.of("col2", "col3"));
    assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcount");
    BaseBrokerRequestHandler.handleSegmentPartitionedDistinctCountOverride(pinotQuery,
        ImmutableSet.of("col1", "col2", "col3"));
    assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "segmentpartitioneddistinctcount");
  }

  @Test
  public void testDistinctMultiValuedOverride() {
    String query = "SELECT DISTINCT_COUNT(col1) FROM myTable";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    BaseBrokerRequestHandler.handleDistinctMultiValuedOverride(pinotQuery, ImmutableSet.of("col2", "col3"));
    assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcount");
    BaseBrokerRequestHandler.handleDistinctMultiValuedOverride(pinotQuery,
        ImmutableSet.of("col1", "col2", "col3"));
    assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcountmv");
  }

  @Test
  public void testApproximateFunctionOverride() {
    {
      String query = "SELECT DISTINCT_COUNT(col1) FROM myTable GROUP BY col2 HAVING DISTINCT_COUNT(col1) > 10 "
          + "ORDER BY DISTINCT_COUNT(col1) DESC";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcountsmarthll");
      assertEquals(
          pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
          "distinctcountsmarthll");
      assertEquals(
          pinotQuery.getHavingExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
          "distinctcountsmarthll");

      query = "SELECT DISTINCT_COUNT_MV(col1) FROM myTable";
      pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcountsmarthll");

      query = "SELECT DISTINCT col1 FROM myTable";
      pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinct");

      query = "SELECT DISTINCT_COUNT_HLL(col1) FROM myTable";
      pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcounthll");

      query = "SELECT DISTINCT_COUNT_BITMAP(col1) FROM myTable";
      pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcountbitmap");
    }

    for (String query : Arrays.asList("SELECT PERCENTILE(col1, 95) FROM myTable",
        "SELECT PERCENTILE_MV(col1, 95) FROM myTable", "SELECT PERCENTILE95(col1) FROM myTable",
        "SELECT PERCENTILE95MV(col1) FROM myTable")) {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "percentilesmarttdigest");
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1),
          RequestUtils.getLiteralExpression(95));
    }
    {
      String query = "SELECT PERCENTILE_TDIGEST(col1, 95) FROM myTable";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "percentiletdigest");

      query = "SELECT PERCENTILE_EST(col1, 95) FROM myTable";
      pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.handleApproximateFunctionOverride(pinotQuery);
      assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "percentileest");
    }
  }
}
