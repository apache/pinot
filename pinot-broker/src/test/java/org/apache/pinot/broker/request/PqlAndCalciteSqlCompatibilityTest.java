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
package org.apache.pinot.broker.request;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.parsers.utils.BrokerRequestComparisonUtils;
import org.apache.pinot.pql.parsers.PinotQuery2BrokerRequestConverter;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Some tests for the SQL compiler.
 * Please note that this test will load test resources: `pql_queries.list` and `pql_queries.list` under `pinot-common` module.
 */
public class PqlAndCalciteSqlCompatibilityTest {
  private static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();

  // OPTIMIZER is used to flatten certain queries with filtering optimization.
  // The reason is that SQL parser will parse the structure into a binary tree mode.
  // PQL parser will flat the case of multiple children under AND/OR.
  // After optimization, both BrokerRequests from PQL and SQL should look the same and be easier to compare.
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();

  @Test
  public void testSinglePqlAndSqlCompatible() {
    String query =
        "SELECT CarrierDelay, Origin, DayOfWeek FROM mytable WHERE ActualElapsedTime BETWEEN 163 AND 322 OR CarrierDelay IN (17, 266) OR AirlineID IN (19690, 20366) ORDER BY TaxiIn, TailNum LIMIT 1";
    testPqlSqlCompatibilityHelper(query, query);
  }

  @Test
  public void testSinglePqlAndSqlGroupByOrderByCompatible() {
    String query =
        "select group_city, sum(rsvp_count), count(*) from meetupRsvp group by group_city order by sum(rsvp_count), group_city DESC limit 100";
    testPqlSqlCompatibilityHelper(query, query);
  }

  @Test
  public void testPqlAndSqlCompatible()
      throws Exception {
    try (BufferedReader pqlReader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(
        PqlAndCalciteSqlCompatibilityTest.class.getClassLoader().getResourceAsStream("pql_queries.list"))));
        BufferedReader sqlReader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(
            PqlAndCalciteSqlCompatibilityTest.class.getClassLoader().getResourceAsStream("sql_queries.list"))))) {
      String pql;
      while ((pql = pqlReader.readLine()) != null) {
        String sql = sqlReader.readLine();
        testPqlSqlCompatibilityHelper(pql, sql);
      }
    }
  }

  private void testPqlSqlCompatibilityHelper(String pql, String sql) {
    BrokerRequest pqlBrokerRequest = PQL_COMPILER.compileToBrokerRequest(pql);
    OPTIMIZER.optimize(pqlBrokerRequest, null);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest sqlBrokerRequest = converter.convert(CalciteSqlParser.compileToPinotQuery(sql));
    OPTIMIZER.optimize(sqlBrokerRequest, null);
    Assert.assertTrue(BrokerRequestComparisonUtils.validate(pqlBrokerRequest, sqlBrokerRequest));
  }

  @Test
  public void testPqlSqlOrderByCompatibility() {
    String pql = "SELECT count(*) FROM Foo GROUP BY VALUEIN(mv_col, 10790, 16344) TOP 10";
    String sql =
        "SELECT VALUEIN(mv_col, 10790, 16344), count(*) FROM Foo GROUP BY VALUEIN(mv_col, 10790, 16344) ORDER BY count(*) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "count(*)");

    pql = "SELECT sum(col1) FROM Foo GROUP BY TIMECONVERT(timeCol, 'MILLISECONDS', 'SECONDS') TOP 10";
    sql =
        "SELECT TIMECONVERT(timeCol, 'MILLISECONDS', 'SECONDS'), sum(col1) FROM Foo GROUP BY TIMECONVERT(timeCol, 'MILLISECONDS', 'SECONDS') ORDER BY sum(col1) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "sum(col1)");

    pql =
        "SELECT max(add(col1,col2)) FROM Foo GROUP BY DATETIMECONVERT(timeCol, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES') TOP 10";
    sql =
        "SELECT DATETIMECONVERT(timeCol, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES'), max(add(col1,col2)) FROM Foo GROUP BY DATETIMECONVERT(timeCol, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES') ORDER BY max(add(col1,col2)) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "max(add(col1,col2))");

    pql = "SELECT max(div(col1,col2)) FROM Foo GROUP BY TIMECONVERT(timeCol, 'MILLISECONDS', 'SECONDS')";
    sql =
        "SELECT TIMECONVERT(timeCol, 'MILLISECONDS', 'SECONDS'), max(div(col1,col2)) FROM Foo GROUP BY TIMECONVERT(timeCol, 'MILLISECONDS', 'SECONDS') ORDER BY max(div(col1,col2)) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "max(div(col1,col2))");

    pql = "SELECT count(*) FROM Foo GROUP BY VALUEIN(time, 10790, 16344) TOP 10";
    sql =
        "SELECT VALUEIN(\"time\", 10790, 16344), count(*) FROM Foo GROUP BY VALUEIN(\"time\", 10790, 16344) ORDER BY count(*) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "count(*)");

    pql = "SELECT count(*) FROM Foo GROUP BY TIMECONVERT(time, 'MILLISECONDS', 'SECONDS') TOP 10";
    sql =
        "SELECT TIMECONVERT(\"time\", 'MILLISECONDS', 'SECONDS'), count(*) FROM Foo GROUP BY TIMECONVERT(\"time\", 'MILLISECONDS', 'SECONDS') ORDER BY count(*) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "count(*)");

    pql =
        "SELECT count(*) FROM Foo GROUP BY DATETIMECONVERT(time, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES') TOP 10";
    sql =
        "SELECT DATETIMECONVERT(\"time\", '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES'), count(*) FROM Foo GROUP BY DATETIMECONVERT(\"time\", '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES') ORDER BY count(*) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "count(*)");

    pql = "SELECT max(div(col1,col2)) FROM Foo GROUP BY TIMECONVERT(time, 'MILLISECONDS', 'SECONDS') TOP 10";
    sql =
        "SELECT TIMECONVERT(\"time\", 'MILLISECONDS', 'SECONDS'), max(div(col1,col2)) FROM Foo GROUP BY TIMECONVERT(\"time\", 'MILLISECONDS', 'SECONDS') ORDER BY max(div(col1,col2)) LIMIT 10";
    testPqlSqlOrderByCompatibilityHelper(pql, sql, "max(div(col1,col2))");
  }

  private void testPqlSqlOrderByCompatibilityHelper(String pql, String sql, String orderByColumn) {
    BrokerRequest pqlBrokerRequest = PQL_COMPILER.compileToBrokerRequest(pql);
    OPTIMIZER.optimize(pqlBrokerRequest, null);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest sqlBrokerRequest = converter.convert(CalciteSqlParser.compileToPinotQuery(sql));
    OPTIMIZER.optimize(sqlBrokerRequest, null);
    Assert.assertTrue(BrokerRequestComparisonUtils.validate(pqlBrokerRequest, sqlBrokerRequest, /*ignoreOrderBy*/true));

    // Validate ORDER BY here since brokerRequest from PQL will have order by as NULL and brokerRequest from SQL will
    // have valid ORDER BY
    List<SelectionSort> orderBy = sqlBrokerRequest.getOrderBy();
    Assert.assertEquals(orderBy.size(), 1);
    SelectionSort sort = orderBy.get(0);
    Assert.assertEquals(sort.getColumn(), orderByColumn);
  }
}
