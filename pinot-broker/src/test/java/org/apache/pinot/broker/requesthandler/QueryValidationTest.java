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

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.pinot.common.exception.InvalidColumnNameException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryValidationTest {
  private static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();

  @Test
  public void testLargeLimit() {
    String pql = "SELECT * FROM testTable LIMIT 10000";
    testUnsupportedPQLQuery(pql, "Value for 'LIMIT' (10000) exceeds maximum allowed value of 1000");

    pql = "SELECT COUNT(*) FROM testTable GROUP BY col1 LIMIT 10000";
    testUnsupportedPQLQuery(pql, "Value for 'LIMIT' (10000) exceeds maximum allowed value of 1000");

    pql = "SELECT COUNT(*) FROM testTable GROUP BY col1 TOP 10000";
    testUnsupportedPQLQuery(pql, "Value for 'TOP' (10000) exceeds maximum allowed value of 1000");

    String sql = "SELECT * FROM testTable LIMIT 10000 OPTION(groupByMode=sql,responseFormat=sql)";
    testUnsupportedSQLQuery(sql, "Value for 'LIMIT' (10000) exceeds maximum allowed value of 1000");
  }

  /**
   * The behavior of GROUP BY with multiple aggregations, is different in PQL vs SQL.
   * As a result, we have 2 groupByModes, to maintain backward compatibility.
   * The results of PQL groupByMode (if numAggregations > 1) cannot be returned in SQL responseFormat, as the results are non-tabular
   * Checking for this upfront, in validateRequest, to avoid executing the query and wasting resources
   *
   * Tests for this case as described above
   */
  @Test
  public void testUnsupportedGroupByQueries() {
    String pqlErrorMessage =
        "The results of a GROUP BY query with multiple aggregations in PQL is not tabular, and cannot be returned in SQL responseFormat";

    String pql =
        "SELECT MAX(column1), SUM(column2) FROM testTable GROUP BY column3 ORDER BY column3 option(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql =
        "SELECT MAX(column1), SUM(column2) FROM testTable GROUP BY column3 TOP 3 option(groupByMode=pql,responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql =
        "SELECT MAX(column1), SUM(column2) FROM testTable WHERE column5 = '100' GROUP BY column3 option(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql =
        "SELECT MAX(column1), SUM(column2), SUM(column10) FROM testTable GROUP BY column3 option(groupByMode=pql,responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql = "SELECT MAXMV(column1), SUMMV(column2) FROM testTable GROUP BY column3 option(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql =
        "SELECT MAXMV(column1), SUM(column2) FROM testTable GROUP BY column3 ORDER BY MAXMV(column1) option(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql =
        "SELECT PERCENTILE95(column1), DISTINCTCOUNTHLL(column2) FROM testTable GROUP BY column3 option(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    String sqlErrorMessage = "SQL query should always have response format and group-by mode set to SQL";

    String sql = "SELECT * FROM testTable";
    testUnsupportedSQLQuery(sql, sqlErrorMessage);

    sql = "SELECT * FROM testTable OPTION(groupByMode=sql)";
    testUnsupportedSQLQuery(sql, sqlErrorMessage);

    sql = "SELECT * FROM testTable OPTION(responseFormat=sql)";
    testUnsupportedSQLQuery(sql, sqlErrorMessage);
  }

  @Test
  public void testUnsupportedDistinctQueries() {
    String pql = "SELECT DISTINCT(col1, col2) FROM foo ORDER BY col3";
    testUnsupportedPQLQuery(pql, "ORDER By should be only on some/all of the columns passed as arguments to DISTINCT");

    pql = "SELECT DISTINCT(col1, col2) FROM foo GROUP BY col1";
    testUnsupportedPQLQuery(pql, "DISTINCT with GROUP BY is currently not supported");

    pql = "SELECT sum(col1), min(col2), DISTINCT(col3, col4) FROM foo";
    testUnsupportedPQLQuery(pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT sum(col1), DISTINCT(col2, col3), min(col4) FROM foo";
    testUnsupportedPQLQuery(pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT DISTINCT(col1, col2), DISTINCT(col3) FROM foo";
    testUnsupportedPQLQuery(pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT DISTINCT(col1, col2), sum(col3), min(col4) FROM foo";
    testUnsupportedPQLQuery(pql, "Aggregation functions cannot be used with DISTINCT");
  }

  private void testUnsupportedPQLQuery(String query, String errorMessage) {
    try {
      BrokerRequest brokerRequest = PQL_COMPILER.compileToBrokerRequest(query);
      BaseBrokerRequestHandler.validateRequest(brokerRequest, 1000);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  private void testUnsupportedSQLQuery(String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.validateRequest(pinotQuery, 1000, null);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  @Test
  public void testInvalidColumnNames() {
    BrokerMetrics brokerMetrics = new BrokerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    Set<String> columnNamesFromSchema = Sets.newHashSet("column1", "column2", "column3");

    String sql = "SELECT * FROM testTable LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), 0L);

    sql = "SELECT column1 FROM testTable LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), 0L);

    sql = "SELECT column1 FROM testTable WHERE column2 = '1' LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), 0L);

    sql =
        "SELECT SUM(column1), COUNT(column3) FROM testTable WHERE column2 = '1' group by column3 order by column1 LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), 0L);

    sql = "SELECT COUNT(*) FROM testTable";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), 0L);

    sql = "SELECT AVG(column2) FROM testTable WHERE column3 IN (column4) LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), 0L);

    long metricCount = 0L;
    // column4 doesn't exist in the schema
    sql = "SELECT column4 FROM testTable WHERE column2 = '1' order by column4 LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);

    // column4 doesn't exist in the schema
    sql =
        "SELECT SUM(column4), COUNT(column3) FROM testTable WHERE column2 = '1' group by column3 order by column1 LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);

    // column5 doesn't exist in the schema
    sql =
        "SELECT SUM(column1), COUNT(column3) FROM testTable WHERE column5 = '1' group by column3 order by column1 LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);

    // column6 doesn't exist in the schema
    sql =
        "SELECT SUM(column1), COUNT(column3) FROM testTable WHERE column2 = '1' group by column6 order by column1 LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);

    // column7 doesn't exist in the schema
    sql =
        "SELECT SUM(column1), COUNT(column3) FROM testTable WHERE column2 = '1' group by column3 order by column7 LIMIT 100";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);

    // column8 doesn't exist in the schema
    sql = "SELECT SUM(column8), column2 FROM testTable WHERE true GROUP BY column2 HAVING SUM(column8) > 10";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);

    // column9 and column10 don't exist in the schema
    sql = "SELECT column9 AS column1, column10 AS column2 FROM testTable";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);

    // column11 doesn't exist in the schema
    sql = "SELECT column2 FROM testTable WHERE column11 in (1, 2, 3)";
    Assert.assertEquals(getInvalidColumnNamesCount(sql, columnNamesFromSchema, brokerMetrics), ++metricCount);
  }

  private long getInvalidColumnNamesCount(String query, Set<String> columnNamesFromSchema,
      BrokerMetrics brokerMetrics) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query + "option(groupByMode=sql,responseFormat=sql)");
    try {
      BaseBrokerRequestHandler.validateRequest(pinotQuery, 1000, columnNamesFromSchema);
    } catch (InvalidColumnNameException e) {
      brokerMetrics.addMeteredTableValue("testTable", BrokerMeter.INVALID_COLUMN_NAMES_IN_QUERY, 1L);
    }
    return brokerMetrics.getMeteredTableValue("testTable", BrokerMeter.INVALID_COLUMN_NAMES_IN_QUERY).count();
  }
}
