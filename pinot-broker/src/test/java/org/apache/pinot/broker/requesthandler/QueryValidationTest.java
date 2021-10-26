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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
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
   * The results of PQL groupByMode (if numAggregations > 1) cannot be returned in SQL responseFormat, as the results
   * are non-tabular
   * Checking for this upfront, in validateRequest, to avoid executing the query and wasting resources
   *
   * Tests for this case as described above
   */
  @Test
  public void testUnsupportedGroupByQueries() {
    String pqlErrorMessage =
        "The results of a GROUP BY query with multiple aggregations in PQL is not tabular, and cannot be returned in "
            + "SQL responseFormat";

    String pql =
        "SELECT MAX(column1), SUM(column2) FROM testTable GROUP BY column3 ORDER BY column3 option(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql = "SELECT MAX(column1), SUM(column2) FROM testTable GROUP BY column3 TOP 3 option(groupByMode=pql,"
        + "responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql = "SELECT MAX(column1), SUM(column2) FROM testTable WHERE column5 = '100' GROUP BY column3 option"
        + "(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql = "SELECT MAX(column1), SUM(column2), SUM(column10) FROM testTable GROUP BY column3 option(groupByMode=pql,"
        + "responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql = "SELECT MAXMV(column1), SUMMV(column2) FROM testTable GROUP BY column3 option(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql = "SELECT MAXMV(column1), SUM(column2) FROM testTable GROUP BY column3 ORDER BY MAXMV(column1) option"
        + "(responseFormat=sql)";
    testUnsupportedPQLQuery(pql, pqlErrorMessage);

    pql = "SELECT PERCENTILE95(column1), DISTINCTCOUNTHLL(column2) FROM testTable GROUP BY column3 option"
        + "(responseFormat=sql)";
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

  @Test
  public void testUnsupportedNonExistColumnsQueries() {
    String sql = "SELECT DISTINCT(col1, col2) FROM foo OPTION(groupByMode=sql,responseFormat=sql)";
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "col1"), sql,
        "Unknown columnName 'col2' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col2", "col2"), sql,
        "Unknown columnName 'col1' found in the query");
    testExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col2", "col2", "col1", "col1"), sql);
    sql = "SELECT sum(Col1) FROM foo OPTION(groupByMode=sql,responseFormat=sql)";
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "col1"), sql,
        "Unknown columnName 'Col1' found in the query");
    testExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "col1"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "Col1"), sql);
    sql = "SELECT sum(Col1) AS sum_col1 FROM foo OPTION(groupByMode=sql,responseFormat=sql)";
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "col1"), sql,
        "Unknown columnName 'Col1' found in the query");
    testExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "col1"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "Col1"), sql);
    sql = "SELECT sum(Col1) AS sum_col1 FROM foo HAVING sum_col1 > 10 OPTION(groupByMode=sql,responseFormat=sql)";
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "col1"), sql,
        "Unknown columnName 'Col1' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "cOL1"), sql,
        "Unknown columnName 'Col1' found in the query");
    testExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "col1"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "Col1"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "cOL1"), sql);
    sql = "SELECT sum(Col1) AS sum_col1, b AS B, c as D FROM foo GROUP BY B, D OPTION(groupByMode=sql,"
        + "responseFormat=sql)";
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "col1", "b", "b", "c", "c"), sql,
        "Unknown columnName 'Col1' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "B", "B", "c", "c"), sql,
        "Unknown columnName 'b' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "c", "c"), sql,
        "Unknown columnName 'b' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "C", "C"), sql,
        "Unknown columnName 'c' found in the query");
    testExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "c", "c"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "col1", "b", "b", "c", "c"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "COL1", "b", "B", "c", "C"), sql);
    sql = "SELECT sum(Col1) AS sum_col1, b AS B, c as D FROM foo GROUP BY 2, 3 OPTION(groupByMode=sql,"
        + "responseFormat=sql)";
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "col1", "B", "B", "c", "c", "D", "D"), sql,
        "Unknown columnName 'Col1' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("col1", "col1", "b", "b", "c", "c"), sql,
        "Unknown columnName 'Col1' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "B", "B", "c", "c"), sql,
        "Unknown columnName 'b' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "c", "c"), sql,
        "Unknown columnName 'b' found in the query");
    testNonExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "C", "C"), sql,
        "Unknown columnName 'c' found in the query");
    testExistedColumnInSQLQuery("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "c", "c", "D", "D"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "col1", "b", "b", "c", "c", "d", "d"), sql);
    testExistedColumnInSQLQuery("foo", true, ImmutableMap.of("col1", "COL1", "b", "B", "c", "C"), sql);
  }

  private void testUnsupportedPQLQuery(String query, String errorMessage) {
    try {
      BrokerRequest brokerRequest = PQL_COMPILER.compileToBrokerRequest(query);
      BaseBrokerRequestHandler.validateRequest(brokerRequest, 1000);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
  }

  private void testUnsupportedSQLQuery(String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.validateRequest(pinotQuery, 1000);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
  }

  private void testNonExistedColumnInSQLQuery(String rawTableName, boolean isCaseInsensitive,
      Map<String, String> columnNameMap, String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.updateColumnNames(rawTableName, pinotQuery, isCaseInsensitive, columnNameMap);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  private void testExistedColumnInSQLQuery(String rawTableName, boolean isCaseInsensitive,
      Map<String, String> columnNameMap, String query) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseBrokerRequestHandler.updateColumnNames(rawTableName, pinotQuery, isCaseInsensitive, columnNameMap);
    } catch (Exception e) {
      Assert.fail("Query should have succeed");
    }
  }
}
