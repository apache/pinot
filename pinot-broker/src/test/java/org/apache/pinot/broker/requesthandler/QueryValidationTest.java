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

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryValidationTest {

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
    Pql2Compiler compiler = new Pql2Compiler();
    String errorMsg =
        "The results of a GROUP BY query with multiple aggregations in PQL is not tabular, and cannot be returned in SQL responseFormat";

    String query =
        "SELECT MAX(column1), SUM(column2) FROM testTable GROUP BY column3 ORDER BY column3 option(responseFormat=sql)";
    testUnsupportedQueriesHelper(compiler, query, errorMsg);

    query =
        "SELECT MAX(column1), SUM(column2) FROM testTable GROUP BY column3 TOP 3 option(groupByMode=pql,responseFormat=sql)";
    testUnsupportedQueriesHelper(compiler, query, errorMsg);

    query =
        "SELECT MAX(column1), SUM(column2) FROM testTable WHERE column5 = '100' GROUP BY column3 option(responseFormat=sql)";
    testUnsupportedQueriesHelper(compiler, query, errorMsg);

    query =
        "SELECT MAX(column1), SUM(column2), SUM(column10) FROM testTable GROUP BY column3 option(groupByMode=pql,responseFormat=sql)";
    testUnsupportedQueriesHelper(compiler, query, errorMsg);

    query = "SELECT MAXMV(column1), SUMMV(column2) FROM testTable GROUP BY column3 option(responseFormat=sql)";
    testUnsupportedQueriesHelper(compiler, query, errorMsg);

    query =
        "SELECT MAXMV(column1), SUM(column2) FROM testTable GROUP BY column3 ORDER BY MAXMV(column1) option(responseFormat=sql)";
    testUnsupportedQueriesHelper(compiler, query, errorMsg);

    query =
        "SELECT PERCENTILE95(column1), DISTINCTCOUNTHLL(column2) FROM testTable GROUP BY column3 option(responseFormat=sql)";
    testUnsupportedQueriesHelper(compiler, query, errorMsg);
  }

  @Test
  public void testUnsupportedDistinctQueries() {
    Pql2Compiler compiler = new Pql2Compiler();

    String pql = "SELECT DISTINCT(col1, col2) FROM foo ORDER BY col3";
    testUnsupportedQueriesHelper(compiler, pql,
        "ORDER By should be only on some/all of the columns passed as arguments to DISTINCT");

    pql = "SELECT DISTINCT(col1, col2) FROM foo GROUP BY col1";
    testUnsupportedQueriesHelper(compiler, pql, "DISTINCT with GROUP BY is currently not supported");

    pql = "SELECT sum(col1), min(col2), DISTINCT(col3, col4) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT sum(col1), DISTINCT(col2, col3), min(col4) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT DISTINCT(col1, col2), DISTINCT(col3) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");

    pql = "SELECT DISTINCT(col1, col2), sum(col3), min(col4) FROM foo";
    testUnsupportedQueriesHelper(compiler, pql, "Aggregation functions cannot be used with DISTINCT");
  }

  private void testUnsupportedQueriesHelper(Pql2Compiler compiler, String query, String errorMessage) {
    try {
      BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);
      BaseBrokerRequestHandler.validateRequest(brokerRequest, 1000);
      Assert.fail("query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }
}
