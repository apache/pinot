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
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryValidationTest {

  @Test
  public void testLargeLimit() {
    String query = "SELECT * FROM testTable LIMIT 10000";
    testUnsupportedQuery(query, "Value for 'LIMIT' (10000) exceeds maximum allowed value of 1000");
  }

  @Test
  public void testNonExistingColumns() {
    String query = "SELECT DISTINCT(col1, col2) FROM foo";
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "col1"), query,
        "Unknown columnName 'col2' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("col2", "col2"), query,
        "Unknown columnName 'col1' found in the query");
    testExistingColumns("foo", false, ImmutableMap.of("col2", "col2", "col1", "col1"), query);
    query = "SELECT sum(Col1) FROM foo";
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "col1"), query,
        "Unknown columnName 'Col1' found in the query");
    testExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "col1"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "Col1"), query);
    query = "SELECT sum(Col1) AS sum_col1 FROM foo";
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "col1"), query,
        "Unknown columnName 'Col1' found in the query");
    testExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "col1"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "Col1"), query);
    query = "SELECT sum(Col1) AS sum_col1 FROM foo HAVING sum_col1 > 10";
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "col1"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "cOL1"), query,
        "Unknown columnName 'Col1' found in the query");
    testExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "col1"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "Col1"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "cOL1"), query);
    query = "SELECT sum(Col1) AS sum_col1, b AS B, c as D FROM foo GROUP BY B, D";
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "col1", "b", "b", "c", "c"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "B", "B", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "C", "C"), query,
        "Unknown columnName 'c' found in the query");
    testExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "c", "c"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "col1", "b", "b", "c", "c"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "COL1", "b", "B", "c", "C"), query);
    query = "SELECT sum(Col1) AS sum_col1, b AS B, c as D FROM foo GROUP BY 2, 3";
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "col1", "B", "B", "c", "c", "D", "D"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("col1", "col1", "b", "b", "c", "c"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "B", "B", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "C", "C"), query,
        "Unknown columnName 'c' found in the query");
    testExistingColumns("foo", false, ImmutableMap.of("Col1", "Col1", "b", "b", "c", "c", "D", "D"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "col1", "b", "b", "c", "c", "d", "d"), query);
    testExistingColumns("foo", true, ImmutableMap.of("col1", "COL1", "b", "B", "c", "C"), query);
  }

  @Test
  public void testRejectGroovyQuery() {
    testRejectGroovyQuery(
        "SELECT groovy('{\"returnType\":\"INT\",\"isSingleValue\":true}', 'arg0 + arg1', colA, colB) FROM foo", true);
    testRejectGroovyQuery(
        "SELECT GROOVY('{\"returnType\":\"INT\",\"isSingleValue\":true}', 'arg0 + arg1', colA, colB) FROM foo", true);
    testRejectGroovyQuery(
        "SELECT groo_vy('{\"returnType\":\"INT\",\"isSingleValue\":true}', 'arg0 + arg1', colA, colB) FROM foo", true);
    testRejectGroovyQuery(
        "SELECT foo FROM bar WHERE GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', 'arg0 + arg1', colA,"
            + " colB) = 'foobarval'", true);
    testRejectGroovyQuery(
        "SELECT COUNT(colA) FROM bar GROUP BY GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', "
            + "'arg0 + arg1', colA, colB)", true);
    testRejectGroovyQuery(
        "SELECT foo FROM bar HAVING GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', 'arg0 + arg1', colA,"
            + " colB) = 'foobarval'", true);

    testRejectGroovyQuery("SELECT foo FROM bar", false);
  }

  @Test
  public void testReplicaGroupToQueryInvalidQuery() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("SET numReplicaGroupsToQuery='illegal'; SELECT COUNT(*) FROM MY_TABLE");
    Assert.assertThrows(IllegalArgumentException.class,
        () -> BaseSingleStageBrokerRequestHandler.validateRequest(pinotQuery, 10));
  }

  private void testRejectGroovyQuery(String query, boolean queryContainsGroovy) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);

    try {
      BaseSingleStageBrokerRequestHandler.rejectGroovyQuery(pinotQuery);
      if (queryContainsGroovy) {
        Assert.fail("Query should have failed since groovy was found in query: " + pinotQuery);
      }
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Groovy transform functions are disabled for queries");
    }
  }

  private void testUnsupportedQuery(String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.validateRequest(pinotQuery, 1000);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
  }

  private void testNonExistingColumns(String rawTableName, boolean isCaseInsensitive, Map<String, String> columnNameMap,
      String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.updateColumnNames(rawTableName, pinotQuery, isCaseInsensitive, columnNameMap);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  private void testExistingColumns(String rawTableName, boolean isCaseInsensitive, Map<String, String> columnNameMap,
      String query) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.updateColumnNames(rawTableName, pinotQuery, isCaseInsensitive, columnNameMap);
    } catch (Exception e) {
      Assert.fail("Query should have succeeded");
    }
  }
}
