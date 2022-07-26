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
package org.apache.pinot.query;

import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryCompilationTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryParserDataProvider")
  public void testQueryParser(String query, String digest)
      throws Exception {
    PlannerContext plannerContext = new PlannerContext();
    SqlNode sqlNode = _queryEnvironment.parse(query, plannerContext);
    _queryEnvironment.validate(sqlNode);
    Assert.assertEquals(sqlNode.toString(), digest);
  }

  @Test(dataProvider = "testQueryDataProvider")
  public void testQueryPlanWithoutException(String query)
      throws Exception {
    try {
      QueryPlan queryPlan = _queryEnvironment.planQuery(query);
      Assert.assertNotNull(queryPlan);
    } catch (RuntimeException e) {
      Assert.fail("failed to plan query: " + query, e);
    }
  }

  @Test(dataProvider = "testQueryExceptionDataProvider")
  public void testQueryWithException(String query, String exceptionSnippet) {
    try {
      _queryEnvironment.planQuery(query);
      Assert.fail("query plan should throw exception");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause().getMessage().contains(exceptionSnippet));
    }
  }

  @DataProvider(name = "testQueryParserDataProvider")
  private Object[][] provideQueriesAndDigest() {
    return new Object[][] {
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0",
            "SELECT *\n" + "FROM `a`\n" + "INNER JOIN `b` ON `a`.`col1` = `b`.`col2`\n" + "WHERE `a`.`col3` >= 0"},
    };
  }

  @DataProvider(name = "testQueryExceptionDataProvider")
  private Object[][] provideQueriesWithException() {
    return new Object[][] {
        // wrong table is being used after JOIN
        new Object[]{"SELECT b.col1 - a.col3 FROM a JOIN c ON a.col1 = c.col3", "Table 'b' not found"},
        // non-agg column not being grouped
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a", "'a.col1' is not being grouped"},
    };
  }
}
