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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryExceptionTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryExceptionDataProvider")
  public void testQueryWithException(String query, String exceptionSnippet) {
    try {
      _queryEnvironment.planQuery(query);
      Assert.fail("query plan should throw exception");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause().getMessage().contains(exceptionSnippet));
    }
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
