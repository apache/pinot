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
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryParserTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryParserDataProvider")
  public void testQueryParser(String query, String digest)
      throws Exception {
    PlannerContext plannerContext = new PlannerContext();
    SqlNode sqlNode = _queryEnvironment.parse(query, plannerContext);
    _queryEnvironment.validate(sqlNode);
    Assert.assertEquals(sqlNode.toString(), digest);
  }

  @DataProvider(name = "testQueryParserDataProvider")
  private Object[][] provideQueriesAndDigest() {
    return new Object[][] {
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0",
            "SELECT *\n" + "FROM `a`\n" + "INNER JOIN `b` ON `a`.`col1` = `b`.`col2`\n" + "WHERE `a`.`col3` >= 0"},
    };
  }
}
