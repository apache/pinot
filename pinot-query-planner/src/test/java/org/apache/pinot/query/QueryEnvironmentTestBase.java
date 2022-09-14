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

import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;


public class QueryEnvironmentTestBase {
  protected QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    // the port doesn't matter as we are not actually making a server call.
    RoutingManager routingManager = QueryEnvironmentTestUtils.getMockRoutingManager(1, 2);
    _queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(QueryEnvironmentTestUtils.mockTableCache())),
        new WorkerManager("localhost", 3, routingManager));
  }

  @DataProvider(name = "testQueryDataProvider")
  protected Object[][] provideQueries() {
    return new Object[][] {
        new Object[]{"SELECT * FROM a ORDER BY col1 LIMIT 10"},
        new Object[]{"SELECT * FROM b ORDER BY col1, col2 DESC LIMIT 10"},
        new Object[]{"SELECT * FROM d"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0 AND a.col3 > b.col3"},
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        new Object[]{"SELECT a.col1, a.ts, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0"},
        new Object[]{"SELECT a.col1, a.col3 + a.ts FROM a WHERE a.col3 >= 0 AND a.col2 = 'a'"},
        new Object[]{"SELECT SUM(a.col3), COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a'"},
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"},
        new Object[]{"SELECT a.col1, COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"},
        new Object[]{"SELECT a.col2, a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col1 = 'a' "
            + " GROUP BY a.col1, a.col2"},
        new Object[]{"SELECT a.col1, AVG(b.col3) FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0 GROUP BY a.col1"},
        new Object[]{"SELECT a.col1, COUNT(*), SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1 "
            + "HAVING COUNT(*) > 10 AND MAX(a.col3) >= 0 AND MIN(a.col3) < 20 AND SUM(a.col3) <= 10 "
            + "AND AVG(a.col3) = 5"},
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10"},
        new Object[]{"SELECT dateTrunc('DAY', a.ts + b.ts) FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        new Object[]{"SELECT a.col2, a.col3 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE a.col3 >= 0 GROUP BY a.col2, a.col3"},
    };
  }
}
