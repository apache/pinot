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
package org.apache.pinot.core.query.optimizer.statement;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;


class TestHelper {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();

  private TestHelper() {
    // Utility classes should not have a public or default constructor.
  }

  /**
   * Given two queries, this function will validate that the query obtained after rewriting the first query is the
   * same as the second query.
   */
  static void assertEqualsQuery(String queryOriginal, String queryAfterRewrite, TableConfig config,
      Schema schema) {
    BrokerRequest userBrokerRequest = SQL_COMPILER.compileToBrokerRequest(queryOriginal);
    PinotQuery userQuery = userBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(userQuery, config, schema);

    BrokerRequest rewrittenBrokerRequest = SQL_COMPILER.compileToBrokerRequest(queryAfterRewrite);
    PinotQuery rewrittenQuery = rewrittenBrokerRequest.getPinotQuery();
    OPTIMIZER.optimize(rewrittenQuery, config, schema);

    // Currently there is no way to specify Double.NEGATIVE_INFINITY in SQL, so in the test cases we specify string
    // '-Infinity' as
    // default null value, but change "stringValue:-Infinity" to "doubleValue:-Infinity" to adjust for internal rewrite.
    Assert.assertEquals(userQuery.toString(),
        rewrittenQuery.toString().replace("stringValue:-Infinity", "doubleValue:-Infinity"));
  }

}
