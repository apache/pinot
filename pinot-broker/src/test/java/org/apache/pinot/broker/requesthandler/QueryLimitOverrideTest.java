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
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.requesthandler.PinotQueryParserFactory;
import org.apache.pinot.parsers.QueryCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryLimitOverrideTest {

  @Test
  public void testPql() {
    QueryCompiler pqlCompiler = PinotQueryParserFactory.get("PQL");
    testFixedQuerySetWithCompiler(pqlCompiler);
  }

  @Test
  public void testCalciteSql() {
    QueryCompiler sqlCompiler = PinotQueryParserFactory.get("SQL");
    testFixedQuerySetWithCompiler(sqlCompiler);
  }

  private void testFixedQuerySetWithCompiler(QueryCompiler compiler) {
    // Selections
    testSelectionQueryWithCompiler(compiler, "select * from vegetables LIMIT 999", 1000, 999);
    testSelectionQueryWithCompiler(compiler, "select * from vegetables LIMIT 1000", 1000, 1000);
    testSelectionQueryWithCompiler(compiler, "select * from vegetables LIMIT 1001", 1000, 1000);
    testSelectionQueryWithCompiler(compiler, "select * from vegetables LIMIT 10000", 1000, 1000);

    // GroupBys
    testGroupByQueryWithCompiler(compiler, "select count(*) from vegetables group by a LIMIT 999", 1000, 999);
    testGroupByQueryWithCompiler(compiler, "select count(*) from vegetables group by a LIMIT 1000", 1000, 1000);
    testGroupByQueryWithCompiler(compiler, "select count(*) from vegetables group by a LIMIT 1001", 1000, 1000);
    testGroupByQueryWithCompiler(compiler, "select count(*) from vegetables group by a LIMIT 10000", 1000, 1000);
  }

  private void testSelectionQueryWithCompiler(QueryCompiler compiler, String query, int maxQuerySelectionLimit,
      int expectedLimit) {
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    if (pinotQuery != null) {
      // SQL
      BaseBrokerRequestHandler.handleQueryLimitOverride(pinotQuery, maxQuerySelectionLimit);
      Assert.assertEquals(pinotQuery.getLimit(), expectedLimit);
    } else {
      // PQL
      BaseBrokerRequestHandler.handleQueryLimitOverride(brokerRequest, maxQuerySelectionLimit);
      Assert.assertEquals(brokerRequest.getLimit(), expectedLimit);
    }
  }

  private void testGroupByQueryWithCompiler(QueryCompiler compiler, String query, int maxQuerySelectionLimit,
      int expectedLimit) {
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    if (pinotQuery != null) {
      // SQL
      BaseBrokerRequestHandler.handleQueryLimitOverride(pinotQuery, maxQuerySelectionLimit);
      Assert.assertEquals(pinotQuery.getLimit(), expectedLimit);
    } else {
      // PQL
      BaseBrokerRequestHandler.handleQueryLimitOverride(brokerRequest, maxQuerySelectionLimit);
      Assert.assertEquals(brokerRequest.getGroupBy().getTopN(), expectedLimit);
    }
  }
}
