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
import org.apache.pinot.common.request.PinotQueryParserFactory;
import org.apache.pinot.parsers.AbstractCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MaxQuerySelectionLimitTest {

  @Test
  public void testPql() {
    AbstractCompiler pqlCompiler = PinotQueryParserFactory.get("PQL");
    testFixedQuerySetWithCompiler(pqlCompiler);
    testQueryWithCompiler(pqlCompiler, "select count(*) from vegetables group by a TOP 999", 1000, 10);
    testQueryWithCompiler(pqlCompiler, "select count(*) from vegetables group by a TOP 1000", 1000, 10);
    testQueryWithCompiler(pqlCompiler, "select count(*) from vegetables group by a TOP 1001", 1000, 10);
    testQueryWithCompiler(pqlCompiler, "select count(*) from vegetables group by a TOP 10000", 1000, 10);
  }

  @Test
  public void testCalciteSql() {
    AbstractCompiler sqlCompiler = PinotQueryParserFactory.get("SQL");
    testFixedQuerySetWithCompiler(sqlCompiler);
    testQueryWithCompiler(sqlCompiler, "select count(*) from vegetables group by a LIMIT 999", 1000, 999);
    testQueryWithCompiler(sqlCompiler, "select count(*) from vegetables group by a LIMIT 1000", 1000, 1000);
    testQueryWithCompiler(sqlCompiler, "select count(*) from vegetables group by a LIMIT 1001", 1000, 1001);
    testQueryWithCompiler(sqlCompiler, "select count(*) from vegetables group by a LIMIT 10000", 1000, 10000);
  }

  private void testFixedQuerySetWithCompiler(AbstractCompiler compiler) {
    testQueryWithCompiler(compiler, "select * from vegetables LIMIT 999", 1000, 999);
    testQueryWithCompiler(compiler, "select * from vegetables LIMIT 1000", 1000, 1000);
    testQueryWithCompiler(compiler, "select * from vegetables LIMIT 1001", 1000, 1000);
    testQueryWithCompiler(compiler, "select * from vegetables LIMIT 10000", 1000, 1000);
  }

  private void testQueryWithCompiler(AbstractCompiler compiler, String query, int maxQuerySelectionLimit,
      int expectedLimit) {
    BrokerRequest brokerRequest;
    brokerRequest = compiler.compileToBrokerRequest(query);
    BaseBrokerRequestHandler.handleMaxQuerySelectionLimit(brokerRequest, maxQuerySelectionLimit);
    Assert.assertEquals(brokerRequest.getLimit(), expectedLimit);
  }
}
