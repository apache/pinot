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
package org.apache.pinot.core.query.optimizer.filter;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PushDownNotFilterOptimizerTest {
  private static final PushDownNotFilterOptimizer OPTIMIZER = new PushDownNotFilterOptimizer();

  @Test
  public void testPushDownNotFilterOptimizer() {
    testPushDownNot("NOT(AirTime = 141 AND ArrDelay > 1)", "NOT(AirTime = 141) OR NOT(ArrDelay > 1)");
    testPushDownNot("NOT(AirTime = 141 AND ArrDelay > 1 OR ArrTime > 808)", "(NOT(AirTime = 141) OR NOT(ArrDelay > 1)) "
        + "AND NOT(ArrTime > 808)");
    testPushDownNot("NOT(AirTime = 141 OR ArrDelay > 1)", "NOT(AirTime = 141) AND NOT(ArrDelay > 1)");
    testPushDownNot("NOT(AirTime = 141 OR ArrDelay > 1 OR ArrTime > 808)", "NOT(AirTime = 141) AND NOT(ArrDelay > 1) "
        + "AND NOT(ArrTime > 808)");
  }

  @Test
  public void testNoOptimizerChange() {
    testPushDownNot("NOT(AirTime = 141)", "NOT(AirTime = 141)");
    testPushDownNot("AirTime = 141", "AirTime = 141");
    testPushDownNot("true", "true");
  }

  private void testPushDownNot(String filterString, String expectedOptimizedFilterString) {
    Expression expectedExpression = CalciteSqlParser.compileToExpression(expectedOptimizedFilterString);
    Expression optimizedFilterExpression = OPTIMIZER.optimize(CalciteSqlParser.compileToExpression(filterString));
    assertEquals(expectedExpression, optimizedFilterExpression);
  }
}
