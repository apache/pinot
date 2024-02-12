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
package org.apache.pinot.query.queries;

import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StringToBytesPlansTest extends QueryEnvironmentTestBase {


  @Test
  public void explicitCastOnProjection() {
    //language=SQL
    assertEquivalentPlan(
        "select cast(a.col2 as BINARY) from a",
        "select hexToBytes(a.col2) from a"
    );
  }

  @Test
  public void explicitLiteralCastOnProjection() {
    //language=SQL
    assertEquivalentPlan(
        "select length(cast('80c062bf21bba70a9b404e969de0503750' as BINARY)) > 4 from a",
        "select length(hexToBytes('80c062bf21bba70a9b404e969de0503750')) > 4 from a"
    );
  }

  @Test
  public void explicitCastOnFilter() {
    //language=SQL
    assertEquivalentPlan(
        "select * from a where bytesToHex(cast(a.col2 as BINARY)) = 'noop'",
        "select * from a where bytesToHex(hexToBytes(a.col2)) = 'noop'"
    );
  }

  @Test
  public void explicitLiteralCastOnFilter() {
    //language=SQL
    assertEquivalentPlan(
        "select * from a where length(cast('80c062bf21bba70a9b404e969de0503750' as BINARY)) > 4",
        "select * from a where length(hexToBytes('80c062bf21bba70a9b404e969de0503750')) > 4"
    );
  }

  @Test
  public void explicitCastOnJoin() {
    //language=SQL
    assertEquivalentPlan(
        "select * from a join a as a2 on a.col2 = bytesToHex(cast(a.col2 as BINARY))",
        "select * from a join a as a2 on a.col2 = bytesToHex(hexToBytes(a.col2))"
    );
  }

  void assertEquivalentPlan(String rawQuery, String expected) {
    String explain = "explain plan for ";
    String expectedPlan = _queryEnvironment.explainQuery(explain + expected, RANDOM_REQUEST_ID_GEN.nextLong());
    String rawPlan = _queryEnvironment.explainQuery(explain + rawQuery, RANDOM_REQUEST_ID_GEN.nextLong());
    Assert.assertEquals(rawPlan, expectedPlan,
        String.format("Plan for %s doesn't match with the plan of the equivalent query %s",
            rawQuery, expected));
  }
}
