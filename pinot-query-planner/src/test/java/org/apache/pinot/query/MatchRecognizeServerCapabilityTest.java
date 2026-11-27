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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.helix.InstanceCapability;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// A server that predates `MATCH_RECOGNIZE` cannot read its plan node. The planner rejects `MATCH_RECOGNIZE` with an
/// actionable error until every server advertises [InstanceCapability#MSE_MATCH_RECOGNIZE], wherever it builds the
/// dispatchable plan: for the execution and for an `EXPLAIN` that plans the implementation.
public class MatchRecognizeServerCapabilityTest {
  private static final String MATCH_QUERY = "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts "
      + "MEASURES MATCH_NUMBER() AS mno PATTERN (A B+) DEFINE B AS B.col3 > 10)";
  /// The validation finds a `MATCH_RECOGNIZE` that is not the outermost query too.
  private static final String NESTED_MATCH_QUERY = "SELECT COUNT(*) FROM (" + MATCH_QUERY + ") JOIN b ON TRUE";
  private static final String EXPECTED_ERROR = "does not advertise the instance capability 'MSE_MATCH_RECOGNIZE'";

  private final AtomicInteger _minServerRevision = new AtomicInteger();
  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(3, 1, 2, QueryEnvironmentTestBase.TABLE_SCHEMAS,
        QueryEnvironmentTestBase.SERVER1_SEGMENTS, QueryEnvironmentTestBase.SERVER2_SEGMENTS, null, false,
        this::stubCapabilityRevision);
  }

  @BeforeMethod
  public void resetRevision() {
    _minServerRevision.set(InstanceCapability.MSE_MATCH_RECOGNIZE.revision());
  }

  @Test
  public void testRejectedWhileAServerLacksTheCapability() {
    _minServerRevision.set(0);
    for (String query : new String[]{MATCH_QUERY, NESTED_MATCH_QUERY}) {
      assertRejected(() -> _queryEnvironment.planQuery(query));
      assertRejected(() -> _queryEnvironment.explainQuery("EXPLAIN PLAN FOR " + query, 0));
      assertRejected(() -> _queryEnvironment.explainQuery("EXPLAIN IMPLEMENTATION PLAN FOR " + query, 0));
      // A logical plan is not dispatched, so it does not depend on the servers.
      assertTrue(_queryEnvironment.explainQuery("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " + query, 0)
          .contains("LogicalMatch"));
    }
    // The capability only gates MATCH_RECOGNIZE.
    assertNotNull(_queryEnvironment.planQuery("SELECT col1, COUNT(*) FROM a GROUP BY col1"));
  }

  @Test
  public void testPlannedOnceEveryServerAdvertisesTheCapability() {
    assertNotNull(_queryEnvironment.planQuery(MATCH_QUERY));
    assertNotNull(_queryEnvironment.planQuery(NESTED_MATCH_QUERY));
    // A later revision supports everything that revision 1 does.
    _minServerRevision.set(InstanceCapability.MSE_MATCH_RECOGNIZE.revision() + 1);
    assertNotNull(_queryEnvironment.planQuery(MATCH_QUERY));
  }

  private RoutingManager stubCapabilityRevision(RoutingManager routingManager) {
    RoutingManager spy = spy(routingManager);
    doAnswer(invocation -> _minServerRevision.get()).when(spy)
        .getMinServerCapabilityRevision(InstanceCapability.MSE_MATCH_RECOGNIZE);
    return spy;
  }

  private static void assertRejected(Runnable planning) {
    QueryException exception = expectThrows(QueryException.class, planning::run);
    assertEquals(exception.getErrorCode(), QueryErrorCode.QUERY_PLANNING);
    assertTrue(exception.getMessage().contains(EXPECTED_ERROR), exception.getMessage());
    assertTrue(exception.getMessage().contains("Upgrade all servers"), exception.getMessage());
  }
}
