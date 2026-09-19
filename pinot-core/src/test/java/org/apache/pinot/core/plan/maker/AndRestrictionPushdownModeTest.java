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
package org.apache.pinot.core.plan.maker;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests how [InstancePlanMakerImplV2] resolves [Server.AndRestrictionPushdownMode] into the per-query flag read by
/// the AND filter operators.
///
/// Under `AUTO` the push-down is skipped for a selection query without ORDER BY: it stops once it has LIMIT rows,
/// while the push-down materializes the whole filter result. See
/// [issue 19339](https://github.com/apache/pinot/issues/19339).
public class AndRestrictionPushdownModeTest {
  private static final String SELECTION_ONLY = "SELECT col1 FROM testTable WHERE col2 = 1";
  private static final String SELECTION_ORDER_BY = "SELECT col1 FROM testTable WHERE col2 = 1 ORDER BY col1";
  private static final String AGGREGATION = "SELECT SUM(col1) FROM testTable WHERE col2 = 1";
  private static final String GROUP_BY = "SELECT col1, SUM(col3) FROM testTable WHERE col2 = 1 GROUP BY col1";
  private static final String DISTINCT = "SELECT DISTINCT col1 FROM testTable WHERE col2 = 1";

  @Test
  public void testDefaultIsNever() {
    assertFalse(resolve(null, AGGREGATION), "The push-down ships disabled");
    assertFalse(resolve(null, SELECTION_ONLY));
  }

  @Test
  public void testAutoSkipsQueriesThatCanStopEarly() {
    assertFalse(resolve("auto", SELECTION_ONLY), "A selection-only query stops at its LIMIT");
    assertFalse(resolve("auto", DISTINCT), "DistinctOperator stops once it has LIMIT rows");
    // Whether an ORDER BY selection can stop early is decided per segment, so AUTO excludes it conservatively
    assertFalse(resolve("auto", SELECTION_ORDER_BY));
    assertFalse(resolve("auto", AGGREGATION + " LIMIT 0"), "LIMIT 0 reads nothing");
  }

  @Test
  public void testAutoAppliesToQueriesThatReadEveryMatchingDocument() {
    assertTrue(resolve("auto", AGGREGATION));
    assertTrue(resolve("auto", GROUP_BY));
  }

  @Test
  public void testAlwaysAppliesEvenToQueriesThatCanStopEarly() {
    assertTrue(resolve("always", SELECTION_ONLY));
    assertTrue(resolve("always", DISTINCT));
  }

  @Test
  public void testNeverSkipsEvenAggregationQuery() {
    assertFalse(resolve("never", AGGREGATION));
  }

  @Test
  public void testModeIsCaseInsensitive() {
    assertTrue(resolve("AlWaYs", SELECTION_ONLY));
    assertTrue(resolve("AuTo", AGGREGATION));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidQueryOptionValueIsRejected() {
    // A boolean is the most likely mistake, since the option name reads like a toggle
    resolve("true", AGGREGATION);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidServerConfigValueIsRejected() {
    planMakerWithServerDefault("sometimes");
  }

  @Test
  public void testServerDefaultAppliesWithoutQueryOption() {
    InstancePlanMakerImplV2 planMaker = planMakerWithServerDefault("always");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ONLY);
    planMaker.applyQueryOptions(queryContext);
    assertTrue(queryContext.isAndRestrictionPushdownEnabled(), "The server default must apply");
  }

  @Test
  public void testQueryOptionOverridesServerDefault() {
    InstancePlanMakerImplV2 planMaker = planMakerWithServerDefault("always");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(AGGREGATION);
    queryContext.getQueryOptions().put(QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE, "never");
    planMaker.applyQueryOptions(queryContext);
    assertFalse(queryContext.isAndRestrictionPushdownEnabled(), "The query option must win over the server default");
  }

  private static InstancePlanMakerImplV2 planMakerWithServerDefault(String mode) {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(new PinotConfiguration(Map.of(Server.AND_RESTRICTION_PUSHDOWN_MODE, mode)));
    return planMaker;
  }

  /// Resolves the flag for the given query, optionally setting the `andRestrictionPushdown` query option. With no
  /// option the server default (AUTO) applies.
  private static boolean resolve(@Nullable String mode, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    if (mode != null) {
      queryContext.getQueryOptions().put(QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE, mode);
    }
    new InstancePlanMakerImplV2().applyQueryOptions(queryContext);
    return queryContext.isAndRestrictionPushdownEnabled();
  }
}
