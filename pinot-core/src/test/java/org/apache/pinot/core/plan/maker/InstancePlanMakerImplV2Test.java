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
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for execution-thread resolution in {@link InstancePlanMakerImplV2}, covering the interplay
 * between {@code max.execution.threads}, {@code default.execution.threads}, and per-query overrides.
 */
public class InstancePlanMakerImplV2Test {

  private static final String BASE_QUERY = "SELECT * FROM testTable";

  private static QueryContext buildQueryContext(String maxExecutionThreadsOption) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(BASE_QUERY);
    if (maxExecutionThreadsOption != null) {
      Map<String, String> queryOptions = queryContext.getQueryOptions();
      queryOptions.put(QueryOptionKey.MAX_EXECUTION_THREADS, maxExecutionThreadsOption);
    }
    return queryContext;
  }

  @Test
  public void testDefaultNotSetFallsBackToMax() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(12);

    QueryContext queryContext = buildQueryContext(null);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 12);
  }

  @Test
  public void testDefaultExecutionThreadsUsedWhenSet() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(16);
    planMaker.setDefaultExecutionThreads(4);

    QueryContext queryContext = buildQueryContext(null);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 4);
  }

  @Test
  public void testQueryOverrideOverridesDefault() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(16);
    planMaker.setDefaultExecutionThreads(4);

    QueryContext queryContext = buildQueryContext("10");
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 10);
  }

  @Test
  public void testQueryOverrideCappedByMax() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(8);
    planMaker.setDefaultExecutionThreads(4);

    QueryContext queryContext = buildQueryContext("20");
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 8);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsDefaultExceedingMax() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Server.MAX_EXECUTION_THREADS, 4);
    config.setProperty(Server.DEFAULT_EXECUTION_THREADS, 8);
    planMaker.init(config);
  }
}
