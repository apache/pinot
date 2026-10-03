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


/// Tests for server-config versus query-option resolution in [InstancePlanMakerImplV2]: execution threads
/// (`max.execution.threads`, `default.execution.threads`) and the streaming selection ORDER BY merge defaults.
public class InstancePlanMakerImplV2Test {

  private static final String BASE_QUERY = "SELECT * FROM testTable";
  private static final String SELECTION_ORDER_BY_QUERY = "SELECT col FROM testTable ORDER BY col LIMIT 10";

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

  @Test
  public void testSortedSelectionMergeDefaultsWithoutServerConfig() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(new PinotConfiguration());

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ORDER_BY_QUERY);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getSortedSelectionMergeAutoMinSortedRatio(),
        Server.DEFAULT_SORTED_SELECTION_MERGE_AUTO_MIN_SORTED_RATIO);
    assertEquals(queryContext.getSortedSelectionMergeBlockSize(), Server.DEFAULT_SORTED_SELECTION_MERGE_BLOCK_SIZE);
  }

  @Test
  public void testSortedSelectionMergeServerConfigAppliesWithoutQueryOption() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(sortedSelectionMergeConfig(0.6, 2_500));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ORDER_BY_QUERY);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getSortedSelectionMergeAutoMinSortedRatio(), 0.6);
    assertEquals(queryContext.getSortedSelectionMergeBlockSize(), 2_500);
  }

  @Test
  public void testSortedSelectionMergeQueryOptionOverridesServerConfig() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(sortedSelectionMergeConfig(0.6, 2_500));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ORDER_BY_QUERY);
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    queryOptions.put(QueryOptionKey.SORTED_SELECTION_MERGE_AUTO_MIN_SORTED_RATIO, "0.9");
    queryOptions.put(QueryOptionKey.SORTED_SELECTION_MERGE_BLOCK_SIZE, "500");
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getSortedSelectionMergeAutoMinSortedRatio(), 0.9);
    assertEquals(queryContext.getSortedSelectionMergeBlockSize(), 500);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsSortedSelectionMergeRatioAboveOne() {
    new InstancePlanMakerImplV2().init(sortedSelectionMergeConfig(1.5, 2_500));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsNegativeSortedSelectionMergeRatio() {
    new InstancePlanMakerImplV2().init(sortedSelectionMergeConfig(-0.1, 2_500));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsNonPositiveSortedSelectionMergeBlockSize() {
    new InstancePlanMakerImplV2().init(sortedSelectionMergeConfig(0.6, 0));
  }

  private static PinotConfiguration sortedSelectionMergeConfig(double autoMinSortedRatio, int blockSize) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Server.SORTED_SELECTION_MERGE_AUTO_MIN_SORTED_RATIO, autoMinSortedRatio);
    config.setProperty(Server.SORTED_SELECTION_MERGE_BLOCK_SIZE, blockSize);
    return config;
  }
}
