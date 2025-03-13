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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class WindowFunnelTest extends WindowFunnelTestBase {

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMaxStepQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "funnelMaxStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation' "
            + ") "
            + "FROM %s LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).intValue(), 4);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMaxStepGroupByQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation' "
            + ") "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 4);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMaxStepGroupByQueriesWithMode(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_order' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_deduplication' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 4);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_increase' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 4);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMaxStepGroupByQueriesWithModeKeepAll(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 3, "
            + "url = '/product/search', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_order', 'keep_all' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 3, "
            + "url = '/product/search', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_order' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMaxStepGroupByQueriesWithMaxStepDuration(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 3, "
            + "url = '/product/search', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'mode=strict_order, keep_all', "
            + "'maxStepDuration=10' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, funnelMaxStep(timestampCol, '1000', 3, "
            + "url = '/product/search', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'mode=strict_order', "
            + "'maxStepDuration=10' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMatchStepGroupByQueriesWithMode(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_order' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      int sumSteps = 0;
      for (JsonNode step : row.get(1)) {
        sumSteps += step.intValue();
      }
      switch (i / 10) {
        case 0:
          assertEquals(sumSteps, 3);
          break;
        case 1:
          assertEquals(sumSteps, 3);
          break;
        case 2:
          assertEquals(sumSteps, 2);
          break;
        case 3:
          assertEquals(sumSteps, 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_deduplication' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      int sumSteps = 0;
      for (JsonNode step : row.get(1)) {
        sumSteps += step.intValue();
      }
      switch (i / 10) {
        case 0:
          assertEquals(sumSteps, 4);
          break;
        case 1:
          assertEquals(sumSteps, 3);
          break;
        case 2:
          assertEquals(sumSteps, 2);
          break;
        case 3:
          assertEquals(sumSteps, 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_increase' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      int sumSteps = 0;
      for (JsonNode step : row.get(1)) {
        sumSteps += step.intValue();
      }
      switch (i / 10) {
        case 0:
          assertEquals(sumSteps, 4);
          break;
        case 1:
          assertEquals(sumSteps, 2);
          break;
        case 2:
          assertEquals(sumSteps, 3);
          break;
        case 3:
          assertEquals(sumSteps, 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine", invocationCount = 10, threadPoolSize = 5)
  public void testFunnelMatchStepWithMultiThreadsReduce(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int numThreadsExtractFinalResult = 2 + new Random().nextInt(10);
    LOGGER.info("Running testFunnelMatchStepWithMultiThreadsReduce with numThreadsExtractFinalResult: {}",
        numThreadsExtractFinalResult);
    String query =
        String.format("SET numThreadsExtractFinalResult=" + numThreadsExtractFinalResult + "; "
            + "SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_increase' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d ", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      int sumSteps = 0;
      for (JsonNode step : row.get(1)) {
        sumSteps += step.intValue();
      }
      switch (i / 10) {
        case 0:
          assertEquals(sumSteps, 4);
          break;
        case 1:
          assertEquals(sumSteps, 2);
          break;
        case 2:
          assertEquals(sumSteps, 3);
          break;
        case 3:
          assertEquals(sumSteps, 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMatchStepGroupByQueriesWithModeSkipLeaf(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_order' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      int sumSteps = 0;
      for (JsonNode step : row.get(1)) {
        sumSteps += step.intValue();
      }
      switch (i / 10) {
        case 0:
          assertEquals(sumSteps, 3);
          break;
        case 1:
          assertEquals(sumSteps, 3);
          break;
        case 2:
          assertEquals(sumSteps, 2);
          break;
        case 3:
          assertEquals(sumSteps, 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_deduplication' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      int sumSteps = 0;
      for (JsonNode step : row.get(1)) {
        sumSteps += step.intValue();
      }
      switch (i / 10) {
        case 0:
          assertEquals(sumSteps, 4);
          break;
        case 1:
          assertEquals(sumSteps, 3);
          break;
        case 2:
          assertEquals(sumSteps, 2);
          break;
        case 3:
          assertEquals(sumSteps, 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_increase' ) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      int sumSteps = 0;
      for (JsonNode step : row.get(1)) {
        sumSteps += step.intValue();
      }
      switch (i / 10) {
        case 0:
          assertEquals(sumSteps, 4);
          break;
        case 1:
          assertEquals(sumSteps, 2);
          break;
        case 2:
          assertEquals(sumSteps, 3);
          break;
        case 3:
          assertEquals(sumSteps, 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testFunnelMatchStepGroupByQueriesWithModeThenSumArray(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("WITH t1 AS (SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_order' ) as steps "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d) "
            + "SELECT sumArrayLong(steps) FROM t1", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).get(0).intValue(), 40);
    assertEquals(row.get(0).get(1).intValue(), 30);
    assertEquals(row.get(0).get(2).intValue(), 20);
    assertEquals(row.get(0).get(3).intValue(), 0);

    query =
        String.format("WITH t1 AS (SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_deduplication' ) as steps "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d) "
            + "SELECT sumArrayLong(steps) FROM t1", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).get(0).intValue(), 40);
    assertEquals(row.get(0).get(1).intValue(), 30);
    assertEquals(row.get(0).get(2).intValue(), 20);
    assertEquals(row.get(0).get(3).intValue(), 10);

    query =
        String.format("WITH t1 AS (SELECT "
            + "userId, funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_increase' ) as steps "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d) "
            + "SELECT sumArrayLong(steps) FROM t1", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).get(0).intValue(), 40);
    assertEquals(row.get(0).get(1).intValue(), 30);
    assertEquals(row.get(0).get(2).intValue(), 20);
    assertEquals(row.get(0).get(3).intValue(), 10);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelMatchStepGroupByQueriesWithModeAndSum(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, arraySumInt(funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_order' )) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, arraySumInt(funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_deduplication' )) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 4);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }

    query =
        String.format("SELECT "
            + "userId, arraySumInt(funnelMatchStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "'strict_increase' )) "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    jsonNode = postQuery(query);
    rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 4);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 2);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 3);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 1);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelCompleteCountGroupByQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, funnelCompleteCount(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation' "
            + ") "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 0);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 0);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 0);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testFunnelCompleteCountGroupByQueriesSkipLeaf(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */"
            + "userId, funnelCompleteCount(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation' "
            + ") "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < 40; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      switch (i / 10) {
        case 0:
          assertEquals(row.get(1).intValue(), 1);
          break;
        case 1:
          assertEquals(row.get(1).intValue(), 0);
          break;
        case 2:
          assertEquals(row.get(1).intValue(), 0);
          break;
        case 3:
          assertEquals(row.get(1).intValue(), 0);
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }
}
