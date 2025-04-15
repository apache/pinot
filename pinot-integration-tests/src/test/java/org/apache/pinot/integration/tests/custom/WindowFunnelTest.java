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
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.File;
import java.util.List;
import java.util.Random;
import org.apache.pinot.integration.tests.window.utils.WindowFunnelUtils;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class WindowFunnelTest extends CustomDataQueryClusterIntegrationTest {

  @Override
  protected long getCountStarResult() {
    return WindowFunnelUtils._countStarResult;
  }

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

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelEventsFunctionEvalGroupByQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
            + "userId, funnelEventsFunctionEval(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation', "
            + "3, timestampCol, userId, url"
            + ") "
            + "FROM %s GROUP BY userId ORDER BY userId LIMIT %d", getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      ArrayNode events = (ArrayNode) row.get(1);
      if (i < 10) {
        assertEquals(events.size(), 13);
        assertEquals(events.get(0).asText().split(",")[0], "1");
        assertEquals(events.get(0).asText().split(",")[1].trim(), "12");
        assertEquals(events.get(1).asInt(), 1000);
        assertEquals(events.get(2).asText(), "user0" + i);
        assertEquals(events.get(3).asText(), "/product/search");
        assertEquals(events.get(4).asInt(), 1010);
        assertEquals(events.get(5).asText(), "user0" + i);
        assertEquals(events.get(6).asText(), "/cart/add");
        assertEquals(events.get(7).asInt(), 1020);
        assertEquals(events.get(8).asText(), "user0" + i);
        assertEquals(events.get(9).asText(), "/checkout/start");
      } else {
        assertEquals(events.size(), 1);
        assertEquals(events.get(0).intValue(), 0);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelStepDurationStatsGroupByQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT userId, funnelStepDurationStats(timestampCol, '1000', 4, "
                + "url = '/product/search', "
                + "url = '/cart/add', "
                + "url = '/checkout/start', "
                + "url = '/checkout/confirmation', "
                + "'durationFunctions=count,avg,min,median,percentile95,max' "
                + ") as statsArray "
                + "FROM %s GROUP BY userId HAVING arrayLength(statsArray) > 0 ORDER BY userId LIMIT %d",
            getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 40);
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      ArrayNode statsArray = (ArrayNode) row.get(1);
      if (i < 10) {
        assertEquals(statsArray.size(), 24);
        assertEquals(statsArray.get(0).doubleValue(), 1.0);
        assertEquals(statsArray.get(1).doubleValue(), 10.0);
        assertEquals(statsArray.get(2).doubleValue(), 10.0);
        assertEquals(statsArray.get(3).doubleValue(), 10.0);
        assertEquals(statsArray.get(4).doubleValue(), 10.0);
        assertEquals(statsArray.get(5).doubleValue(), 10.0);
        assertEquals(statsArray.get(6).doubleValue(), 1.0);
        assertEquals(statsArray.get(7).doubleValue(), 10.0);
        assertEquals(statsArray.get(8).doubleValue(), 10.0);
        assertEquals(statsArray.get(9).doubleValue(), 10.0);
        assertEquals(statsArray.get(10).doubleValue(), 10.0);
        assertEquals(statsArray.get(11).doubleValue(), 10.0);
        assertEquals(statsArray.get(12).doubleValue(), 1.0);
        assertEquals(statsArray.get(13).doubleValue(), 10.0);
        assertEquals(statsArray.get(14).doubleValue(), 10.0);
        assertEquals(statsArray.get(15).doubleValue(), 10.0);
        assertEquals(statsArray.get(16).doubleValue(), 10.0);
        assertEquals(statsArray.get(17).doubleValue(), 10.0);
        assertEquals(statsArray.get(18).doubleValue(), 1.0);
        assertEquals(statsArray.get(19).doubleValue(), 0.0);
        assertEquals(statsArray.get(20).doubleValue(), 0.0);
        assertEquals(statsArray.get(21).doubleValue(), 0.0);
        assertEquals(statsArray.get(22).doubleValue(), 0.0);
        assertEquals(statsArray.get(23).doubleValue(), 0.0);
      } else if (i < 30) {
        assertEquals(statsArray.size(), 24);
        for (int j = 0; j < 24; j++) {
          if (j == 0 || j == 6 || j == 12) {
            assertEquals(statsArray.get(j).doubleValue(), 1.0);
          } else {
            assertEquals(statsArray.get(j).doubleValue(), 0.0);
          }
        }
      } else {
        assertEquals(statsArray.size(), 24);
        for (int j = 0; j < 24; j++) {
          if (j == 0) {
            assertEquals(statsArray.get(j).doubleValue(), 1.0);
          } else {
            assertEquals(statsArray.get(j).doubleValue(), 0.0);
          }
        }
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunnelStepDurationStatsGroupByQueries2(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT userId, funnelStepDurationStats(timestampCol, '1000', 4, "
                + "url = '/product/search', "
                + "url = '/cart/add', "
                + "url = '/checkout/start', "
                + "url = '/checkout/confirmation', "
                + "'durationFunctions=avg,min,median,percentile95,max' "
                + ") as statsArray "
                + "FROM %s GROUP BY userId HAVING arrayLength(statsArray) > 0 ORDER BY userId LIMIT %d",
            getTableName(), getCountStarResult());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).textValue(), "user" + (i / 10) + (i % 10));
      ArrayNode statsArray = (ArrayNode) row.get(1);
      assertEquals(statsArray.size(), 20);
      for (int j = 0; j < 15; j++) {
        assertEquals(statsArray.get(j).doubleValue(), 10.0);
      }
      for (int j = 15; j < 20; j++) {
        assertEquals(statsArray.get(j).doubleValue(), 0.0);
      }
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testFunnelStepDurationStatsGroupByQueries3(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("WITH durationStats AS (SELECT "
                + "/*+ aggOptions( is_partitioned_by_group_by_keys = 'true') */ "
                + "userId, funnelStepDurationStats(timestampCol, '1000', 4, "
                + "url = '/product/search', "
                + "url = '/cart/add', "
                + "url = '/checkout/start', "
                + "url = '/checkout/confirmation', "
                + "'durationFunctions=count,avg,median' "
                + ") as stats "
                + "FROM %s "
                + "GROUP BY userId) "
                + "SELECT "
                + "sum(arrayElementAtDouble(stats, 1)) AS count_step0, "
                + "AVG(arrayElementAtDouble(stats, 2)) AS avg_avg_step0_to_step1, "
                + "AVG(arrayElementAtDouble(stats, 3)) AS avg_median_step0_to_step1, "
                + "sum(arrayElementAtDouble(stats, 4)) AS count_step1, "
                + "AVG(arrayElementAtDouble(stats, 5)) AS avg_avg_step1_to_step2, "
                + "AVG(arrayElementAtDouble(stats, 6)) AS avg_median_step1_to_step2, "
                + "sum(arrayElementAtDouble(stats, 7)) AS count_step2, "
                + "AVG(arrayElementAtDouble(stats, 8)) AS avg_avg_step2_to_step3, "
                + "AVG(arrayElementAtDouble(stats, 9)) AS avg_median_step2_to_step3, "
                + "sum(arrayElementAtDouble(stats, 10)) AS count_step3 "
                + "FROM durationStats "
                + "OPTION(useMultistageEngine=true, numGroupsLimit=2000000, timeoutMs=1800000, "
                + "serverReturnFinalResult=true, numThreadsForFinalReduce=4)",
            getTableName());
    JsonNode jsonNode = postQuery(query);
    JsonNode rows = jsonNode.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 10);
    assertEquals(row.get(0).doubleValue(), 40.0);
    assertEquals(row.get(1).doubleValue(), 2.5);
    assertEquals(row.get(2).doubleValue(), 2.5);
    assertEquals(row.get(3).doubleValue(), 30.0);
    assertEquals(row.get(4).doubleValue(), 2.5);
    assertEquals(row.get(5).doubleValue(), 2.5);
    assertEquals(row.get(6).doubleValue(), 30.0);
    assertEquals(row.get(7).doubleValue(), 2.5);
    assertEquals(row.get(8).doubleValue(), 2.5);
    assertEquals(row.get(9).doubleValue(), 10.0);
  }

  @Override
  public String getTableName() {
    return WindowFunnelUtils.DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return WindowFunnelUtils.createSchema(getTableName());
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return WindowFunnelUtils.createAvroFiles(_tempDir);
  }
}
