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
import com.google.common.collect.ImmutableList;
import java.io.File;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class WindowFunnelTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "WindowFunnelTest";
  private static final String URL_COLUMN = "url";
  private static final String TIMESTAMP_COLUMN = "timestampCol";
  private static final String USER_ID_COLUMN = "userId";
  private static long _countStarResult = 0;

  @Override
  protected long getCountStarResult() {
    return _countStarResult;
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

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(URL_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(USER_ID_COLUMN, FieldSpec.DataType.STRING)
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(URL_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(USER_ID_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null)
    ));

    long[][] userTimestampValues = new long[][]{
        new long[]{1000, 1010, 1020, 1025, 1030},
        new long[]{2010, 2010, 2000},
        new long[]{1000, 1010, 1015, 1020, 11030},
        new long[]{2020, 12010, 12050},
    };
    String[][] userUrlValues = new String[][]{
        new String[]{"/product/search", "/cart/add", "/checkout/start", "/cart/add", "/checkout/confirmation"},
        new String[]{"/checkout/start", "/cart/add", "/product/search"},
        new String[]{"/product/search", "/cart/add", "/cart/add", "/checkout/start", "/checkout/confirmation"},
        new String[]{"/checkout/start", "/cart/add", "/product/search"},
    };
    int repeats = 10;
    long totalRows = 0;
    for (String[] userUrlValue : userUrlValues) {
      totalRows += userUrlValue.length;
    }
    _countStarResult = totalRows * repeats;
    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int repeat = 0; repeat < repeats; repeat++) {
        for (int i = 0; i < userUrlValues.length; i++) {
          for (int j = 0; j < userUrlValues[i].length; j++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put(TIMESTAMP_COLUMN, userTimestampValues[i][j]);
            record.put(URL_COLUMN, userUrlValues[i][j]);
            record.put(USER_ID_COLUMN, "user" + i + repeat);
            fileWriter.append(record);
          }
        }
      }
    }
    return avroFile;
  }
}
