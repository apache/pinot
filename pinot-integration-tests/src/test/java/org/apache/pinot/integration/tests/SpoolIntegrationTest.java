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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SpoolIntegrationTest extends BaseClusterIntegrationTest
    implements ExplainIntegrationTestTrait {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    String property = CommonConstants.MultiStageQueryRunner.KEY_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN;
    brokerConf.setProperty(property, "true");
  }

  @BeforeMethod
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  // Test that intermediate stages can be spooled.
  // In this case Stage 4 is an intermediate stage whose single child is stage 5.
  // Stage 4 is spooled and sends data to stages 3 and 7
  @Test
  public void intermediateSpool()
      throws Exception {
    JsonNode jsonNode = postQuery("SET useSpools = true;\n"
        + "WITH group_and_sum AS (\n"
        + "  SELECT ArrTimeBlk,\n"
        + "    Dest,\n"
        + "    SUM(ArrTime) AS ArrTime\n"
        + "  FROM mytable\n"
        + "  GROUP BY ArrTimeBlk,\n"
        + "    Dest\n"
        + "  limit 1000\n"
        + "),\n"
        + "aggregated_data AS (\n"
        + "  SELECT\n"
        + "    Dest,\n"
        + "    SUM(ArrTime) AS ArrTime\n"
        + "  FROM group_and_sum\n"
        + "  GROUP BY\n"
        + "    Dest\n"
        + "),\n"
        + "joined AS (\n"
        + "  SELECT\n"
        + "    s.Dest,\n"
        + "    s.ArrTime,\n"
        + "    (o.ArrTime) AS ArrTime2\n"
        + "  FROM group_and_sum s\n"
        + "  JOIN aggregated_data o\n"
        + "  ON s.Dest = o.Dest\n"
        + ")\n"
        + "SELECT *\n"
        + "FROM joined\n"
        + "LIMIT 1");
    JsonNode stats = jsonNode.get("stageStats");
    assertNoError(jsonNode);
    DocumentContext parsed = JsonPath.parse(stats.toString());

    checkSpoolTimes(parsed, 4, 3, 1);
    checkSpoolTimes(parsed, 4, 7, 1);
    checkSpoolSame(parsed, 4, 3, 7);
  }

  /**
   * Test a complex with nested spools.
   * <p>
   * Don't try to understand the query, just check that the spools are correct.
   * This query is an actual simplification of a query used in production.
   * It was the way we detected problems fixed in <a href="https://github.com/apache/pinot/pull/15135">#15135</a>.
   * <p>
   * This test was disabled after upgrading Calcite to 1.39.0 which introduces an an optimization that removes some
   * of the "nested spools" that this test expects. We should consider replacing the query in this test with a different
   * one that doesn't get optimized similarly. In the long term, we should allow providing hints that disable such
   * optimizations that would prevent spooling.
   */
  @Test(enabled = false)
  public void testNestedSpools()
      throws Exception {
    JsonNode jsonNode = postQuery("SET useSpools = true;\n"
        + "\n"
        + "WITH\n"
        + "    q1 AS (\n"
        + "        SELECT ArrTimeBlk as userUUID,\n"
        + "               Dest as deviceOS,\n"
        + "               SUM(ArrTime) AS totalTrips\n"
        + "        FROM mytable\n"
        + "        GROUP BY ArrTimeBlk, Dest\n"
        + "    ),\n"
        + "     q2 AS (\n"
        + "         SELECT userUUID,\n"
        + "                deviceOS,\n"
        + "                SUM(totalTrips) AS totalTrips,\n"
        + "                COUNT(DISTINCT userUUID) AS reach\n"
        + "         FROM q1\n"
        + "         GROUP BY userUUID,\n"
        + "                  deviceOS\n"
        + "     ),\n"
        + "     q3 AS (\n"
        + "         SELECT userUUID,\n"
        + "                (totalTrips / reach) AS frequency\n"
        + "         FROM q2\n"
        + "     ),\n"
        + "     q4 AS (\n"
        + "         SELECT rd.userUUID,\n"
        + "                rd.deviceOS,\n"
        + "                rd.totalTrips as totalTrips,\n"
        + "                rd.reach AS reach\n"
        + "         FROM q2 rd\n"
        + "     ),\n"
        + "     q5 AS (\n"
        + "         SELECT userUUID,\n"
        + "                SUM(totalTrips) AS totalTrips\n"
        + "         FROM q4\n"
        + "         GROUP BY userUUID\n"
        + "     ),\n"
        + "     q6 AS (\n"
        + "         SELECT s.userUUID,\n"
        + "                s.totalTrips,\n"
        + "                (s.totalTrips / o.frequency) AS reach,\n"
        + "                'some fake device' AS deviceOS\n"
        + "         FROM q5 s\n"
        + "                  JOIN q3 o ON s.userUUID = o.userUUID\n"
        + "     ),\n"
        + "     q7 AS (\n"
        + "         SELECT rd.userUUID,\n"
        + "                rd.totalTrips,\n"
        + "                rd.reach,\n"
        + "                rd.deviceOS\n"
        + "         FROM q4 rd\n"
        + "         UNION ALL\n"
        + "         SELECT f.userUUID,\n"
        + "                f.totalTrips,\n"
        + "                f.reach,\n"
        + "                f.deviceOS\n"
        + "         FROM q6 f\n"
        + "     ),\n"
        + "     q8 AS (\n"
        + "         SELECT sd.*\n"
        + "         FROM q7 sd\n"
        + "                  JOIN (\n"
        + "             SELECT deviceOS,\n"
        + "                    PERCENTILETDigest(totalTrips, 20) AS p20\n"
        + "             FROM q7\n"
        + "             GROUP BY deviceOS\n"
        + "         ) q ON sd.deviceOS = q.deviceOS\n"
        + "     )\n"
        + "SELECT *\n"
        + "FROM q8");
    JsonNode stats = jsonNode.get("stageStats");
    assertNoError(jsonNode);
    DocumentContext parsed = JsonPath.parse(stats.toString());

    /*
     * Stages are like follows:
     * 1 -> 2 (union)  ->  3 (aggr) ->  4 (leaf, spooled)
     *                 ->  5 (join) ->  6 (aggr, spooled) ->  7 (aggr, spooled) -> 4 (leaf, spooled)
     *                              ->  9 (aggr)          ->  4 (leaf, spooled)
     *   -> 11 (union) -> 12 (aggr) ->  4 (leaf, spooled)
     *                 -> 14 (join) ->  6 (aggr, spooled) ->  7 (aggr, spooled) -> 4 (leaf, spooled)
     *                              -> 18 (aggr)          ->  4 (leaf, spooled)
     */
    checkSpoolTimes(parsed, 6, 5, 1);
    checkSpoolTimes(parsed, 6, 14, 1);
    checkSpoolSame(parsed, 6, 5, 14);

    checkSpoolTimes(parsed, 7, 6, 2);

    checkSpoolTimes(parsed, 4, 3, 1);
    checkSpoolTimes(parsed, 4, 7, 2); // because there are 2 copies of 7 as well
    checkSpoolTimes(parsed, 4, 9, 1);
    checkSpoolTimes(parsed, 4, 12, 1);
    checkSpoolTimes(parsed, 4, 18, 1);
    checkSpoolSame(parsed, 4, 3, 7, 9, 12, 18);
  }

  /**
   * Returns the nodes that have the given descendant id and also have the given parent id as one of their ancestors.
   * @param parent the parent id
   * @param descendant the descendant id
   */
  private List<Map<String, Object>> findDescendantById(DocumentContext stats, int parent, int descendant) {
    @Language("jsonpath")
    String jsonPath = "$..[?(@.stage == " + parent + ")]..[?(@.stage == " + descendant + ")]";
    return stats.read(jsonPath);
  }

  private void checkSpoolTimes(DocumentContext stats, int spoolStageId, int parent, int times) {
    List<Map<String, Object>> descendants = findDescendantById(stats, parent, spoolStageId);
    Assert.assertEquals(descendants.size(), times, "Stage " + spoolStageId + " should be descended from stage "
        + parent + " exactly " + times + " times");
    Map<String, Object> firstSpool = descendants.get(0);
    for (int i = 1; i < descendants.size(); i++) {
      Assert.assertEquals(descendants.get(i), firstSpool, "Stage " + spoolStageId + " should be the same in "
          + "all " + times + " descendants");
    }
  }

  private void checkSpoolSame(DocumentContext stats, int spoolStageId, int... parents) {
    List<Pair<Integer, List<Map<String, Object>>>> spools = Arrays.stream(parents)
        .mapToObj(parent -> Pair.of(parent, findDescendantById(stats, parent, spoolStageId)))
        .collect(Collectors.toList());
    Pair<Integer, List<Map<String, Object>>> notEmpty = spools.stream()
        .filter(s -> !s.getValue().isEmpty())
        .findFirst()
        .orElse(null);
    if (notEmpty == null) {
      Assert.fail("None of the parent nodes " + Arrays.toString(parents) + " have a descendant with id "
          + spoolStageId);
    }
    List<Pair<Integer, List<Map<String, Object>>>> allNotEqual = spools.stream()
        .filter(s -> !s.getValue().get(0).equals(notEmpty.getValue().get(0)))
        .collect(Collectors.toList());
    if (!allNotEqual.isEmpty()) {
      Assert.fail("The descendant with id " + spoolStageId + " is not the same in all parent nodes "
          + spools);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
