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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Runs compatible controller-listener, query-context, and spool scenarios on one shared offline cluster.
public class Set2QueryProfileIntegrationTest extends QueryThreadContextIntegrationTest {

  @Test
  public void testMaxSegmentCompletionTimeClusterConfigChange()
      throws Throwable {
    PinotLLCRealtimeSegmentManager segmentManager = _helixResourceManager.getRealtimeSegmentManager();
    String configKey = PinotLLCRealtimeSegmentManager.MAX_SEGMENT_COMPLETION_TIME_MILLIS_KEY;
    Throwable primaryFailure = null;
    try {
      assertEquals(segmentManager.getMaxSegmentCompletionTimeMillis(),
          PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS);

      updateClusterConfig(Map.of(configKey, "600000"));
      TestUtils.waitForCondition(aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis() == 600_000L,
          1_000L, 10_000L, "Max segment completion time was not updated to 600000ms");

      updateClusterConfig(Map.of(configKey, "900000"));
      TestUtils.waitForCondition(aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis() == 900_000L,
          1_000L, 10_000L, "Max segment completion time was not updated to 900000ms");
    } catch (Throwable t) {
      primaryFailure = t;
      throw t;
    } finally {
      try {
        deleteClusterConfig(configKey);
        TestUtils.waitForCondition(
            aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis()
                == PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS,
            1_000L, 10_000L, "Max segment completion time was not reverted to default after config deletion");
      } catch (Throwable cleanupFailure) {
        if (primaryFailure != null) {
          primaryFailure.addSuppressed(cleanupFailure);
        } else {
          throw cleanupFailure;
        }
      }
    }
  }

  @Test
  public void intermediateSpool()
      throws Exception {
    setUseMultiStageQueryEngine(true);
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
        .filter(spool -> !spool.getValue().isEmpty())
        .findFirst()
        .orElse(null);
    if (notEmpty == null) {
      Assert.fail("None of the parent nodes " + Arrays.toString(parents) + " have a descendant with id "
          + spoolStageId);
    }
    List<Pair<Integer, List<Map<String, Object>>>> allNotEqual = spools.stream()
        .filter(spool -> !spool.getValue().get(0).equals(notEmpty.getValue().get(0)))
        .collect(Collectors.toList());
    if (!allNotEqual.isEmpty()) {
      Assert.fail("The descendant with id " + spoolStageId + " is not the same in all parent nodes " + spools);
    }
  }
}
