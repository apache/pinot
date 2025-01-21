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
package org.apache.pinot.plugin.minion.tasks.mergerollup;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeTask;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MergeRollupTaskUtilsTest {

  @Test
  public void testGetLevelToConfigMap() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put("daily.bucketTimePeriod", "1d");
    taskConfig.put("daily.bufferTimePeriod", "3d");
    taskConfig.put("daily.maxNumRecordsPerSegment", "1000000");
    taskConfig.put("daily.eraseDimensionValues", "a,b");
    taskConfig.put("monthly.bucketTimePeriod", "30d");
    taskConfig.put("monthly.bufferTimePeriod", "10d");
    taskConfig.put("monthly.roundBucketTimePeriod", "7d");
    taskConfig.put("monthly.mergeType", "rollup");
    taskConfig.put("monthly.eraseDimensionValues", "a,b,c,d");
    taskConfig.put("monthly.maxNumRecordsPerTask", "5000000");
    taskConfig.put("monthly.maxNumParallelBuckets", "5");

    Map<String, Map<String, String>> levelToConfigMap = MergeRollupTaskUtils.getLevelToConfigMap(taskConfig);
    assertEquals(levelToConfigMap.size(), 2);

    Map<String, String> dailyConfig = levelToConfigMap.get("daily");
    assertNotNull(dailyConfig);
    assertEquals(dailyConfig.size(), 4);
    assertEquals(dailyConfig.get(MergeTask.BUCKET_TIME_PERIOD_KEY), "1d");
    assertEquals(dailyConfig.get(MergeTask.BUFFER_TIME_PERIOD_KEY), "3d");
    assertEquals(dailyConfig.get(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY), "1000000");
    assertEquals(dailyConfig.get(MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY), "a,b");

    Map<String, String> monthlyConfig = levelToConfigMap.get("monthly");
    assertNotNull(monthlyConfig);
    assertEquals(monthlyConfig.size(), 7);
    assertEquals(monthlyConfig.get(MergeTask.BUCKET_TIME_PERIOD_KEY), "30d");
    assertEquals(monthlyConfig.get(MergeTask.BUFFER_TIME_PERIOD_KEY), "10d");
    assertEquals(monthlyConfig.get(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY), "7d");
    assertEquals(monthlyConfig.get(MergeTask.MERGE_TYPE_KEY), "rollup");
    assertEquals(monthlyConfig.get(MergeTask.MAX_NUM_RECORDS_PER_TASK_KEY), "5000000");
    assertEquals(monthlyConfig.get(MergeTask.MAX_NUM_PARALLEL_BUCKETS), "5");
    assertEquals(monthlyConfig.get(MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY), "a,b,c,d");
  }

  @Test
  public void testBuildMergeLevelKeyPrefix() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put("key", "value");
    String key1 = MergeRollupTaskUtils.buildMergeLevelKeyPrefix("key", taskConfig);
    assertEquals(key1, "key", "Expected key to remain unchanged.");

    taskConfig.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, "hourly");
    String key2 = MergeRollupTaskUtils.buildMergeLevelKeyPrefix("key", taskConfig);
    assertEquals(key2, "hourly.key", "Expected merge level prepended to key.");
  }

  @Test
  public void testEraseDimensionValuesAbsent() {
    Set<String> result1 = MergeRollupTaskUtils.getDimensionsToErase(null);
    assertTrue(result1.isEmpty(), "Expected empty set when 'taskConfig' is null");
    Set<String> result2 = MergeRollupTaskUtils.getDimensionsToErase(new HashMap<>());
    assertTrue(result2.isEmpty(), "Expected empty set when 'eraseDimensionValues' is absent");
  }

  @Test
  public void testEraseSingleDimensionValue() {
    Map<String, String> taskConfig = new HashMap<>();
    String mergeLevel = "daily";
    String key = mergeLevel + "." + MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY;
    taskConfig.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, mergeLevel);
    taskConfig.put(key, "dimension1");
    Set<String> result = MergeRollupTaskUtils.getDimensionsToErase(taskConfig);
    assertEquals(result.size(), 1, "Expected one dimension in the result set");
    assertTrue(result.contains("dimension1"), "Expected set to contain 'dimension1'");
  }

  @Test
  public void testEraseMultipleDimensionValues() {
    Map<String, String> taskConfig = new HashMap<>();
    String mergeLevel = "hourly";
    String key = mergeLevel + "." + MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY;
    taskConfig.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, mergeLevel);
    taskConfig.put(key, " dimension1 , dimension2 , dimension3 ");
    Set<String> result = MergeRollupTaskUtils.getDimensionsToErase(taskConfig);
    assertEquals(result.size(), 3, "Expected three dimensions in the result set with whitespace trimmed");
    assertTrue(result.contains("dimension1"), "Expected set to contain 'dimension1'");
    assertTrue(result.contains("dimension2"), "Expected set to contain 'dimension2'");
    assertTrue(result.contains("dimension3"), "Expected set to contain 'dimension3'");
  }

  @Test
  public void testGetAggregationFunctionParameters() {
    Map<String, String> taskConfig = new HashMap<>();
    String mergeLevel = "hourly";
    String prefix = mergeLevel + "." + MergeTask.AGGREGATION_FUNCTION_PARAMETERS_PREFIX;
    taskConfig.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, mergeLevel);
    taskConfig.put(prefix + "metricColumnA.param1", "value1");
    taskConfig.put(prefix + "metricColumnA.param2", "value2");
    taskConfig.put(prefix + "metricColumnB.param1", "value3");
    taskConfig.put("otherPrefix.metricColumnC.param1", "value1");
    taskConfig.put("aggregationFunction.metricColumnD.param2", "value2");
    Map<String, Map<String, String>> result = MergeRollupTaskUtils.getAggregationFunctionParameters(taskConfig);
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("metricColumnA"));
    assertTrue(result.containsKey("metricColumnB"));
    assertEquals(result.get("metricColumnA").get("param1"), "value1");
    assertEquals(result.get("metricColumnA").get("param2"), "value2");
    assertEquals(result.get("metricColumnB").get("param1"), "value3");
  }

  @Test
  public void testLevelToAggregationFunctionParameters() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put("hourly.aggregationFunctionParameters.metricColumnA.nominalEntries", "16384");
    taskConfig.put("hourly.aggregationFunctionParameters.metricColumnB.nominalEntries", "8192");
    taskConfig.put("daily.aggregationFunctionParameters.metricColumnA.nominalEntries", "8192");
    taskConfig.put("daily.aggregationFunctionParameters.metricColumnB.nominalEntries", "4096");

    Map<String, Map<String, String>> levelToConfigMap = MergeRollupTaskUtils.getLevelToConfigMap(taskConfig);
    assertEquals(levelToConfigMap.size(), 2);

    Map<String, String> hourlyConfig = levelToConfigMap.get("hourly");
    assertNotNull(hourlyConfig);
    assertEquals(hourlyConfig.size(), 2);
    assertEquals(hourlyConfig.get("aggregationFunctionParameters.metricColumnA.nominalEntries"), "16384");
    assertEquals(hourlyConfig.get("aggregationFunctionParameters.metricColumnB.nominalEntries"), "8192");

    Map<String, String> dailyConfig = levelToConfigMap.get("daily");
    assertNotNull(dailyConfig);
    assertEquals(dailyConfig.size(), 2);
    assertEquals(dailyConfig.get("aggregationFunctionParameters.metricColumnA.nominalEntries"), "8192");
    assertEquals(dailyConfig.get("aggregationFunctionParameters.metricColumnB.nominalEntries"), "4096");
  }
}
