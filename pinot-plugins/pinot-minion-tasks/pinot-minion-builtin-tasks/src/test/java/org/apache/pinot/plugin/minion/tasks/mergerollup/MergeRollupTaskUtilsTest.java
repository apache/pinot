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
import org.apache.pinot.core.common.MinionConstants.MergeTask;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class MergeRollupTaskUtilsTest {

  @Test
  public void testGetLevelToConfigMap() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put("daily.bucketTimePeriod", "1d");
    taskConfig.put("daily.bufferTimePeriod", "3d");
    taskConfig.put("daily.maxNumRecordsPerSegment", "1000000");
    taskConfig.put("monthly.bucketTimePeriod", "30d");
    taskConfig.put("monthly.bufferTimePeriod", "10d");
    taskConfig.put("monthly.roundBucketTimePeriod", "7d");
    taskConfig.put("monthly.mergeType", "rollup");
    taskConfig.put("monthly.maxNumRecordsPerTask", "5000000");

    Map<String, Map<String, String>> levelToConfigMap = MergeRollupTaskUtils.getLevelToConfigMap(taskConfig);
    assertEquals(levelToConfigMap.size(), 2);

    Map<String, String> dailyConfig = levelToConfigMap.get("daily");
    assertNotNull(dailyConfig);
    assertEquals(dailyConfig.size(), 3);
    assertEquals(dailyConfig.get(MergeTask.BUCKET_TIME_PERIOD_KEY), "1d");
    assertEquals(dailyConfig.get(MergeTask.BUFFER_TIME_PERIOD_KEY), "3d");
    assertEquals(dailyConfig.get(MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY), "1000000");

    Map<String, String> monthlyConfig = levelToConfigMap.get("monthly");
    assertNotNull(monthlyConfig);
    assertEquals(monthlyConfig.size(), 5);
    assertEquals(monthlyConfig.get(MergeTask.BUCKET_TIME_PERIOD_KEY), "30d");
    assertEquals(monthlyConfig.get(MergeTask.BUFFER_TIME_PERIOD_KEY), "10d");
    assertEquals(monthlyConfig.get(MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY), "7d");
    assertEquals(monthlyConfig.get(MergeTask.MERGE_TYPE_KEY), "rollup");
    assertEquals(monthlyConfig.get(MergeTask.MAX_NUM_RECORDS_PER_TASK_KEY), "5000000");
  }
}
