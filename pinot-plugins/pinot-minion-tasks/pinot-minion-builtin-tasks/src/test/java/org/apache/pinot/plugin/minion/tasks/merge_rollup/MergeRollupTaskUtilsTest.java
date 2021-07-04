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
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.minion.Granularity;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MergeRollupTaskUtilsTest {
  private final String METRIC_COLUMN_A = "metricColA";
  private final String METRIC_COLUMN_B = "metricColB";
  private Map<String, String> _mergeRollupTaskConfig;

  @BeforeClass
  public void setUp() {
    Map<String, String> mergeRollupTaskConfig = new HashMap<>();
    mergeRollupTaskConfig.put("aggregate.metricColA", "sum");
    mergeRollupTaskConfig.put("aggregate.metricColB", "max");
    mergeRollupTaskConfig.put("merge.daily.mergeType", "concat");
    mergeRollupTaskConfig.put("merge.daily.bufferTime", "2d");
    mergeRollupTaskConfig.put("merge.daily.maxNumRecordsPerSegment", "1000000");
    mergeRollupTaskConfig.put("merge.daily.maxNumRecordsPerTask", "5000000");
    mergeRollupTaskConfig.put("merge.monthly.mergeType", "rollup");
    mergeRollupTaskConfig.put("merge.monthly.bufferTime", "30d");
    mergeRollupTaskConfig.put("merge.monthly.maxNumRecordsPerSegment", "2000000");
    mergeRollupTaskConfig.put("merge.monthly.maxNumRecordsPerTask", "5000000");
    _mergeRollupTaskConfig = mergeRollupTaskConfig;
  }

  @Test
  public void testGetRollupAggregationTypeMap() {
    Map<String, AggregationFunctionType> rollupAggregationTypeMap =
        MergeRollupTaskUtils.getRollupAggregationTypes(_mergeRollupTaskConfig);
    Assert.assertEquals(rollupAggregationTypeMap.size(), 2);
    Assert.assertTrue(rollupAggregationTypeMap.containsKey(METRIC_COLUMN_A));
    Assert.assertTrue(rollupAggregationTypeMap.containsKey(METRIC_COLUMN_B));
    Assert.assertEquals(rollupAggregationTypeMap.get(METRIC_COLUMN_A), AggregationFunctionType.SUM);
    Assert.assertEquals(rollupAggregationTypeMap.get(METRIC_COLUMN_B), AggregationFunctionType.MAX);
  }

  @Test
  public void testGetAllMergeProperties() {
    Map<Granularity, MergeProperties> allMergeProperties =
        MergeRollupTaskUtils.getAllMergeProperties(_mergeRollupTaskConfig);
    Assert.assertEquals(allMergeProperties.size(), 2);
    Assert.assertTrue(allMergeProperties.containsKey(Granularity.DAILY));
    Assert.assertTrue(allMergeProperties.containsKey(Granularity.MONTHLY));

    MergeProperties dailyProperty = allMergeProperties.get(Granularity.DAILY);
    Assert.assertEquals(dailyProperty.getMergeType(), MergeType.CONCAT.name());
    Assert.assertEquals(dailyProperty.getBufferTimeMs(), 172800000L);
    Assert.assertEquals(dailyProperty.getMaxNumRecordsPerSegment(), 1000000L);
    Assert.assertEquals(dailyProperty.getMaxNumRecordsPerTask(), 5000000L);

    MergeProperties monthlyProperty = allMergeProperties.get(Granularity.MONTHLY);
    Assert.assertEquals(monthlyProperty.getMergeType(), MergeType.ROLLUP.name());
    Assert.assertEquals(monthlyProperty.getBufferTimeMs(), 2592000000L);
    Assert.assertEquals(monthlyProperty.getMaxNumRecordsPerSegment(), 2000000L);
    Assert.assertEquals(monthlyProperty.getMaxNumRecordsPerTask(), 5000000L);
  }
}
