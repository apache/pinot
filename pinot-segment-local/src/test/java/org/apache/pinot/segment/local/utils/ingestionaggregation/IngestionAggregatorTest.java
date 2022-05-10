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
package org.apache.pinot.segment.local.utils.ingestionaggregation;

import java.util.Arrays;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IngestionAggregatorTest {

  @Test
  public void testInvalidConditions() {
    // If AggregationConfigs isn't set, then metric name won't be changed and aggregator will be null.
    RealtimeSegmentConfig segmentConfig = new RealtimeSegmentConfig.Builder().build();
    IngestionAggregator ingestionAggregator = IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
    Assert.assertNull(ingestionAggregator.getAggregator("d1"));
    Assert.assertEquals(ingestionAggregator.getMetricName("d1"), "d1");

    // If AggregationConfigs is set to an empty list, then metric name won't be changed and aggregator will be null.
    segmentConfig = new RealtimeSegmentConfig.Builder().setIngestionAggregationConfigs(Arrays.asList()).build();
    ingestionAggregator = IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
    Assert.assertNull(ingestionAggregator.getAggregator("d1"));
    Assert.assertEquals(ingestionAggregator.getMetricName("d1"), "d1");
  }

  @Test
  public void testErrorConditions() {
    RealtimeSegmentConfig segmentConfig = new RealtimeSegmentConfig.Builder().setAggregateMetrics(true)
        .setIngestionAggregationConfigs(Arrays.asList(new AggregationConfig("d1", "SUM(s1)"))).build();
    try {
      IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
      Assert.fail("Should fail due to aggregateMetrics being true");
    } catch (IllegalStateException e) {
      // expected
    }

    segmentConfig = new RealtimeSegmentConfig.Builder().setIngestionAggregationConfigs(
        Arrays.asList(new AggregationConfig("d1", "s1"))).build();
    try {
      IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
      Assert.fail("Should fail due to the aggregationFunction not being a function");
    } catch (IllegalStateException e) {
      // expected
    }

    segmentConfig = new RealtimeSegmentConfig.Builder().setIngestionAggregationConfigs(
        Arrays.asList(new AggregationConfig("d1", "DISTINCTCOUNTHLL(s1)"))).build();
    try {
      IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
      Assert.fail("Should fail due to the aggregationFunction not being supported");
    } catch (IllegalStateException e) {
      // expected
    }

    segmentConfig = new RealtimeSegmentConfig.Builder().setIngestionAggregationConfigs(
        Arrays.asList(new AggregationConfig("d1", "s1 + s2"))).build();
    try {
      IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
      Assert.fail("Should fail due the function having multiple arguments");
    } catch (IllegalStateException e) {
      // expected
    }

    segmentConfig = new RealtimeSegmentConfig.Builder().setIngestionAggregationConfigs(
        Arrays.asList(new AggregationConfig("d1", "SUM(s1 + s2)"))).build();
    try {
      IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
      Assert.fail("Should fail due the function argument not being a column");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testValidConditions() {
    RealtimeSegmentConfig segmentConfig = new RealtimeSegmentConfig.Builder().setIngestionAggregationConfigs(
        Arrays.asList(new AggregationConfig("d1", "SUM(s1)"), new AggregationConfig("d2", "MIN(s2)"),
            new AggregationConfig("d3", "MAX(s2)"))).build();
    IngestionAggregator ingestionAggregator = IngestionAggregator.fromRealtimeSegmentConfig(segmentConfig);
    Assert.assertNotNull(ingestionAggregator.getAggregator("d1"));
    Assert.assertEquals(ingestionAggregator.getAggregator("d1").getAggregationType(), AggregationFunctionType.SUM);
    Assert.assertEquals(ingestionAggregator.getMetricName("d1"), "s1");
    Assert.assertEquals(ingestionAggregator.getAggregator("d2").getAggregationType(), AggregationFunctionType.MIN);
    Assert.assertEquals(ingestionAggregator.getMetricName("d2"), "s2");
    Assert.assertEquals(ingestionAggregator.getAggregator("d3").getAggregationType(), AggregationFunctionType.MAX);
    Assert.assertEquals(ingestionAggregator.getMetricName("d3"), "s2");
  }
}
