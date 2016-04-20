/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAggregateMetricUtils;

public class ThirdeyeAggregateMetricUtilsTest {
  List<MetricType> metricTypes;
  Number[] aggMetricValues;
  Number[] metricValues;
  Number[] expectedValues;

  @BeforeTest
  public void setup() {
    metricTypes = new ArrayList<>();
    metricTypes.add(MetricType.INT);
    metricTypes.add(MetricType.FLOAT);
    metricTypes.add(MetricType.LONG);

    aggMetricValues = new Number[3];
    Arrays.fill(aggMetricValues, 0);
    metricValues = new Number[3];
    Arrays.fill(metricValues, 0);
    expectedValues = new Number[3];
    Arrays.fill(expectedValues, 0);

  }

  @Test
  public void testAggregateMetricUtils() throws Exception {

    ThirdeyeAggregateMetricUtils.aggregate(metricTypes, aggMetricValues, metricValues);
    expectedValues[0] = 0;
    expectedValues[1] = 0.0f;
    expectedValues[2] = 0L;
    Assert.assertEquals(expectedValues, aggMetricValues);


    metricValues[0] = 10;
    metricValues[1] = 2.5f;
    metricValues[2] = 111111111111L;
    ThirdeyeAggregateMetricUtils.aggregate(metricTypes, aggMetricValues, metricValues);
    expectedValues[0] = 10;
    expectedValues[1] = 2.5f;
    expectedValues[2] = 111111111111L;
    Assert.assertEquals(expectedValues, aggMetricValues);

    metricValues[0] = 10;
    metricValues[1] = 2.5f;
    metricValues[2] = 111111111111L;
    ThirdeyeAggregateMetricUtils.aggregate(metricTypes, aggMetricValues, metricValues);
    expectedValues[0] = 20;
    expectedValues[1] = 5.0f;
    expectedValues[2] = 222222222222L;
    Assert.assertEquals(expectedValues, aggMetricValues);




  }

}
