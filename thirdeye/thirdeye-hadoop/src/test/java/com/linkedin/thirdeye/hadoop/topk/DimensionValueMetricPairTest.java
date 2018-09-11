/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop.topk;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.MinMaxPriorityQueue;

public class DimensionValueMetricPairTest {

  @Test
  public void comparatorTest() throws Exception {

    MinMaxPriorityQueue<DimensionValueMetricPair> testQueue = MinMaxPriorityQueue.maximumSize(2).create();

    DimensionValueMetricPair d1 = new DimensionValueMetricPair("d1", 1);
    DimensionValueMetricPair d2 = new DimensionValueMetricPair("d2", 2);
    DimensionValueMetricPair d3 = new DimensionValueMetricPair(30, 3);
    DimensionValueMetricPair d4 = new DimensionValueMetricPair("d4", 4);

    testQueue.add(d1);
    testQueue.add(d2);
    testQueue.add(d3);
    testQueue.add(d4);

    for (DimensionValueMetricPair pair : testQueue) {
      Assert.assertEquals(pair.getMetricValue().intValue() > 2, true,
          "Incorrect comparator for DimensionValueMetricPair, queue must retain highest metric values");
    }

  }
}
