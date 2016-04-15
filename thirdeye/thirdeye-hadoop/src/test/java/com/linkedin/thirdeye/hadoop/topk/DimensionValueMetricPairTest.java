package com.linkedin.thirdeye.hadoop.topk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.MinMaxPriorityQueue;

public class DimensionValueMetricPairTest {

  @Test
  public void comparatorTest() throws Exception {

    MinMaxPriorityQueue<DimensionValueMetricPair> testQueue = MinMaxPriorityQueue.maximumSize(2).create();

    DimensionValueMetricPair d1 = new DimensionValueMetricPair("d1", 1);
    DimensionValueMetricPair d2 = new DimensionValueMetricPair("d2", 2);
    DimensionValueMetricPair d3 = new DimensionValueMetricPair("d3", 3);
    DimensionValueMetricPair d4 = new DimensionValueMetricPair("d4", 4);

    testQueue.add(d1);
    testQueue.add(d2);
    testQueue.add(d3);
    testQueue.add(d4);

    for (DimensionValueMetricPair pair : testQueue) {
      Assert.assertEquals(pair.getMetricValue().intValue() > 2, true,
          "Incorrect comparator for DimensionValueMetricPair, queue must retain hight metric values");
    }

  }
}
