package com.linkedin.thirdeye.anomaly.lib.scanstatistics;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.lib.scanstatistics.OnlineNormalStatistics;

public class OnlineNormalStatisticsTest {

  @Test
  public void simpleArray() {
    double[] testArray = {1, 2};
    testCorrectness(testArray);
  }

  @Test
  public void bigArray() {
    double[] testArray = new double[1000];
    for (int i = 0; i < testArray.length; i++) {
      testArray[i] = Math.random();
    }
    testCorrectness(testArray);
  }

  private void testCorrectness(double[] arr) {
    OnlineNormalStatistics stat = new OnlineNormalStatistics(arr);
    DescriptiveStatistics ds = new DescriptiveStatistics(arr);
    Assert.assertEquals(stat.getMean(), ds.getMean(), 0.001);
    Assert.assertEquals(stat.getVariance(), ds.getVariance(), 0.001);
    Assert.assertEquals(stat.getPopulationVariance(), ds.getPopulationVariance(), 0.001);
  }

}