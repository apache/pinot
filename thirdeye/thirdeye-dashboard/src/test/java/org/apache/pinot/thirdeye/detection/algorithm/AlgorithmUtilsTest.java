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

package org.apache.pinot.thirdeye.detection.algorithm;

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import java.io.InputStreamReader;
import java.io.Reader;
import org.joda.time.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AlgorithmUtilsTest {
  DataFrame data;
  DataFrame outlierData;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("timeseries-4w.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(DataFrame.COL_TIME);
      this.data.addSeries(DataFrame.COL_TIME, this.data.getLongs(DataFrame.COL_TIME).multiply(1000));
    }

    this.outlierData = new DataFrame(this.data);
    this.outlierData.set(DataFrame.COL_VALUE,
        this.outlierData.getLongs(DataFrame.COL_TIME).between(86400000L, 86400000L * 3),
        this.outlierData.getDoubles(DataFrame.COL_VALUE).multiply(0.5));
  }

  @Test
  public void testOutlierNoData() {
    Assert.assertTrue(AlgorithmUtils.getOutliers(new DataFrame(), new Duration(0)).isEmpty());
  }

  @Test
  public void testOutlierNone() {
    Assert.assertTrue(AlgorithmUtils.getOutliers(this.data, new Duration(86400000L)).allFalse());
  }

  @Test
  public void testOutlierTooShort() {
    Assert.assertTrue(AlgorithmUtils.getOutliers(this.outlierData, new Duration(86400000L * 3)).allFalse());
  }

  @Test
  public void testOutlier() {
    // NOTE: spline smoothing effect expands window
    Assert.assertEquals(AlgorithmUtils.getOutliers(this.outlierData, new Duration(86400000L)).sum().longValue(), 57);
  }

  @Test
  public void testChangeNoData() {
    Assert.assertTrue(AlgorithmUtils.getChangePoints(new DataFrame(), new Duration(0)).isEmpty());
  }

  @Test
  public void testChangeNone() {
    Assert.assertTrue(AlgorithmUtils.getChangePoints(this.data, new Duration(86400000L * 7)).isEmpty());
  }

  @Test
  public void testChangeTooShort() {
    Assert.assertTrue(AlgorithmUtils.getChangePoints(this.outlierData, new Duration(86400000L * 8)).isEmpty());
  }

  @Test
  public void testChange() {
    Assert.assertEquals(AlgorithmUtils.getChangePoints(this.outlierData, new Duration(86400000L)).size(), 2);
    Assert.assertNotNull(AlgorithmUtils.getChangePoints(this.outlierData, new Duration(86400000L)).lower(86400000L));
    Assert.assertNotNull(AlgorithmUtils.getChangePoints(this.outlierData, new Duration(86400000L)).higher(86400000L * 2));
  }

//  @Test
//  public void testRescaleNoData() {
//    Assert.assertTrue(AlgorithmUtils.getRescaledSeries(new DataFrame(), new HashSet<Long>()).isEmpty());
//  }
//
//  @Test
//  public void testRescaleNone() {
//    Assert.assertEquals(AlgorithmUtils.getRescaledSeries(this.data, new HashSet<Long>()), this.data);
//  }
//
//  @Test
//  public void testRescale() {
//    Set<Long> changePoints = new HashSet<>(Arrays.asList(86400000L, 86400000L * 3));
//    Assert.assertTrue(equals(AlgorithmUtils.getRescaledSeries(this.outlierData, changePoints), this.data, 50));
//  }

  @Test
  public void testFastBSplineNoData() {
    DoubleSeries s = DoubleSeries.buildFrom();
    Assert.assertTrue(equals(AlgorithmUtils.fastBSpline(s, 1), DoubleSeries.buildFrom(), 0.001));
  }

  @Test
  public void testFastBSplineOneElement() {
    DoubleSeries s = DoubleSeries.buildFrom(5);
    Assert.assertTrue(equals(AlgorithmUtils.fastBSpline(s, 1), DoubleSeries.buildFrom(5), 0.001));
  }

  @Test
  public void testFastBSplineTwoElements() {
    DoubleSeries s = DoubleSeries.buildFrom(5, 6);
    Assert.assertTrue(equals(AlgorithmUtils.fastBSpline(s, 1), DoubleSeries.buildFrom(5.5, 5.5), 0.001));
  }

  @Test
  public void testFastBSpline() {
    DoubleSeries s = DoubleSeries.buildFrom(1, 2, 3, 1, 2, 3, 1, 3, 2);
    Assert.assertTrue(equals(AlgorithmUtils.fastBSpline(s, 4), DoubleSeries.buildFrom(2, 2, 2, 2, 2, 2, 2, 2, 2), 0.25));
  }

  @Test
  public void testRobustMeanNoData() {
    DoubleSeries s = DoubleSeries.buildFrom();
    Assert.assertTrue(equals(AlgorithmUtils.robustMean(s, 1), s, 0.001));
  }

  @Test
  public void testRobustMeanSingleWindow() {
    DoubleSeries s = DoubleSeries.buildFrom(1, 2, 3);
    Assert.assertTrue(equals(AlgorithmUtils.robustMean(s, 1), DoubleSeries.buildFrom(1, 2, 3), 0.001));
  }

  @Test
  public void testRobustMeanSmallWindow() {
    DoubleSeries s = DoubleSeries.buildFrom(1, 2, 3);
    Assert.assertTrue(equals(AlgorithmUtils.robustMean(s, 2), DoubleSeries.buildFrom(Double.NaN, 1.5, 2.5), 0.001));
  }

  @Test
  public void testRobustMean() {
    DoubleSeries s = DoubleSeries.buildFrom(1, 2, 3, 4, 5, 6);
    Assert.assertTrue(equals(AlgorithmUtils.robustMean(s, 4), DoubleSeries.buildFrom(Double.NaN, Double.NaN, Double.NaN, 2.5, 3.5, 4.5), 0.001));
  }

  private static boolean equals(DataFrame a, DataFrame b, double epsilon) {
    if (!a.getSeriesNames().equals(b.getSeriesNames())) {
      return false;
    }

    if (!a.get(DataFrame.COL_TIME).equals(b.get(DataFrame.COL_TIME))) {
      return false;
    }

    DoubleSeries valA = a.getDoubles(DataFrame.COL_VALUE);
    DoubleSeries valB = b.getDoubles(DataFrame.COL_VALUE);

    return equals(valA, valB, epsilon);
  }

  private static boolean equals(DoubleSeries a, DoubleSeries b, double epsilon) {
    return !a.lte(b.add(epsilon)).and(a.gte(b.subtract(epsilon))).hasFalse();
  }
}
