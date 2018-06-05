package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.DataFrame;
import java.io.InputStreamReader;
import java.io.Reader;
import org.joda.time.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class AlgorithmUtilsTest {
  DataFrame data;
  DataFrame outlierData;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("timeseries-4w.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(COL_TIME);
      this.data.addSeries(COL_TIME, this.data.getLongs(COL_TIME).multiply(1000));
    }

    this.outlierData = new DataFrame(this.data);
    this.outlierData.set(COL_VALUE,
        this.outlierData.getLongs(COL_TIME).between(86400000L, 86400000L * 3),
        this.outlierData.getDoubles(COL_VALUE).subtract(500));
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
    Assert.assertEquals(AlgorithmUtils.getOutliers(this.outlierData, new Duration(86400000L)).sum().longValue(), 48);
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
    Assert.assertTrue(AlgorithmUtils.getChangePoints(this.outlierData, new Duration(86400000L)).contains(86400000L));
    Assert.assertTrue(AlgorithmUtils.getChangePoints(this.outlierData, new Duration(86400000L)).contains(86400000L * 3 - 3600000L));
    // NOTE: last point is technically part of lower level, but above estimated median
  }
}
