package com.linkedin.thirdeye.anomaly.lib.scanstatistics;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.anomaly.lib.util.STLDecomposition;
import com.linkedin.thirdeye.anomaly.lib.util.STLDecomposition.STLResult;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;
import com.linkedin.thirdeye.anomaly.builtin.ScanStatisticsAnomalyDetectionFunction;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.inference.TTest;

public class STLTest {
  @Test
  public void stlTest() throws IOException {
    testSTL("timeseries.csv", "outputTimeSeries.csv", 168, 1, 15, 0.75, 0.10, true);
    testSTL("outputtest1.csv", 24, 10, 1.0, 0.1, 1, 15, 0.75, 0.75, true);
    testSTL("outputtest2.csv", 24, 5, -2.0, 1.0, 1, 15, 0.75, 0.75, true);
    testSTL("outputtest3.csv", 12, 1, 1, 10.0, 1, 15, 0.75, 0.75, true);

  }
  private void testSTL(String outputfile, int seasonality, double level, double slope, double noisevariance, int innerloop, int outerloop,
      double lowpassbandwidth, double trendcomponentbandwidth, boolean periodic) throws IOException{
    int size = 500;
    Random rng = new Random();
    double[] dataPoints = new double[size];
    double[] generated_trend = new double[size];
    double[] generated_seasonal = new double[size];
    double[] generated_residual = new double[size];
    long[] timestamps = new long[size];
    double[] slopes = new double[3];
    for (int jj=0; jj< size; jj++) {
        generated_trend[jj] = jj * slope * level + level;
        generated_seasonal[jj] = level * Math.sin(((double)jj/(double)seasonality) * 2 * Math.PI);
        generated_residual[jj] = rng.nextGaussian() * Math.sqrt(noisevariance);
        dataPoints[jj] = generated_trend[jj] + generated_seasonal[jj] + generated_residual[jj];
    }
    double[][] data = removeSeasonality(timestamps, dataPoints, seasonality, innerloop, outerloop, lowpassbandwidth,
            trendcomponentbandwidth, true);
    double[] data_trend = data[0];
    double[] data_remainder = data[1];
    double[] data_seasonal = data[2];
    double[] data_seasonal_check = new double[seasonality];
    double[] seasonal_check = new double[seasonality];
    outputToCsv(generated_trend, data_trend, generated_seasonal, data_seasonal, generated_residual,
        data_remainder, outputfile);
    double trend_pearson = CheckTimeSeriesSimularityPearson(generated_trend, data_trend);
    double remainder_pearson = CheckTimeSeriesSimularityPearson(generated_residual, data_remainder);
    double seasonal_pearson = CheckTimeSeriesSimularityPearson(generated_seasonal, data_seasonal);
    boolean trend_pearson_test =  trend_pearson > 0.75;
    boolean remainder_pearson_test =  remainder_pearson > 0.75;
    boolean seasonal_pearson_test =  seasonal_pearson > 0.75;
    System.out.println(trend_pearson + "," + remainder_pearson + "," + seasonal_pearson);
    for (int ll = 0; ll < seasonality; ll++) {
        seasonal_check[ll] = level * Math.sin(((double)ll/(double)seasonality) * 2 * Math.PI);
        data_seasonal_check[ll] = data_seasonal[ll];

      }
      boolean reject_null =  checkTimeSeriesSimularityTtest(seasonal_check, data_seasonal_check);
      Assert.assertEquals(reject_null, false);
    }


  private void testSTL(String filename, String outputfilename, int seasonality, int innerloop, int outerloop,
      double lowpassbandwidth, double trendcomponentbandwidth, boolean periodic) throws IOException{
    String[] lines = ResourceUtils.getResourceAsString(filename).split("\n");
    int numData = lines.length;
    long[] timestamps = new long[numData];
    double[] series = new double[numData];
    double[] trend_r = new double[numData];
    double[] residual_r = new double[numData];
    double[] seasonal_r = new double[numData];
    for (int i = 0; i < numData; i++) {
      timestamps[i] = i;
      String[] values = lines[i].split(",");
      String value = values[1];
      if (value.equals("NA")) {
        series[i] = Double.NaN;
      } else {
        series[i] = Double.valueOf(value);
      }
      trend_r[i] = Double.valueOf(values[2]);
      residual_r[i] = Double.valueOf(values[3]);
      seasonal_r[i] = Double.valueOf(values[4]);
    }
    ScanStatisticsAnomalyDetectionFunction.removeMissingValuesByAveragingNeighbors(series);

    double[][] data = removeSeasonality(timestamps, series, seasonality, innerloop, outerloop, lowpassbandwidth,
        trendcomponentbandwidth, true);
    double[] data_trend = data[0];
    double[] data_remainder = data[1];
    double[] data_seasonal = data[2];
    double[] data_seasonal_check = new double[seasonality];
    double[] seasonal_r_check = new double[seasonality];
    outputToCsv(trend_r, data_trend, seasonal_r, data_seasonal, residual_r, data_remainder, outputfilename);

    double trend_pearson = CheckTimeSeriesSimularityPearson(trend_r, data_trend);
    double remainder_pearson = CheckTimeSeriesSimularityPearson(residual_r, data_remainder);
    double seasonal_pearson = CheckTimeSeriesSimularityPearson(seasonal_r, data_seasonal);
    boolean trend_pearson_test =  trend_pearson > 0.75;
    boolean remainder_pearson_test =  remainder_pearson > 0.75;
    boolean seasonal_pearson_test =  seasonal_pearson > 0.75;
    System.out.println(trend_pearson + "," + remainder_pearson + "," + seasonal_pearson);
    Assert.assertEquals(trend_pearson_test, true);
    Assert.assertEquals(remainder_pearson_test, true);
    Assert.assertEquals(seasonal_pearson_test, true);
    boolean reject_null =  checkTimeSeriesSimularityTtest(seasonal_r_check, data_seasonal_check);
    Assert.assertEquals(reject_null, false);
  }

  private void outputToCsv(double[] trend_r, double[] data_trend,
      double[] seasonal_r, double[] data_seasonal, double[] residual_r, double[] data_remainder, String outputFilename) throws IOException {
    int numData = trend_r.length;
    List<String[]> output_list = new ArrayList<String []> ();
    for (int ii = 0; ii < numData; ii++) {
      String[] a_line = new String[6];
      a_line[0] = Double.toString(trend_r[ii]);
      a_line[1] = Double.toString(data_trend[ii]);
      a_line[2] = Double.toString(seasonal_r[ii]);
      a_line[3] = Double.toString(data_seasonal[ii]);
      a_line[4] = Double.toString(residual_r[ii]);
      a_line[5] = Double.toString(data_remainder[ii]);
      output_list.add(ii, a_line);
    }

    File file = new File(outputFilename);
    OutputStream fop = new FileOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fop));
    for (String[] element : output_list) {
      String csvLine = Joiner.on(",").join(element);
      writer.write(csvLine);
      writer.newLine();
    }
    writer.close();
  }

  private boolean checkTimeSeriesSimularityTtest(double[] r_generated, double[] java_generated) {
      TTest t_test_obj = new TTest();
      // Performs a paired t-test evaluating the null hypothesis that the mean of the paired differences between sample1
      // and sample2 is 0 in favor of the two-sided alternative that the mean paired difference is not equal to 0,
      // with significance level 0.05.
      double p_value = t_test_obj.tTest(r_generated, java_generated);
      boolean reject_null_hyphothesis = (p_value < 0.05);
      return reject_null_hyphothesis;
  }
  private double CheckTimeSeriesSimularityPearson(double[] r_generated, double[] java_generated) {
     PearsonsCorrelation pearson = new PearsonsCorrelation();
     double pearson_correlation = pearson.correlation(r_generated, java_generated);
     return pearson_correlation;

  }

  public static double[][] removeSeasonality(long[] timestamps, double[] series, int seasonality,
      int innerloop, int outerloop, double lowpassbandwidth, double trendcomponentbandwidth, boolean periodic) {
    STLDecomposition.Config config = new STLDecomposition.Config();
    config.setNumberOfObservations(seasonality);
    config.setNumberOfInnerLoopPasses(innerloop);
    config.setNumberOfRobustnessIterations(outerloop);
    config.setLowPassFilterBandwidth(lowpassbandwidth);
    config.setTrendComponentBandwidth(trendcomponentbandwidth);
    config.setPeriodic(periodic);
    config.setNumberOfDataPoints(timestamps.length);
    STLDecomposition stl = new STLDecomposition(config);

    STLResult res = stl.decompose(timestamps, series);

    double[] trend = res.getTrend();
    double[] remainder = res.getRemainder();
    double[] seasonalityOutput = new double[trend.length];

    for (int i = 0; i < trend.length; i++) {
      seasonalityOutput[i] = series[i] -(trend[i] + remainder[i]);
    }
    double[][] results = new double[3][trend.length];
    results[0] = trend;
    results[1] = remainder;
    results[2] = seasonalityOutput;
    return results;
  }

}
