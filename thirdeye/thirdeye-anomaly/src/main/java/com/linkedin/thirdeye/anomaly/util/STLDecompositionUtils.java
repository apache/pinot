package com.linkedin.thirdeye.anomaly.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a hack to get STL decomposition working. The implementation should be replaced by java, or a version using
 * rserve.
 */
public class STLDecompositionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(STLDecompositionUtils.class);

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /**
   * @param timestamps
   * @param series
   * @param seasonality
   * @return
   *  The timeseries with seasonality removed
   */
  public static double[] removeSeasonality(long[] timestamps, double[] series, int seasonality) {
    if (seasonality != 168) {
      throw new IllegalArgumentException("only 168 seasonality is supported");
    }

    int numDataPoints = timestamps.length;
    if (numDataPoints != series.length) {
      throw new IllegalArgumentException("time series lengths do not match");
    }

    if (seasonality < 0) {
      throw new IllegalArgumentException("seasonality cannot be negative");
    }

    double[] result = new double[numDataPoints];

    Process process = null;
    BufferedReader stdout = null;
    BufferedWriter stdin = null;
    try {
      ProcessBuilder processBuilder = new ProcessBuilder(
          "Rscript",
          STLDecompositionUtils.class.getClassLoader().getResource("r/stl-wrapper.R").getPath());
      processBuilder.redirectErrorStream(true);
      process = processBuilder.start();

      stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
      stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));

      stdin.write("\"Bucket\",\"Median\"\n");
      for (int i = 0; i < numDataPoints; i++) {
        stdin.write(DATE_FORMAT.format(new Date(timestamps[i])) + "," + series[i] + "\n");
      }
      stdin.close();

      process.waitFor();

      String headers = stdout.readLine();
      for (int i = 0; i < numDataPoints; i++){
        String line = stdout.readLine();
        result[i] = Double.valueOf(line.split(",")[1]);
      }
    } catch (Exception e) {
      LOGGER.info("exception trying to run stl", e);
      return null;
    } finally {
      if (process != null) {
        process.destroyForcibly();
      }
      if (stdin != null) {
        try {
          stdin.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
      if (stdout != null) {
        try {
          stdout.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
    }

    return result;
  }

//  public static void main(String[] args) throws IOException {
//    String[] lines = ResourceUtils.getResourceAsString("timeseries.csv").split("\n");
//    int numData = lines.length;
//    long[] timestamps = new long[numData];
//    double[] series = new double[numData];
//    for (int i = 0; i < numData; i++) {
//      timestamps[i] = i;
//      String value = lines[i].split(",")[1];
//      if (value.equals("NA")) {
//        series[i] = 0;
//      } else {
//        series[i] = Double.valueOf(value);
//      }
//    }
//
//    double[] result = removeSeasonality(timestamps, series, 168);
//
//    for (int i = 0; i < result.length; i++) {
//      System.out.println(result[i]);
//    }
//  }

}