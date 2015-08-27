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

  /**
   * @param timestamps
   *  This should be a sorted array of timestamps with no gaps
   * @param series
   *  The raw series values
   * @param seasonality
   *  The seasonality in number of buckets
   * @return
   *  The timeseries with seasonality removed
   */
  public static double[] removeSeasonality(long[] timestamps, double[] series, int seasonality) {
    int numDataPoints = timestamps.length;
    if (numDataPoints != series.length) {
      throw new IllegalArgumentException("time series lengths do not match");
    }

    if (seasonality < 0) {
      throw new IllegalArgumentException("seasonality cannot be negative");
    }

    Process process = null;
    BufferedReader stdout = null;
    BufferedWriter stdin = null;

    try {
      ProcessBuilder processBuilder = new ProcessBuilder(
          "Rscript",
          STLDecompositionUtils.class.getClassLoader().getResource("r/stl-wrapper.R").getPath(),
          "" + seasonality);
      processBuilder.redirectErrorStream(true);
      process = processBuilder.start();

      // the Rscript's stdout
      stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));

      // the Rscript's stdin
      stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));

      stdin.write("\"Bucket\",\"Series\"\n");
      for (int i = 0; i < numDataPoints; i++) {
        stdin.write(timestamps[i] + "," + series[i] + "\n");
      }
      stdin.close();

      // wait for Rscript to finish
      process.waitFor();

      // read the result line by line
      double[] result = new double[numDataPoints];

      for (int i = 0; i < numDataPoints; i++){
        String line = stdout.readLine();
        result[i] = Double.valueOf(line);
      }

      return result;
    } catch (Exception e) {
      LOGGER.info("exception trying to run stl", e);
      return null;
    } finally {
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
      if (process != null) {
        process.destroyForcibly();
      }
    }
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