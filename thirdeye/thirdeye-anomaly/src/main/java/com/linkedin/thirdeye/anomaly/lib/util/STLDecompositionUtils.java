package com.linkedin.thirdeye.anomaly.lib.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.util.ResourceUtils;


/**
 * This is a hack to get STL decomposition working. The implementation should be replaced by java, or a version using
 * rserve.
 */
public class STLDecompositionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(STLDecompositionUtils.class);

  private static final String TMP_STL_WRAPPER_R_PATH;

  static {
    String outputPath = null;
    try {
      outputPath = "/tmp/" + md5(ResourceUtils.getResourceAsString("r/stl-wrapper.R") + "-stl-wrapper.R");
    } catch (IOException e) {
      e.printStackTrace();
    }
    TMP_STL_WRAPPER_R_PATH = outputPath;

    InputStream in = null;
    OutputStream out = null;
    try {
      in = STLDecompositionUtils.class.getClassLoader().getResourceAsStream("r/stl-wrapper.R");
      out = new FileOutputStream(TMP_STL_WRAPPER_R_PATH);
      IOUtils.copy(in, out);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static String md5(String s) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOGGER.error("no md5 available", e);
    }
    md.update(s.getBytes());
    byte[] digest = md.digest();
    StringBuffer sb = new StringBuffer();
    for (byte b : digest) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }


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
          TMP_STL_WRAPPER_R_PATH,
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

}