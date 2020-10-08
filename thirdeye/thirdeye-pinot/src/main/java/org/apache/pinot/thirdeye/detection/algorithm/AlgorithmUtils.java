/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection.algorithm;

import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;


/**
 * Utility class for cross-cutting aspects of detection algorithms such as
 * outlier and change point detection
 *
 * TODO implement all detection methods. all the methods!
 */
public class AlgorithmUtils {
  private static final int FAST_SPLINE_ITERATIONS = 4;
  private static final long MIN_WINDOW_SIZE = TimeUnit.DAYS.toMillis(1);

  private static final String COL_TIME = DataFrame.COL_TIME;
  private static final String COL_VALUE = DataFrame.COL_VALUE;

  private AlgorithmUtils() {
    // left blank
  }

  /**
   * Determines outliers based on run length vs quartiles.
   * <b>NOTE:</b> this is very basic and sensitive to noise
   *
   * @param df time series
   * @param minDuration time series
   * @return outlier flag series
   */
  public static BooleanSeries getOutliers(DataFrame df, Duration minDuration) {
    if (df.isEmpty()) {
      return BooleanSeries.empty();
    }

    LongSeries time = df.getLongs(COL_TIME);
    DoubleSeries value = fastBSpline(df.getDoubles(COL_VALUE), FAST_SPLINE_ITERATIONS);

    byte[] outlier = new byte[df.size()];
    Arrays.fill(outlier, (byte) 0);

    double upTercile = value.quantile(0.666).doubleValue();
    double downTercile = value.quantile(0.333).doubleValue();

    int runStart = 0;
    int runSide = 0;
    for (int i = 0; i < df.size(); i++) {
      if (!value.isNull(i)) {
        int side = 0;
        if (value.getDouble(i) > upTercile)
          side = 1;
        if (value.getDouble(i) < downTercile)
          side = -1;

        // run side changed or last run
        if (runSide != side || i >= df.size() - 1) {
          long past = time.getLong(runStart);
          long now = time.getLong(i);
          long runDuration = now - past;
          if (runSide != 0 && runDuration >= minDuration.getMillis()) {
            Arrays.fill(outlier, runStart, i, (byte) 1);
          }
          runStart = i;
          runSide = side;
        }
      }
    }

    return BooleanSeries.buildFrom(outlier);
  }

  /**
   * Determines change points based on run length vs median
   *
   * @param df time series
   * @param minDuration time series
   * @return change points
   */
  public static TreeSet<Long> getChangePoints(DataFrame df, Duration minDuration) {
    if (df.isEmpty()) {
      return new TreeSet<>();
    }

    LongSeries time = df.getLongs(COL_TIME);
    DoubleSeries value = fastBSpline(df.getDoubles(COL_VALUE), FAST_SPLINE_ITERATIONS);

    TreeSet<Long> changePoints = new TreeSet<>();

    int lastChange = 0;
    int runStart = 0;
    int runSide = 0;
    for (int i = 0; i < df.size(); i++) {
      if (!value.isNull(i) && lastChange < i) {
        double median = value.slice(lastChange, i).median().doubleValue();
        double val = value.getDouble(i);

        int side = Double.compare(val, median);

        // run side changed or last run
        if (runSide != side || i >= df.size() - 1) {
          long past = time.getLong(runStart);
          long now = time.getLong(i);
          long runDuration = now - past;
          if (runDuration >= minDuration.getMillis()) {
            lastChange = runStart;
            changePoints.add(past);
          }
          runStart = i;
          runSide = side;
        }
      }
    }

    return changePoints;
  }

  /**
   * Determines change points based on run length vs terciles. Typically used with diff
   * series.
   *
   * @param df time series
   * @param minDuration time series
   * @return change points
   */
  public static TreeSet<Long> getChangePointsTercileMethod(DataFrame df, Duration minDuration) {
    if (df.isEmpty()) {
      return new TreeSet<>();
    }

    LongSeries time = df.getLongs(COL_TIME);
    DoubleSeries value = fastBSpline(df.getDoubles(COL_VALUE), FAST_SPLINE_ITERATIONS);

    TreeSet<Long> changePoints = new TreeSet<>();

    int lastChange = 0;
    int runStart = 0;
    int runSide = 0;
    for (int i = 0; i < df.size(); i++) {
      if (!value.isNull(i) && lastChange < i) {
        DoubleSeries history = value.slice(lastChange, i);
        double upTercile = history.quantile(0.666).doubleValue();
        double downTercile = history.quantile(0.333).doubleValue();

        double val = value.getDouble(i);

        int side = 0;
        if (val > upTercile)
          side = 1;
        if (val < downTercile)
          side = -1;

        // run side changed or last run
        if (runSide != side || i >= df.size() - 1) {
          long past = time.getLong(runStart);
          long now = time.getLong(i);
          long runDuration = now - past;
          if (runSide != 0 && runDuration >= minDuration.getMillis()) {
            lastChange = runStart;
            changePoints.add(past);
          }
          runStart = i;
          runSide = side;
        }
      }
    }

    return changePoints;
  }

  /**
   * Determines change points based on robust mean window
   *
   * @param df time series
   * @param windowSize robust mean window size
   * @param minDuration time series
   * @return change points
   */
  public static TreeSet<Long> getChangePointsRobustMean(DataFrame df, int windowSize, Duration minDuration) {
    if (df.isEmpty()) {
      return new TreeSet<>();
    }

    LongSeries time = df.getLongs(COL_TIME);
    DoubleSeries value = robustMean(df.getDoubles(COL_VALUE), windowSize);

    TreeSet<Long> changePoints = new TreeSet<>();

    int lastChange = 0;
    int runStart = 0;
    int runSide = 0;
    for (int i = 0; i < df.size(); i++) {
      if (!value.isNull(i) && lastChange < i) {
        DoubleSeries medianSeries = value.slice(lastChange, i).median();

        if (medianSeries.allNull()) {
          continue;
        }

        double median = medianSeries.doubleValue();
        double val = value.getDouble(i);

        int side = Double.compare(val, median);

        // run side changed or last run
        if (runSide != side || i >= df.size() - 1) {
          long past = time.getLong(runStart);
          long now = time.getLong(i);
          long runDuration = now - past;
          if (runDuration >= minDuration.getMillis()) {
            lastChange = runStart;
            changePoints.add(past);
          }
          runStart = i;
          runSide = side;
        }
      }
    }

    return changePoints;
  }

  /**
   * Helper for simulating B-spline with moving averages.
   *
   * @param s double series
   * @return smoothed series
   */
  public static DoubleSeries fastBSpline(DoubleSeries s, int n) {
    // NOTE: rules-of-thumb from https://en.wikipedia.org/wiki/Gaussian_filter
    // four successive simple averages

    if (s.size() <= 1) {
      return s;
    }

    DoubleSeries copy = s.copy();
    double[] values = copy.values();
    double[] newValues = new double[values.length];
    for (int j = 0; j < n; j++) {
      for (int i = 0; i < values.length; i++) {
        if (DoubleSeries.isNull(values[i])) {
          newValues[i] = DoubleSeries.NULL;
          continue;
        }

        double sum = 0;
        int count = 0;

        if (i > 0 && !DoubleSeries.isNull(values[i - 1])) {
          sum += values[i - 1];
          count++;
        }

        if (i < values.length - 1 && !DoubleSeries.isNull(values[i + 1])) {
          sum += values[i + 1];
          count++;
        }

        newValues[i] = (values[i] + sum) / (count + 1);
      }

      System.arraycopy(newValues, 0, values, 0, values.length);
    }

    return copy;
  }

  /**
   * Helper for robust mean with window size
   *
   * @param s double series
   * @return smoothed series
   */
  public static DoubleSeries robustMean(DoubleSeries s, int n) {
    return s.groupByMovingWindow(n).aggregate(new Series.DoubleFunction() {
      @Override
      public double apply(double... values) {
        if (values.length <= 0)
          return DoubleSeries.NULL;

        double[] sorted = Arrays.copyOf(values, values.length);
        Arrays.sort(sorted);
        int hi = (int) Math.ceil(sorted.length * 0.75);
        int lo = (int) Math.floor(sorted.length * 0.25);

        double sum = 0.0;
        for (int i = lo; i < hi; i++) {
          sum += sorted[i];
        }

        return sum / (hi - lo);
      }
    }).getValues().getDoubles();
  }

  /**
   * Returns the time series re-scaled to and stitched backwards from change points
   * (i.e. the last timestamp will be on its original level). This method is suitable
   * for level changes but not for trending time series.
   *
   * <br/><b>NOTE:</b> rescales relative to segment median
   *
   * @param dfTimeseries time series dataframe
   * @param changePoints set of change points
   * @param lookForward look forward period for segment adjustment (all if negative)
   * @return re-scaled time series
   */
  public static DataFrame getRescaledSeries(DataFrame dfTimeseries, Collection<Long> changePoints, long lookForward) {
    if (dfTimeseries.isEmpty()) {
      return dfTimeseries;
    }

    DataFrame df = new DataFrame(dfTimeseries);

    List<Long> myChangePoints = new ArrayList<>(changePoints);
    Collections.sort(myChangePoints);

    LongSeries timestamps = df.getLongs(COL_TIME);

    // create segments
    List<DataFrame> segments = new ArrayList<>();
    long prevTimestamp = timestamps.min().longValue();
    for (Long changePoint : myChangePoints) {
      segments.add(df.filter(timestamps.between(prevTimestamp, changePoint)).dropNull(COL_TIME));
      prevTimestamp = changePoint;
    }

    segments.add(df.filter(timestamps.gte(prevTimestamp)).dropNull(COL_TIME));

    // compute median
    List<Double> medians = new ArrayList<>();
    for (DataFrame segment : segments) {
      long start = segment.getLong(COL_TIME, 0);
      long cutoff = lookForward > 0 ? (start + lookForward) : Long.MAX_VALUE;
      medians.add(segment.filter(segment.getLongs(COL_TIME).lt(cutoff)).dropNull(COL_TIME).getDoubles(COL_VALUE).median().doubleValue());
    }

    // rescale time series
    double lastMedian = medians.get(medians.size() - 1);
    for (int i = 0; i < segments.size(); i++) {
      DataFrame segment = segments.get(i);
      double median = medians.get(i);
      double scalingFactor = lastMedian / median;
      segment.addSeries(COL_VALUE, segment.getDoubles(COL_VALUE).multiply(scalingFactor));
    }

    return DataFrame.concatenate(segments);
  }
}
