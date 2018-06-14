package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import org.joda.time.Duration;


/**
 * Utility class for cross-cutting aspects of detection algorithms such as
 * outlier and change point detection
 *
 * TODO implement all detection methods. all the methods!
 */
public class AlgorithmUtils {
  private static final int DEFAULT_SPLINE_ITERATIONS = 4;

  private static final String COL_TIME = DataFrameUtils.COL_TIME;
  private static final String COL_VALUE = DataFrameUtils.COL_VALUE;

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
  static BooleanSeries getOutliers(DataFrame df, Duration minDuration) {
    if (df.isEmpty()) {
      return BooleanSeries.empty();
    }

    LongSeries time = df.getLongs(COL_TIME);
    DoubleSeries value = df.getDoubles(COL_VALUE);

    byte[] outlier = new byte[df.size()];
    Arrays.fill(outlier, (byte) 0);

    // TODO expanding window from last change point
    double upQuartile = value.quantile(0.75).doubleValue();
    double downQuartile = value.quantile(0.25).doubleValue();

    int runStart = 0;
    int runSide = 0;
    for (int i = 0; i < df.size(); i++) {
      if (!value.isNull(i)) {
        int side = 0;
        if (value.getDouble(i) > upQuartile)
          side = 1;
        if (value.getDouble(i) < downQuartile)
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
   * <b>NOTE:</b> this is very basic and sensitive to noise
   *
   * @param df time series
   * @param minDuration time series
   * @return change points
   */
  static TreeSet<Long> getChangePoints(DataFrame df, Duration minDuration) {
    if (df.isEmpty()) {
      return new TreeSet<>();
    }

    LongSeries time = df.getLongs(COL_TIME);
    DoubleSeries value = fastBSpline(df.getDoubles(COL_VALUE), DEFAULT_SPLINE_ITERATIONS);

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
   * Helper for simulating B-spline with moving averages.
   *
   * @param s double series
   * @param n number of iterations (e.g. 4 for cubic)
   * @return smoothed series
   */
  static DoubleSeries fastBSpline(DoubleSeries s, int n) {
    // NOTE: rules-of-thumb from https://en.wikipedia.org/wiki/Gaussian_filter
    // four successive moving averages

    DoubleSeries copy = s.copy();
    double[] values = copy.values();
    for (int j = 0; j < n; j++) {
      for (int i = 1; i < values.length - 1; i++) {
        values[i] = (values[i - 1] + values[i] + values[i + 1]) / 3;
      }
    }

    return copy;
  }

  /**
   * Returns the time series re-scaled to and stitched backwards from change points
   * (i.e. the last timestamp will be on its original level).
   * <br/><b>NOTE:</b> rescales relative to segment median
   *
   * @param dfTimeseries time series dataframe
   * @param changePoints set of change points
   * @return re-scaled time series
   */
  static DataFrame getRescaledSeries(DataFrame dfTimeseries, Collection<Long> changePoints) {
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
      medians.add(segment.getDoubles(COL_VALUE).median().doubleValue());
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
