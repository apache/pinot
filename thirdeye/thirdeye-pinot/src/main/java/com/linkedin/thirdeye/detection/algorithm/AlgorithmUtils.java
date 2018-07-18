package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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

//  /**
//   * Determines outliers based on run length vs quartiles.
//   * <b>NOTE:</b> this is very basic and sensitive to noise
//   *
//   * @param df time series
//   * @param minDuration time series
//   * @return outlier flag series
//   */
//  public static BooleanSeries getOutliersTercileMethod(DataFrame df, Duration minDuration, Set<Long> changePoints) {
//    if (df.isEmpty()) {
//      return BooleanSeries.empty();
//    }
//
//    LongSeries time = df.getLongs(COL_TIME);
//    DoubleSeries value = fastBSpline(df.getDoubles(COL_VALUE), FAST_SPLINE_ITERATIONS);
//
//    byte[] outlier = new byte[df.size()];
//    Arrays.fill(outlier, (byte) 0);
//
//    TreeSet<Long> lastChanges = new TreeSet<>(changePoints);
//    lastChanges.add(time.min().longValue());
//
//    int runStart = 0;
//    int runSide = 0;
//    for (int i = 0; i < df.size(); i++) {
//      if (!value.isNull(i)) {
//        long current = time.getLong(i);
//        long minStart = current - MIN_WINDOW_SIZE;
//        long lastChange = lastChanges.floor(current);
//        long start = Math.min(minStart, lastChange);
//
//        DoubleSeries localValues = value.filter(time.between(start, current + 1));
//
//        double upTercile = localValues.quantile(0.666).doubleValue();
//        double downTercile = localValues.quantile(0.333).doubleValue();
//
//        int side = 0;
//        if (value.getDouble(i) > upTercile)
//          side = 1;
//        if (value.getDouble(i) < downTercile)
//          side = -1;
//
//        // run side changed or last run
//        if (runSide != side || i >= df.size() - 1) {
//          long past = time.getLong(runStart);
//          long now = time.getLong(i);
//          long runDuration = now - past;
//          if (runSide != 0 && runDuration >= minDuration.getMillis()) {
//            Arrays.fill(outlier, runStart, i, (byte) 1);
//          }
//          runStart = i;
//          runSide = side;
//        }
//      }
//    }
//
//    return BooleanSeries.buildFrom(outlier);
//  }

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
   * (i.e. the last timestamp will be on its original level).
   * <br/><b>NOTE:</b> rescales relative to segment median
   *
   * @param dfTimeseries time series dataframe
   * @param changePoints set of change points
   * @return re-scaled time series
   */
  public static DataFrame getRescaledSeries(DataFrame dfTimeseries, Collection<Long> changePoints) {
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

//  /**
//   * Returns indices of upper bound value in sorted series {@code s} for a sorted iterable
//   * {@code values}
//   *
//   * @param s sorted long series
//   * @param values sorted values iterable
//   * @return uperr bound indices
//   */
//  private static List<Integer> findAllUpperBounds(LongSeries s, Iterable<Long> values) {
//    Iterator<Long> itValue = values.iterator();
//    if (!itValue.hasNext()) {
//      return Collections.emptyList();
//    }
//
//    List<Integer> indices = new ArrayList<>();
//
//    long nextValue = itValue.next();
//    for (int i = 0; i < s.size(); i++) {
//      if (!s.isNull(i)) {
//        if (s.getLong(i) >= nextValue) {
//          indices.add(i);
//
//          if (!itValue.hasNext()) {
//            break;
//          }
//
//          nextValue = itValue.next();
//        }
//      }
//    }
//
//    return indices;
//  }
}
