package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import java.util.Arrays;
import java.util.TreeSet;
import org.joda.time.Duration;


/**
 * Utility class for cross-cutting aspects of detection algorithms such as
 * outlier and change point detection
 *
 * TODO implement all detection methods. all the methods!
 */
public class AlgorithmUtils {
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
    double upDecile = value.quantile(0.90).doubleValue();
    double downDecile = value.quantile(0.10).doubleValue();

    int runStart = 0;
    int runSide = 0;
    for (int i = 0; i < df.size(); i++) {
      if (!value.isNull(i)) {
        int side = 0;
        if (value.getDouble(i) > upDecile)
          side = 1;
        if (value.getDouble(i) < downDecile)
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
    DoubleSeries value = df.getDoubles(COL_VALUE);

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

}
