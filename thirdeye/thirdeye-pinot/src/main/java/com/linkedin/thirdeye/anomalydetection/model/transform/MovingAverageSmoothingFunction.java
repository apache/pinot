package com.linkedin.thirdeye.anomalydetection.model.transform;

import com.linkedin.thirdeye.anomalydetection.Utils;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.DimensionMap;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MovingAverageSmoothingFunction extends AbstractTransformationFunction {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MovingAverageSmoothingFunction.class);

  public static final String BUCKET_SIZE = "bucketSize";
  public static final String BUCKET_UNIT = "bucketUnit";
  public static final String MOVING_AVERAGE_SMOOTHING_WINDOW_SIZE = "movingAverageSmoothingWindowSize";

  /**
   * Smooths the given time series using moving average.
   *
   * If the input time series is shorter than the moving average window size, then this method
   * does not apply smoothing on the time series, i.e., it returns the original time series.
   *
   * The transformed time series is shorten by the size of the moving average window in
   * comparison to the original time series. For instance, if there are 10 consecutive data points
   * the a time series and the window size for moving average is 2, then the transformed time series
   * contains only 9 consecutive data points; The first data points has no other data point to
   * average and thus it is discarded.
   *
   * @param timeSeries the time series that provides the data points to be transformed.
   *
   * @return a time series that is smoothed using moving average.
   */
  @Override
  public TimeSeries transform(TimeSeries timeSeries) {
    Interval timeSeriesInterval = timeSeries.getTimeSeriesInterval();
    long startTime = timeSeriesInterval.getStartMillis();
    long endTime = timeSeriesInterval.getEndMillis();

    long bucketSizeInMillis = Utils.getBucketInMillis(BUCKET_SIZE, BUCKET_UNIT, getProperties());

    int movingAverageWindowSize =
        Integer.valueOf(getProperties().getProperty(MOVING_AVERAGE_SMOOTHING_WINDOW_SIZE));

    // Check if the moving average window size is larger than the time series itself
    long transformedStartTime = startTime + bucketSizeInMillis * (movingAverageWindowSize - 1);
    if (transformedStartTime > endTime) {
      String metricName = timeSeries.getTimeSeriesKey().getMetricName();
      DimensionMap dimensionMap = timeSeries.getTimeSeriesKey().getDimensionMap();
      LOGGER.warn(
          "Input time series (Metric:{}, Dimension:{}) is shorter than the moving average "
              + "smoothing window; therefore, smoothing is not applied on this time series.",
          metricName, dimensionMap);
      return timeSeries;
    }

    TimeSeries transformedTimeSeries = new TimeSeries();
    transformedTimeSeries.setTimeSeriesKey(timeSeries.getTimeSeriesKey());
    Interval transformedInterval = new Interval(transformedStartTime, endTime);
    transformedTimeSeries.setTimeSeriesInterval(transformedInterval);

    for (long timeKeyToTransform : timeSeries.timestampSet()) {
      if (timeKeyToTransform < transformedStartTime) {
        continue;
      }
      double sum = 0d;
      int count = 0;
      for (int i = 0; i < movingAverageWindowSize; ++i) {
        long timeKey = timeKeyToTransform - bucketSizeInMillis * i;
        if (timeSeries.hasTimestamp(timeKey)) {
          sum += timeSeries.get(timeKey);
          ++count;
        }
      }
      double average = sum / count; // count is at least one due to the existence of timeKeyToTransform
      transformedTimeSeries.set(timeKeyToTransform, average);
    }

    return transformedTimeSeries;
  }
}
