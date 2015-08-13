package com.linkedin.thirdeye.anomaly.lib.fanomaly;

import java.util.Arrays;
import java.util.Set;

import org.jblas.DoubleMatrix;

/**
 *
 */
public class DataUtils {

  public static DoubleMatrix[] removeTimeStamps(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps,
      Set<Long> omitTimestamps) {
    if (omitTimestamps.size() > 0) {
      for (int i = 0; i < inputTimeSeries.length; i++) {
        if (omitTimestamps.contains(inputTimeStamps[i])) {
          inputTimeSeries[i] = null;
        }
      }
    }
    return inputTimeSeries;
  }

  public static DoubleMatrix[] getPredictionData(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps, long trainEnd,
      int stepsAhead) throws Exception {
    int startPredictionIndex = -1;
    for (int i = 0; i < inputTimeStamps.length; i++) {
      if (inputTimeStamps[i] > trainEnd) {
        startPredictionIndex = i;
        break;
      }
    }
    if (startPredictionIndex == -1) {
      return null;
    } else {
      int endPredictionIndex = startPredictionIndex + stepsAhead;
      if (endPredictionIndex > inputTimeStamps.length) {
        endPredictionIndex = inputTimeStamps.length;
      }
      return Arrays.copyOfRange(inputTimeSeries, startPredictionIndex, endPredictionIndex);
    }
  }

  public static DoubleMatrix[] getTrainingData(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps, long trainStart,
      long trainEnd) throws Exception {
    // clean data
    // sanity check
    if (inputTimeSeries.length != inputTimeStamps.length) {
      throw new Exception("timestamp and data not equal length.");
    }

    for (int i = 1; i < inputTimeStamps.length; i++) {
      if (inputTimeStamps[i] < inputTimeStamps[i - 1]) {
        throw new Exception("timestamp is not sorted.");
      }
    }

    // split input data to get training data
    int startIndex = 0;
    int endIndex = inputTimeStamps.length;
    for (int i = 0; i < inputTimeStamps.length; i++) {
      if (inputTimeStamps[i] >= trainStart) {
        startIndex = i;
        break;
      }
    }

    for (int i = 0; i < inputTimeStamps.length; i++) {
      if (inputTimeStamps[i] > trainEnd) {
        endIndex = i;
        break;
      }
    }

    if (trainStart < inputTimeStamps[0] || trainEnd > inputTimeStamps[inputTimeStamps.length - 1]
        || startIndex >= endIndex)
    {
      throw new Exception("training data range not valid.");
    }

    // get subarray
    return Arrays.copyOfRange(inputTimeSeries, startIndex, endIndex);
  }

  public static double estimateTrainingMean(DoubleMatrix[] processTrainingTimeSeries, int seasonal) {
    return estimateTrainingRawMoment(processTrainingTimeSeries, seasonal, 1);
  }

  public static double estimateTrainingRawMoment(DoubleMatrix[] processTrainingTimeSeries, int seasonal, int degree) {
    double estimate = 0;
    double count = 0;
    for (int ii = 0; ii < seasonal + 1; ii++)
    {
      if (processTrainingTimeSeries[ii] != null)
      {
        count++;
        estimate += Math.pow(processTrainingTimeSeries[ii].get(0, 0), degree);
      }
    }
    return estimate / count;
  }

  public static double estimateTrainingVariance(DoubleMatrix[] processTrainingTimeSeries, int seasonal) {
    double estimateMean = estimateTrainingMean(processTrainingTimeSeries, seasonal);
    double estimateVariance = 0;
    double count = 0;
    for (int ii = 0; ii < seasonal + 1; ii++)
    {
      if (processTrainingTimeSeries[ii] != null)
      {
        count++;
        estimateVariance += Math.pow(processTrainingTimeSeries[ii].get(0, 0) - estimateMean, 2);
      }
    }
    return  estimateVariance / count;
  }
}
