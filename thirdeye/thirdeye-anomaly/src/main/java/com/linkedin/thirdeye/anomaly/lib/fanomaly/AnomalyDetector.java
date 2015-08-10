package com.linkedin.thirdeye.anomaly.lib.fanomaly;


import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.jblas.DoubleMatrix;


public abstract class AnomalyDetector {
  public long trainStart;
  public long trainEnd;
  public int stepsAhead;
  public long timeGranularity;
  public Set<Long> omitTimestamps;

  public AnomalyDetector(long trainStartInput, long trainEndInput, int stepsAheadInput, long timeGranularityInput,
      Set<Long> omitTimestampsInput) {
    trainStart = trainStartInput;
    trainEnd = trainEndInput;
    stepsAhead = stepsAheadInput;
    timeGranularity = timeGranularityInput;
    omitTimestamps = omitTimestampsInput;
  }

  // utility function
  public abstract Map<Long, FanomalyDataPoint> DetectAnomaly(double[] inputTimeSeries, long[] inputTimeStamps,
      long offset) throws Exception;

  protected DoubleMatrix[] RemoveTimeStamps(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps) {
    if (omitTimestamps.size() > 0) {
      for (int i = 0; i < inputTimeSeries.length; i++) {
        if (omitTimestamps.contains(inputTimeStamps[i])) {
          inputTimeSeries[i] = null;
        }
      }
    }
    return inputTimeSeries;
  }

  protected DoubleMatrix[] getPredictionData(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps) throws Exception {
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
      return  Arrays.copyOfRange(inputTimeSeries, startPredictionIndex, endPredictionIndex);
    }
  }

  // todo getTrainingTimeStamps
  protected DoubleMatrix[] getTrainingData(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps) throws Exception {
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
}

