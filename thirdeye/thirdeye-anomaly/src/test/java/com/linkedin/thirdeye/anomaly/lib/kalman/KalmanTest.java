package com.linkedin.thirdeye.anomaly.lib.kalman;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.api.function.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;
import com.linkedin.thirdeye.anomaly.builtin.ScanStatisticsAnomalyDetectionFunction;

public class KalmanTest {

  @Test
  public void ScanRemoveAnomalyTest() throws IOException {
    //NoHistoryAnomaliesRemoved("timeseries.csv",1369, 200);
    TestNoHistoryRemoveKalman("timeseries.csv");



  }

  private void TestNoHistoryRemoveKalman(String filename) throws IOException {
    Set<Long> omitTimestamps = new HashSet<Long>();
    int seasonal = 168;
    int order = 1;
    double r = 10000;
    double pValueThreshold = 0.05;
    double[] series = GetData(filename);
    int numData = series.length;
    long[] timestamps = new long[numData];
    for (int i = 0; i < numData; i++) {
      timestamps[i] = i;
    }
    StateSpaceAnomalyDetector stateSpaceDetector = new StateSpaceAnomalyDetector(
        timestamps[0],
        timestamps[numData-1],
        -1, // stepsAhead
        1,
        omitTimestamps,
        seasonal,
        order,
        order + seasonal - 1, // numStates
        1, // outputStates
        r);

    Map<Long, StateSpaceDataPoint> resultsByTimeWindow;
    try {
      resultsByTimeWindow = stateSpaceDetector.detectAnomalies(series, timestamps, -1);
      System.out.println(resultsByTimeWindow.size());
    } catch (Exception e) {
      throw new FunctionDidNotEvaluateException("something went wrong", e);
    }


    for (long timeWindow : new TreeSet<>(resultsByTimeWindow.keySet()))
    {
      StateSpaceDataPoint stateSpaceDataPoint = resultsByTimeWindow.get(timeWindow);

      if (stateSpaceDataPoint.pValue <  pValueThreshold)
      {
        System.out.println(timeWindow);
        System.out.println("actualValue: " + stateSpaceDataPoint.actualValue);
        System.out.println("predictedValue" + stateSpaceDataPoint.predictedValue);
        System.out.println("pValue: " + stateSpaceDataPoint.pValue);

      }
    }


  }



  private double[] GetData(String filename) throws IOException {
    String[] lines = ResourceUtils.getResourceAsString(filename).split("\n");
    int numData = lines.length;
    double[] series = new double[numData];
    for (int i = 0; i < numData; i++) {
        String[] values = lines[i].split(",");
        String value = values[1];
        if (value.equals("NA")) {
          series[i] = Double.NaN;
        } else {
          series[i] = Double.valueOf(value);
        }
    }
    ScanStatisticsAnomalyDetectionFunction.removeMissingValuesByAveragingNeighbors(series);
    for (int i = 0; i < numData; i++) {
      series[i] = Math.exp(series[i]);
    }
    return series;
  }

}
