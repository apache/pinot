package com.linkedin.thirdeye.anomaly.lib.scanstatistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Range;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.util.ResourceUtils;
import com.linkedin.thirdeye.anomaly.builtin.ScanStatisticsAnomalyDetectionFunction;

public class ScanRemoveAnomalyTest {
  @Test
  public void ScanRemoveAnomalyTest() throws IOException {
    SequentialMonitoring("timeseries.csv", "output.csv", 720, 72, 200, ScanStatistics.Pattern.DOWN,
        ScanStatistics.Pattern.UP, 1.0, 1.0);
    //NoHistoryAnomaliesRemoved("timeseries.csv", 720, 200);

  }

  double[] GetData(String filename) throws IOException {
    String[] lines = ResourceUtils.getResourceAsString(filename).split("\n");
    int numData = lines.length;
    double[] deseasonal_series = new double[numData];
    for (int i = 0; i < numData; i++) {
      String[] values = lines[i].split(",");
      deseasonal_series[i] = Double.valueOf(values[3]) + Double.valueOf(values[2]);
    }
    return deseasonal_series;
  }

  private void SequentialMonitoring(String filename, String outputFilename, int training_size, int monitor_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      ScanStatistics.Pattern complementary_pattern,
      double target_level, double complementary_level) throws IOException {
    double[] deseasonal_series = GetData(filename);
    int numData = deseasonal_series.length;
    double up_level = 1.0;
    double down_level = 1.0;
    double notequal_level = 1.0;
    if (target_pattern == ScanStatistics.Pattern.UP) {
      up_level = target_level;
    } else if (target_pattern == ScanStatistics.Pattern.DOWN) {
      down_level = target_level;
    } else if (target_pattern == ScanStatistics.Pattern.NOTEQUAL) {
      notequal_level = target_level;
    }
    if (complementary_pattern == ScanStatistics.Pattern.UP) {
      up_level = complementary_level;
    } else if (complementary_pattern == ScanStatistics.Pattern.DOWN) {
      down_level = complementary_level;
    } else if (complementary_pattern == ScanStatistics.Pattern.NOTEQUAL) {
      notequal_level = complementary_level;
    }

    long[] timestamps = new long[numData];
    for (int i = 0; i < numData; i++) {
      timestamps[i] = i;
    }
    int monitor_start = training_size;
    int monitor_end = deseasonal_series.length - monitor_size;
    Set<Long> aggregateAnomalousTimestamps = new HashSet<Long>();
    Set<Long> targetAnomalousTimestamps = new HashSet<Long>();
    Set<Long> complementaryAnomalousTimestamps = new HashSet<Long>();

    ArrayList<String> output_list = new ArrayList<String>();

    for (int monitor_idx=monitor_start; monitor_idx <= monitor_end; monitor_idx+=monitor_size) {
      double[] train = Arrays.copyOfRange(deseasonal_series, monitor_idx-training_size, monitor_idx);
      double[] monitor = Arrays.copyOfRange(deseasonal_series, monitor_idx, monitor_idx + monitor_size);
      long[] trainingTimestamps = Arrays.copyOfRange(timestamps, monitor_idx-training_size, monitor_idx);
      ScanStatistics scanStatistics = new ScanStatistics(
          numSims,
          1,
          100000,
          0.05,
          target_pattern,
          1,
          false,
          up_level,
          down_level,
          notequal_level);

      ScanStatistics complementaryScanStatistics = new ScanStatistics(
          numSims,
          1,
          100000,
          0.05,
          complementary_pattern,
          1,
          false,
          up_level,
          down_level,
          notequal_level);


      double[] trainingDataWithOutAggregateAnomalies = ScanStatisticsAnomalyDetectionFunction.removeAnomalies(
          trainingTimestamps, train, aggregateAnomalousTimestamps);

      double[] trainingDataWithOutComplementaryAnomalies = ScanStatisticsAnomalyDetectionFunction.removeAnomalies(
          trainingTimestamps, train, complementaryAnomalousTimestamps);

      Range<Integer> target_anomaly = scanStatistics.getInterval(
          trainingDataWithOutAggregateAnomalies, monitor);

      Range<Integer> complementary_anomaly = complementaryScanStatistics.getInterval(
          trainingDataWithOutComplementaryAnomalies, monitor);

      if (target_anomaly != null) {
        for (int i = target_anomaly.lowerEndpoint(); i < target_anomaly.upperEndpoint(); i++) {
          aggregateAnomalousTimestamps.add((long)(i+monitor_idx));
          targetAnomalousTimestamps.add((long)(i+monitor_idx));
        }
        Range<Integer> anomalyOffset = Range.closedOpen(target_anomaly.lowerEndpoint() + monitor_idx,
            target_anomaly.upperEndpoint() + monitor_idx);
        output_list.add("Split : " + monitor_idx + ",Target Pattern Anomaly : " + anomalyOffset);
      }
      if (complementary_anomaly != null) {
        for (int i = complementary_anomaly.lowerEndpoint(); i < complementary_anomaly.upperEndpoint(); i++) {
          aggregateAnomalousTimestamps.add((long)(i+monitor_idx));
          complementaryAnomalousTimestamps.add((long)(i+monitor_idx));
        }
      }
    }
    File file = new File(outputFilename);
    OutputStream fop = new FileOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fop));
    for (String element : output_list) {
      System.out.println(element);
      writer.write(element);
      writer.newLine();
    }
    writer.close();
  }

  private void NoHistoryAnomaliesRemoved(String filename, int split, int numSims) throws IOException{
    double[] deseasonal_series = GetData(filename);
    double[] train = Arrays.copyOfRange(deseasonal_series, 0, split);
    double[] monitor = Arrays.copyOfRange(deseasonal_series, split, deseasonal_series.length);
    ArrayList<Range<Integer>> anomaly_down_list = new ArrayList<Range<Integer>> ();
    ArrayList<Range<Integer>> anomaly_up_list = new ArrayList<Range<Integer>> ();
    ArrayList<Range<Integer>> anomaly_notequal_list = new ArrayList<Range<Integer>> ();
    ScanStatistics scanStatistics = new ScanStatistics(
        numSims,
        1,
        100000,
        0.05,
        ScanStatistics.Pattern.DOWN,
        1,
        false);

    Range<Integer> anomaly = scanStatistics.getInterval(train, monitor);
    if (anomaly != null) {
      Range<Integer> anomalyOffset = Range.closedOpen(anomaly.lowerEndpoint() + split, anomaly.upperEndpoint() + split);
      anomaly_down_list.add(anomalyOffset);
      System.out.println("N : " + deseasonal_series.length);
      System.out.println("Split : " + split);
      System.out.println("Down Pattern Anomaly : " + anomalyOffset);
    }

    scanStatistics = new ScanStatistics(
        numSims,
        1,
        100000,
        0.05,
        ScanStatistics.Pattern.UP,
        1,
        false);

    anomaly = scanStatistics.getInterval(train, monitor);
    if (anomaly != null) {
      Range<Integer> anomalyOffset = Range.closedOpen(anomaly.lowerEndpoint() + split, anomaly.upperEndpoint() + split);
      anomaly_up_list.add(anomalyOffset);
      System.out.println("N : " + deseasonal_series.length);
      System.out.println("Split : " + split);
      System.out.println("Up Pattern Anomaly : " + anomalyOffset);
    }

    scanStatistics = new ScanStatistics(
        numSims,
        1,
        100000,
        0.05,
        ScanStatistics.Pattern.NOTEQUAL,
        1,
        false);

    anomaly = scanStatistics.getInterval(train, monitor);
    if (anomaly != null) {
      Range<Integer> anomalyOffset = Range.closedOpen(anomaly.lowerEndpoint() + split, anomaly.upperEndpoint() + split);
      anomaly_notequal_list.add(anomalyOffset);
      System.out.println("N : " + deseasonal_series.length);
      System.out.println("Split : " + split);
      System.out.println("NOTEQUAL Pattern Anomaly : " + anomalyOffset);
    }

    if (!anomaly_down_list.isEmpty()) {
      System.out.println("N : " + deseasonal_series.length);
      System.out.println("Split : " + split);
      System.out.println("Down Pattern Anomaly : " + anomaly_down_list.get(0));
    }
    if (!anomaly_up_list.isEmpty()) {
      System.out.println("N : " + deseasonal_series.length);
      System.out.println("Split : " + split);
      System.out.println("Up Pattern Anomaly : " + anomaly_up_list.get(0));
    }
    if (!anomaly_notequal_list.isEmpty()) {
      System.out.println("N : " + deseasonal_series.length);
      System.out.println("Split : " + split);
      System.out.println("NotEqual Pattern Anomaly : " + anomaly_notequal_list.get(0));
    }
  }

}
