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

import com.linkedin.thirdeye.anomaly.util.ResourceUtils;
import com.linkedin.thirdeye.anomaly.builtin.ScanStatisticsAnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.lib.scanstatistics.STLTest;

public class ScanRemoveAnomalyTest {
  @Test
  public void ScanRemoveAnomalyTest() throws IOException {
    //SequentialMonitoringInputOne("timeseries.csv", "output1.csv", 720, 72, 200, ScanStatistics.Pattern.UP,
    //    ScanStatistics.Pattern.DOWN, 1.0, 1.0);
  // SequentialMonitoringInputTwo("caseStudyOne.csv", "output2.csv", 720, 72, 24, 200, ScanStatistics.Pattern.DOWN,
       // ScanStatistics.Pattern.UP, 1.0, 1.0);
   //SequentialMonitoringSingleSideInputOne("RcaseOne.csv", "RseasonalOutputOne.csv", "RoutputOne.csv", 648, 72, 24, 500,
   //     ScanStatistics.Pattern.DOWN, 1.0);
   SequentialMonitoringSingleSideInputTwo("debug1_deseasonal.csv",  "RoutputTwo.csv", 648, 72, 24, 500,
       ScanStatistics.Pattern.DOWN, 1.0);
    //NoHistoryAnomaliesRemoved("timeseries.csv", 720, 200);

  }

  public void SequentialMonitoringSingleSideInputTwo(
      String deSeasonalInputFilename,
      String outputFilename,
      int training_size,
      int monitor_size,
      int scan_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      double target_level) throws IOException {
    int[] columns = {1};
    double[] series = GetData(deSeasonalInputFilename, columns);
    String[] inputTimestamps = GetTimestamp(deSeasonalInputFilename, 0);
    long[] timestamps = new long[series.length];
    for (int ii = 0; ii < series.length; ii++) {
      timestamps[ii] = ii;
    }
    SequentialMonitoringCompareWithR(series, inputTimestamps, outputFilename,
        training_size, monitor_size,
        scan_size, numSims, target_pattern,target_level);
  }

  public void SequentialMonitoringSingleSideInputOne(
      String filename,
      String seasonalOutputFilename,
      String outputFilename,
      int training_size,
      int monitor_size,
      int scan_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      double target_level) throws IOException {
    int[] columns = {3};
    double[] series = GetData(filename, columns);
    String[] inputTimestamps = GetTimestamp(filename, 1);
    double[] deseasonal_series = new double[series.length];
    long[] timestamps = new long[series.length];
    for (int ii = 0; ii < series.length; ii++) {
      timestamps[ii] = ii;
      if (series[ii] == 0) {
        series[ii] = Double.NaN;
        System.out.println(ii);
      } else {
        series[ii] = Math.log(series[ii]);

      }
    }
    ScanStatisticsAnomalyDetectionFunction.removeMissingValuesByAveragingNeighbors(series);

    //double[][] data = STLTest.removeSeasonality(timestamps, series, 168, 1, 15, 0.75, 0.10, true);

    double[][] data = STLTest.removeSeasonality(timestamps, series, 168, 15, 1, 0.75, 0.10, true);
    double[] data_trend = data[0];
    double[] data_remainder = data[1];
    for (int ii = 0; ii < series.length; ii++) {
      deseasonal_series[ii] = data_trend[ii] + data_remainder[ii];
    }
    System.out.println("seasonality removed." + series.length);

    File file = new File(seasonalOutputFilename);
    OutputStream fop = new FileOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fop));
    for (double element : deseasonal_series) {
      writer.write(Double.toString(element));
      writer.newLine();
    }
    writer.close();

    SequentialMonitoringCompareWithR(series, inputTimestamps, outputFilename,
        training_size, monitor_size,
        scan_size, numSims, target_pattern,target_level);

  }

  String[] GetTimestamp(String filename, int columnIdx) throws IOException {
      String[] lines = ResourceUtils.getResourceAsString(filename).split("\n");
      int numData = lines.length;
      String[] inputTimestamp= new String[numData];
      for (int i = 0; i < numData; i++) {
        String[] values = lines[i].split(",");
        inputTimestamp[i] = values[columnIdx];
      }
      return inputTimestamp;
    }



  double[] GetData(String filename, int[] columnIdx) throws IOException {
    String[] lines = ResourceUtils.getResourceAsString(filename).split("\n");
    int numData = lines.length;
    double[] deseasonal_series = new double[numData];
    for (int i = 0; i < numData; i++) {
      String[] values = lines[i].split(",");
      deseasonal_series[i] = 0;
      for (int idx =0; idx < columnIdx.length; idx++) {
        deseasonal_series[i] += Double.valueOf(values[columnIdx[idx]]);
        //deseasonal_series[i] = Double.valueOf(values[3]) + Double.valueOf(values[2]);

      }
    }
    return deseasonal_series;
  }



  public void SequentialMonitoringInputOne(String filename, String outputFilename,
      int training_size,
      int monitor_size,
      int scan_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      ScanStatistics.Pattern complementary_pattern,
      double target_level, double complementary_level) throws IOException {
    int[] columns = {2, 3};
    double[] deseasonal_series = GetData(filename, columns);
    SequentialMonitoring(deseasonal_series, outputFilename, training_size, monitor_size,
        scan_size, numSims, target_pattern,
        complementary_pattern, target_level, complementary_level);
  }

  public void SequentialMonitoringInputTwo(String filename, String outputFilename,
      int training_size,
      int monitor_size,
      int scan_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      ScanStatistics.Pattern complementary_pattern,
      double target_level, double complementary_level) throws IOException {
    int[] columns = {1};
    double[] series = GetData(filename, columns);
    double[] deseasonal_series = new double[series.length];
    long[] timestamps = new long[series.length];
    for (int ii = 0; ii < series.length; ii++) {
      timestamps[ii] = ii;
    }
    ScanStatisticsAnomalyDetectionFunction.removeMissingValuesByAveragingNeighbors(series);

    double[][] data = STLTest.removeSeasonality(timestamps, series, 168, 1, 15, 0.75, 0.10, true);
    double[] data_trend = data[0];
    double[] data_remainder = data[1];
    for (int ii = 0; ii < series.length; ii++) {
      deseasonal_series[ii] = data_trend[ii] + data_remainder[ii];
    }

    System.out.println("seasonality removed." + series.length);

    File file = new File("deseasonal.csv");
    OutputStream fop = new FileOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fop));
    for (double element : deseasonal_series) {
      writer.write(Double.toString(element));
      writer.newLine();
    }
    writer.close();

    SequentialMonitoring(deseasonal_series, outputFilename, training_size, monitor_size, scan_size, numSims, target_pattern,
        complementary_pattern, target_level, complementary_level);

  }

  private void SequentialMonitoringCompareWithR(double[] series,
      String[] inputTimestamps,
      String outputFilename,
      int training_size,
      int monitor_size,
      int scan_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      double target_level) throws IOException {
    int numData = series.length;
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

    long[] timestamps = new long[numData];
    for (int i = 0; i < numData; i++) {
      timestamps[i] = i;
    }
    int monitor_start = training_size;
    int monitor_end = series.length - scan_size;
    Set<Long> targetAnomalousTimestamps = new HashSet<Long>();
    System.out.println(monitor_start + "," + monitor_end +"," + series.length);

    ArrayList<String> output_list = new ArrayList<String>();
    /*double[][] data = STLTest.removeSeasonality(
        Arrays.copyOfRange(timestamps, 0, training_size),
        Arrays.copyOfRange(series, 0, training_size),
        168, 15, 1, 0.75, 0.10, true);
    double[] data_seasonal = data[2];

    File file = new File("deseasonal_compare_with_R.csv");
    OutputStream fop = new FileOutputStream(file);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fop));
    for (double element : data_seasonal) {
      writer.write(Double.toString(element));
      writer.newLine();
    }
    writer.close();*/

    int training_size_for_scan = training_size - monitor_size + scan_size;
    for (int monitor_idx=monitor_start; monitor_idx <= monitor_end; monitor_idx+=scan_size) {

      int current_monitor_start = 0;
      int current_monitor_end = 0;
      int current_train_start = 0;
      int current_train_end = 0;

      current_monitor_end = monitor_idx + scan_size;
      current_monitor_start = current_monitor_end - monitor_size;
      current_train_end = current_monitor_start;
      current_train_start = current_train_end - training_size_for_scan;
      System.out.println(monitor_idx + "," + inputTimestamps[current_monitor_start] + "," +
          inputTimestamps[current_monitor_end-1]);

      double[] train = Arrays.copyOfRange(series, current_train_start, current_train_end);
      double[] monitor = Arrays.copyOfRange(series, current_monitor_start, current_monitor_end);
     /* double[] train = new double[training_size_for_scan];
      double[] monitor = new double[monitor_size];
      for (int ii = current_train_start; ii < current_train_end; ii++) {
          if (ii < training_size) {
            System.out.println("train"+inputTimestamps[ii]+","+data_seasonal[ii]+","+series[ii]);
            train[ii-current_train_start] = series[ii] - data_seasonal[ii];
          } else {
            int tmp_idx = ii - 168;
            while (tmp_idx >= training_size) {
              tmp_idx = tmp_idx- 168;
            }
            System.out.println("train"+inputTimestamps[ii]+","+ inputTimestamps[tmp_idx] +"," +data_seasonal[tmp_idx]+
                "," + series[ii]);
            train[ii-current_train_start] = series[ii] - data_seasonal[tmp_idx];
          }
      } */
    /*  if (monitor_idx==monitor_start) {
        int count = 0;
        for (double element : train) {
          System.out.println(count +","+element);
          count += 1;
        }
      }*/

     /* for (int ii = current_monitor_start; ii < current_monitor_end; ii++) {
        if (ii < training_size) {
          System.out.println("monitor"+inputTimestamps[ii]+","+data_seasonal[ii]+","+series[ii]);
          monitor[ii-current_monitor_start] = series[ii] - data_seasonal[ii];
        } else {
          int tmp_idx = ii - 168;
          while (tmp_idx >= training_size) {
            tmp_idx = tmp_idx- 168;
          }
          System.out.println("monitor"+inputTimestamps[ii]+","+ inputTimestamps[tmp_idx] +"," +data_seasonal[tmp_idx]+
              "," + series[ii]);
          monitor[ii-current_monitor_start] = series[ii] - data_seasonal[tmp_idx];
        }
      }*/

      long[] trainingTimestamps = Arrays.copyOfRange(timestamps, current_train_start, current_train_end);
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

    double[] trainingDataWithOutTargetAnomalies = ScanStatisticsAnomalyDetectionFunction.removeAnomalies(
        trainingTimestamps, train, targetAnomalousTimestamps);

    Range<Integer> target_anomaly = scanStatistics.getInterval(
        trainingDataWithOutTargetAnomalies, monitor);
    if (target_anomaly != null) {
      for (int i = target_anomaly.lowerEndpoint(); i < target_anomaly.upperEndpoint(); i++) {
        targetAnomalousTimestamps.add((long)(i+current_monitor_start));
      }
      Range<Integer> anomalyOffset = Range.closedOpen(target_anomaly.lowerEndpoint() + current_monitor_start,
          target_anomaly.upperEndpoint() + current_monitor_start);
      output_list.add(anomalyOffset.lowerEndpoint() + "," +
          inputTimestamps[anomalyOffset.lowerEndpoint()] + "," +
          (anomalyOffset.upperEndpoint() -1)+"," +
          inputTimestamps[anomalyOffset.upperEndpoint()-1] + "," +
          current_monitor_start + "," +
          inputTimestamps[current_monitor_start] + "," +
          (current_monitor_end - 1) + "," +
          inputTimestamps[current_monitor_end - 1]);
      System.out.println(inputTimestamps[anomalyOffset.lowerEndpoint()] + "," + inputTimestamps[anomalyOffset.upperEndpoint()-1]);
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

  private void SequentialMonitoringReplay(double[] series,
      String[] inputTimestamps,
      String outputFilename,
      int training_size,
      int monitor_size,
      int scan_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      double target_level) throws IOException {
    int numData = series.length;
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

    long[] timestamps = new long[numData];
    for (int i = 0; i < numData; i++) {
      timestamps[i] = i;
    }
    int monitor_start = training_size;
    int monitor_end = series.length - monitor_size;
    Set<Long> targetAnomalousTimestamps = new HashSet<Long>();

    ArrayList<String> output_list = new ArrayList<String>();

    for (int monitor_idx=monitor_start; monitor_idx <= monitor_end; monitor_idx+=scan_size) {
      System.out.println(monitor_idx);
      double[] training_before_deseason_series = Arrays.copyOfRange(series, monitor_idx-training_size, monitor_idx);
      double[] train = new double[training_size];
      double[][] data = STLTest.removeSeasonality(
          Arrays.copyOfRange(timestamps, monitor_idx-training_size, monitor_idx),
          training_before_deseason_series, 168, 15, 1, 0.75, 0.10, true);
      double[] data_trend = data[0];
      double[] data_remainder = data[1];
      double[] data_seasonal = data[2];
      double[] monitor = Arrays.copyOfRange(series, monitor_idx, monitor_idx + monitor_size);
      for (int ii = 0; ii < training_size; ii++) {
        train[ii] = data_trend[ii] + data_remainder[ii];
      }
      for (int ii = 0; ii < monitor_size; ii++) {
        int tmp_idx = training_size + ii - 168;
        while (tmp_idx >= training_size) {
          tmp_idx = tmp_idx - 168;
        }
        monitor[ii] = monitor[ii] - data_seasonal[tmp_idx];
      }

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

    double[] trainingDataWithOutTargetAnomalies = ScanStatisticsAnomalyDetectionFunction.removeAnomalies(
        trainingTimestamps, train, targetAnomalousTimestamps);

    Range<Integer> target_anomaly = scanStatistics.getInterval(
        trainingDataWithOutTargetAnomalies, monitor);
    if (target_anomaly != null) {
      for (int i = target_anomaly.lowerEndpoint(); i < target_anomaly.upperEndpoint(); i++) {
        targetAnomalousTimestamps.add((long)(i+monitor_idx));
      }
      Range<Integer> anomalyOffset = Range.closedOpen(target_anomaly.lowerEndpoint() + monitor_idx,
          target_anomaly.upperEndpoint() + monitor_idx);
      output_list.add(anomalyOffset.lowerEndpoint() + "," +
          inputTimestamps[anomalyOffset.lowerEndpoint()] + "," +
          anomalyOffset.upperEndpoint() +"," +
          inputTimestamps[anomalyOffset.upperEndpoint()] + "," +
          monitor_idx + "," +
          inputTimestamps[monitor_idx] + "," +
          (monitor_idx+monitor_size-1) + "," +
          inputTimestamps[monitor_idx+monitor_size-1]);
      System.out.println(inputTimestamps[anomalyOffset.lowerEndpoint()] + "," + inputTimestamps[anomalyOffset.upperEndpoint()]);
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


  private void SequentialMonitoring(double[] deseasonal_series, String outputFilename, int training_size,
      int monitor_size,
      int scan_size,
      int numSims,
      ScanStatistics.Pattern target_pattern,
      ScanStatistics.Pattern complementary_pattern,
      double target_level, double complementary_level) throws IOException {
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
      System.out.println(monitor_idx);
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
      //complementary_anomaly = null;

      if (target_anomaly != null) {
        for (int i = target_anomaly.lowerEndpoint(); i < target_anomaly.upperEndpoint(); i++) {
          aggregateAnomalousTimestamps.add((long)(i+monitor_idx));
          targetAnomalousTimestamps.add((long)(i+monitor_idx));
        }
        Range<Integer> anomalyOffset = Range.closedOpen(target_anomaly.lowerEndpoint() + monitor_idx,
            target_anomaly.upperEndpoint() + monitor_idx);
        output_list.add("Split : " + monitor_idx + ",Target Pattern Anomaly : " + anomalyOffset);
        System.out.println(anomalyOffset.lowerEndpoint() + "," + anomalyOffset.upperEndpoint() +"," );
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
    int[] columns = {2, 3};
    double[] deseasonal_series = GetData(filename, columns);
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
