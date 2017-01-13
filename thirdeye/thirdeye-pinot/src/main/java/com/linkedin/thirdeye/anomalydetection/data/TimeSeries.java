package com.linkedin.thirdeye.anomalydetection.data;

import java.util.ArrayList;

public class TimeSeries {
  TimeSeriesKey key;
  ArrayList<Long> timestamps;
  ArrayList<Double> dataPoints; // Double.Nan means empty data point
}
