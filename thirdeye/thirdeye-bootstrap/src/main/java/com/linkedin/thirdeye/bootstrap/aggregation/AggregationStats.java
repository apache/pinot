package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;

/**
 * Class to record stats about the aggregation job
 * @author kgopalak
 */
public class AggregationStats {

  private final MetricSchema schema;
  Map<String, Histogram> perTimeWindowHistogram = new HashMap<String, Histogram>();
  Map<String, Histogram> aggregateHistogram = new HashMap<String, Histogram>();
  String metricNameFormat;
  String metricValueFormat;
  MetricRegistry registry = new MetricRegistry();

  public AggregationStats(MetricSchema schema) {
    this.schema = schema;
    int maxNameLength = "MetricName".length();
    for (int i = 0; i < schema.getNumMetrics(); i++) {
      String metricName = schema.getMetricName(i);
      if (metricName.length() > maxNameLength) {
        maxNameLength = metricName.length();
      }
      Histogram histogram = registry.histogram(metricName);
      perTimeWindowHistogram.put(metricName, histogram);
      Histogram aggHistogram = registry.histogram(metricName);
      aggregateHistogram.put(metricName, aggHistogram);
    }
    metricNameFormat = "%-" + (maxNameLength + 5) + "s";
    metricValueFormat = "%-15s";

  }

  /**
   * Record the output
   * @param series
   */
  public void record(MetricTimeSeries series) {
    long[] sums = new long[schema.getNumMetrics()];
    for (long timeWindow : series.getTimeWindowSet()) {
      for (int i = 0; i < schema.getNumMetrics(); i++) {
        String metricName = schema.getMetricName(i);
        Number number = series.get(timeWindow, metricName);
        sums[i] += number.longValue();
        perTimeWindowHistogram.get(metricName).update(number.longValue());
      }
    }
    for (int i = 0; i < schema.getNumMetrics(); i++) {
      String metricName = schema.getMetricName(i);
      aggregateHistogram.get(metricName).update(sums[i]);
    }
  }

  private static String fields[] = new String[] {
      "75th", "95th", "98th", "99th", "999th", "Max", "Min", "Mean", "Median", "count", "StdDev"
  };

  public String toString() throws RuntimeException {
    StringWriter sw = new StringWriter();
    Map<String, Map<String, Object>> resultPerTimeWindow =
        new HashMap<String, Map<String, Object>>();
    Map<String, Map<String, Object>> resultAggregated = new HashMap<String, Map<String, Object>>();
    sw.append(String.format(metricNameFormat, "MetricName"));
    for (String field : fields) {
      sw.append(String.format(metricValueFormat, field));
    }
    for (int i = 0; i < schema.getNumMetrics(); i++) {
      sw.append("\n");
      String metricName = schema.getMetricName(i);
      Histogram histogram = perTimeWindowHistogram.get(metricName);
      Map<String, Object> map = toMap(histogram);
      sw.append(String.format(metricNameFormat, metricName));
      for (String field : fields) {
        sw.append(String.format(metricValueFormat, map.get(field)));
      }
      resultPerTimeWindow.put(metricName, map);
    }
    for (int i = 0; i < schema.getNumMetrics(); i++) {
      sw.append("\n");
      String metricName = schema.getMetricName(i);
      Histogram histogram = aggregateHistogram.get(metricName);
      Map<String, Object> map = toMap(histogram);
      sw.append(String.format(metricNameFormat, metricName));
      for (String field : fields) {
        sw.append(String.format(metricValueFormat, map.get(field)));
      }
      resultAggregated.put(metricName, map);
    }
    return sw.toString();
  }

  private static DecimalFormat format = new DecimalFormat("#.##");

  private Map<String, Object> toMap(Histogram histogram) {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    Snapshot snapshot = histogram.getSnapshot();
    map.put("75th", format.format(snapshot.get75thPercentile()));
    map.put("95th", format.format(snapshot.get95thPercentile()));
    map.put("98th", format.format(snapshot.get98thPercentile()));
    map.put("99th", format.format(snapshot.get99thPercentile()));
    map.put("999th", format.format(snapshot.get999thPercentile()));
    map.put("Max", format.format(snapshot.getMax()));
    map.put("Min", format.format(snapshot.getMin()));
    map.put("StdDev", format.format(snapshot.getStdDev()));
    map.put("Mean", format.format(snapshot.getMean()));
    map.put("Median", format.format(snapshot.getMedian()));
    map.put("size", format.format(snapshot.size()));
    map.put("count", format.format(histogram.getCount()));
    return map;
  }
}
