package com.linkedin.thirdeye.tracking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RequestStatisticsFormatter {
  private static final int TOP_K = 10;

  private static final Comparator<Map.Entry<String, Long>> COMP_LONG = new Comparator<Map.Entry<String, Long>>() {
    @Override
    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
      return -1 * Long.compare(o1.getValue(), o2.getValue());
    }
  };

  private static final Comparator<Map.Entry<String, Double>> COMP_DOUBLE = new Comparator<Map.Entry<String, Double>>() {
    @Override
    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
      return -1 * Double.compare(o1.getValue(), o2.getValue());
    }
  };

  private static final int MAX_KEY_LEN = 50;
  private static final String FORMAT_LONG = "  %-50s %10d\n";
  private static final String FORMAT_DURATION = "  %-50s %10.1f\n";
  private static final String FORMAT_RATE = "  %-50s %10.3f\n";

  public String format(RequestStatistics stats) {
    StringBuilder builder = new StringBuilder();

    builder.append("Request count:\n");
    builder.append(String.format(FORMAT_LONG, "total", stats.requestsTotal));
    builder.append("datasource:\n");
    builder.append(format(stats.requestsPerDatasource, COMP_LONG, FORMAT_LONG));
    builder.append("dataset:\n");
    builder.append(format(stats.requestsPerDataset, COMP_LONG, FORMAT_LONG));
    builder.append("metric:\n");
    builder.append(format(stats.requestsPerMetric, COMP_LONG, FORMAT_LONG));
    builder.append("principal:\n");
    builder.append(format(stats.requestsPerPrincipal, COMP_LONG, FORMAT_LONG));

    builder.append('\n');
    builder.append("Average duration (ms):\n");
    builder.append(String.format(FORMAT_DURATION, "total", stats.durationTotal / (double) stats.requestsTotal / 1E6));
    builder.append("datasource:\n");
    builder.append(format(durationInMs(stats.durationPerDatasource, stats.requestsPerDatasource), COMP_DOUBLE, FORMAT_DURATION));
    builder.append("dataset:\n");
    builder.append(format(durationInMs(stats.durationPerDataset, stats.requestsPerDataset), COMP_DOUBLE, FORMAT_DURATION));
    builder.append("metric:\n");
    builder.append(format(durationInMs(stats.durationPerMetric, stats.requestsPerMetric), COMP_DOUBLE, FORMAT_DURATION));
    builder.append("principal:\n");
    builder.append(format(durationInMs(stats.durationPerPrincipal, stats.requestsPerPrincipal), COMP_DOUBLE, FORMAT_DURATION));

    builder.append('\n');
    builder.append("Failure rate:\n");
    builder.append(String.format(FORMAT_RATE, "total", stats.failureTotal / (double) stats.requestsTotal));
    builder.append("datasource:\n");
    builder.append(format(divide(stats.failurePerDatasource, stats.requestsPerDatasource), COMP_DOUBLE, FORMAT_RATE));
    builder.append("dataset:\n");
    builder.append(format(divide(stats.failurePerDataset, stats.requestsPerDataset), COMP_DOUBLE, FORMAT_RATE));
    builder.append("metric:\n");
    builder.append(format(divide(stats.failurePerMetric, stats.requestsPerMetric), COMP_DOUBLE, FORMAT_RATE));
    builder.append("principal:\n");
    builder.append(format(divide(stats.failurePerPrincipal, stats.requestsPerPrincipal), COMP_DOUBLE, FORMAT_RATE));

    return builder.toString();
  }

  private <K, V> List<Map.Entry<K, V>> topk(Map<K, V> map, Comparator<Map.Entry<K, V>> comparator) {
    List<Map.Entry<K, V>> entries = new ArrayList<>(map.entrySet());
    Collections.sort(entries, comparator);
    return entries.subList(0, Math.min(entries.size(), TOP_K));
  }

  private <K, V> String format(Map<K, V> map, Comparator<Map.Entry<K, V>> comparator, String format) {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<K, V> entry : topk(map, comparator)) {
      String key = entry.getKey().toString();
      if (key.length() > MAX_KEY_LEN) {
        key = key.substring(0, MAX_KEY_LEN - 3) + "...";
      }
      builder.append(String.format(format, key, entry.getValue()));
    }
    return builder.toString();
  }

  private Map<String, Double> divide(Map<String, Long> a, Map<String, Long> b) {
    Map<String, Double> out = new HashMap<>();
    for (String key : a.keySet()) {
      if (!b.containsKey(key)) {
        continue;
      }
      out.put(key, a.get(key) / (double) b.get(key));
    }
    return out;
  }

  private Map<String, Double> durationInMs(Map<String, Long> durations, Map<String, Long> counts) {
    Map<String, Double> out = new HashMap<>();
    for (String key : durations.keySet()) {
      if (!durations.containsKey(key)) {
        continue;
      }
      out.put(key, counts.get(key) / (double) counts.get(key) / 1E6);
    }
    return out;
  }
}
