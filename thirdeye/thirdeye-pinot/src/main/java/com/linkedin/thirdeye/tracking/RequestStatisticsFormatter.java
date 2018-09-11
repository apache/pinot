/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.tracking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


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
  private static final String FORMAT_DURATION = "  %-50s %10.0f\n";
  private static final String FORMAT_RATE = "  %-50s %10.3f\n";

  public String format(RequestStatistics stats) {
    StringBuilder builder = new StringBuilder();

    builder.append("Request count:\n");
    builder.append(String.format(FORMAT_LONG, "total", stats.requestsTotal));
    builder.append(String.format("datasource (%d):\n", stats.requestsPerDatasource.size()));
    builder.append(format(stats.requestsPerDatasource, COMP_LONG, FORMAT_LONG));
    builder.append(String.format("dataset (%d):\n", stats.requestsPerDataset.size()));
    builder.append(format(stats.requestsPerDataset, COMP_LONG, FORMAT_LONG));
    builder.append(String.format("metric (%d):\n", stats.requestsPerMetric.size()));
    builder.append(format(stats.requestsPerMetric, COMP_LONG, FORMAT_LONG));
    builder.append(String.format("principal (%d):\n", stats.requestsPerPrincipal.size()));
    builder.append(format(stats.requestsPerPrincipal, COMP_LONG, FORMAT_LONG));

    builder.append('\n');
    builder.append("Average duration (in millis):\n");
    builder.append(String.format(FORMAT_DURATION, "total", stats.durationTotal / (double) stats.requestsTotal / 1E6));
    builder.append(String.format("datasource (%d):\n", stats.durationPerDatasource.size()));
    builder.append(format(durationInMs(stats.durationPerDatasource, stats.requestsPerDatasource), COMP_DOUBLE, FORMAT_DURATION));
    builder.append(String.format("dataset (%d):\n", stats.durationPerDataset.size()));
    builder.append(format(durationInMs(stats.durationPerDataset, stats.requestsPerDataset), COMP_DOUBLE, FORMAT_DURATION));
    builder.append(String.format("metric (%d):\n", stats.durationPerMetric.size()));
    builder.append(format(durationInMs(stats.durationPerMetric, stats.requestsPerMetric), COMP_DOUBLE, FORMAT_DURATION));
    builder.append(String.format("principal (%d):\n", stats.durationPerPrincipal.size()));
    builder.append(format(durationInMs(stats.durationPerPrincipal, stats.requestsPerPrincipal), COMP_DOUBLE, FORMAT_DURATION));

    builder.append('\n');
    builder.append("Failure rate:\n");
    builder.append(String.format(FORMAT_RATE, "total", stats.failureTotal / (double) stats.requestsTotal));
    builder.append(String.format("datasource (%d):\n", stats.failurePerDatasource.size()));
    builder.append(format(divide(stats.failurePerDatasource, stats.requestsPerDatasource), COMP_DOUBLE, FORMAT_RATE));
    builder.append(String.format("dataset (%d):\n", stats.failurePerDataset.size()));
    builder.append(format(divide(stats.failurePerDataset, stats.requestsPerDataset), COMP_DOUBLE, FORMAT_RATE));
    builder.append(String.format("metric (%d):\n", stats.failurePerMetric.size()));
    builder.append(format(divide(stats.failurePerMetric, stats.requestsPerMetric), COMP_DOUBLE, FORMAT_RATE));
    builder.append(String.format("principal (%d):\n", stats.failurePerPrincipal.size()));
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
      String key = Objects.toString(entry.getKey());
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
      if (!counts.containsKey(key)) {
        continue;
      }
      out.put(key, durations.get(key) / (double) counts.get(key) / 1E6);
    }
    return out;
  }
}
