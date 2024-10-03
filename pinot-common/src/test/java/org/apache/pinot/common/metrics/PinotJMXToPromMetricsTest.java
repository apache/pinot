/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.common.metrics;

import com.google.common.base.Objects;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;


public class PinotJMXToPromMetricsTest {

  protected HttpClient _httpClient;

  protected static final List<String> METER_TYPES =
      List.of("Count", "FiveMinuteRate", "MeanRate", "OneMinuteRate", "FifteenMinuteRate");

  protected static final List<String> TIMER_TYPES =
      List.of("Count", "FiveMinuteRate", "Max", "999thPercentile", "95thPercentile", "75thPercentile", "98thPercentile",
          "OneMinuteRate", "50thPercentile", "99thPercentile", "FifteenMinuteRate", "Mean", "StdDev", "MeanRate",
          "Min");

  protected static final String RAW_TABLE_NAME = "myTable";
  protected static final String TABLE_NAME_WITH_TYPE =
      TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(RAW_TABLE_NAME);

  protected static final String KAFKA_TOPIC = "myTopic";
  protected static final String PARTITION_GROUP_ID = "partitionGroupId";
  protected static final String CLIENT_ID =
      String.format("%s-%s-%s", TABLE_NAME_WITH_TYPE, KAFKA_TOPIC, PARTITION_GROUP_ID);
  protected static final String TABLE_STREAM_NAME = String.format("%s_%s", TABLE_NAME_WITH_TYPE, KAFKA_TOPIC);

  protected static final List<String> EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE =
      List.of("table", "myTable", "tableType", "REALTIME");

  protected static final List<String> EXPORTED_LABELS_FOR_CLIENT_ID =
      List.of("partition", PARTITION_GROUP_ID, "table", RAW_TABLE_NAME, "tableType", TableType.REALTIME.toString(),
          "topic", KAFKA_TOPIC);

  protected void assertGaugeExportedCorrectly(String exportedGaugePrefix, String exportedMetricPrefix)
      throws IOException, URISyntaxException {
    List<ServerJMXToPromMetricsTest.PromMetric> promMetrics =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertTrue(promMetrics.contains(
        ServerJMXToPromMetricsTest.PromMetric.withName(exportedMetricPrefix + exportedGaugePrefix + "_" + "Value")));
  }

  protected void assertGaugeExportedCorrectly(String exportedGaugePrefix, List<String> labels,
      String exportedMetricPrefix)
      throws IOException, URISyntaxException {
    List<ServerJMXToPromMetricsTest.PromMetric> promMetrics =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertTrue(promMetrics.contains(ServerJMXToPromMetricsTest.PromMetric.withNameAndLabels(
        exportedMetricPrefix + exportedGaugePrefix + "_" + "Value", labels)));
  }

  protected void assertTimerExportedCorrectly(String exportedTimerPrefix, String exportedMetricPrefix)
      throws IOException, URISyntaxException {
    List<ServerJMXToPromMetricsTest.PromMetric> promMetrics =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : TIMER_TYPES) {
      Assert.assertTrue(promMetrics.contains(ServerJMXToPromMetricsTest.PromMetric.withName(
          exportedMetricPrefix + exportedTimerPrefix + "_" + meterType)));
    }
  }

  protected void assertTimerExportedCorrectly(String exportedTimerPrefix, List<String> labels,
      String exportedMetricPrefix)
      throws IOException, URISyntaxException {
    List<ServerJMXToPromMetricsTest.PromMetric> promMetrics =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(promMetrics.contains(ServerJMXToPromMetricsTest.PromMetric.withNameAndLabels(
          exportedMetricPrefix + exportedTimerPrefix + "_" + meterType, labels)));
    }
  }

  protected void assertMeterExportedCorrectly(String exportedMeterPrefix, String exportedMetricPrefix)
      throws IOException, URISyntaxException {
    List<ServerJMXToPromMetricsTest.PromMetric> promMetrics =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(promMetrics.contains(ServerJMXToPromMetricsTest.PromMetric.withName(
          exportedMetricPrefix + exportedMeterPrefix + "_" + meterType)));
    }
  }

  protected void assertMeterExportedCorrectly(String exportedMeterPrefix, List<String> labels,
      String exportedMetricPrefix)
      throws IOException, URISyntaxException {
    List<ServerJMXToPromMetricsTest.PromMetric> promMetrics =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(promMetrics.contains(ServerJMXToPromMetricsTest.PromMetric.withNameAndLabels(
          exportedMetricPrefix + exportedMeterPrefix + "_" + meterType, labels)));
    }
  }

  protected List<ServerJMXToPromMetricsTest.PromMetric> parseExportedPromMetrics(String response)
      throws IOException {

    List<ServerJMXToPromMetricsTest.PromMetric> exportedPromMetrics = new ArrayList<>();

    BufferedReader reader = new BufferedReader(new StringReader(response));

    String line;
    while ((line = reader.readLine()) != null) {
      if (line.startsWith("pinot_")) {
        exportedPromMetrics.add(ServerJMXToPromMetricsTest.PromMetric.fromExportedMetric(line));
      }
    }
    reader.close();
    return exportedPromMetrics;
  }

  protected SimpleHttpResponse getExportedPromMetrics()
      throws IOException, URISyntaxException {
    return _httpClient.sendGetRequest(new URI("http://localhost:9021/metrics"));
  }

  public static class PromMetric {
    private final String _metricName;
    private final Map<String, String> _labels;

    public String getMetricName() {
      return _metricName;
    }

    public Map<String, String> getLabels() {
      return _labels;
    }

    private PromMetric(String metricName, Map<String, String> labels) {
      _metricName = metricName;
      _labels = labels;
    }

    public static PromMetric fromExportedMetric(String exportedMetric) {
      int spaceIndex = exportedMetric.indexOf(' ');
      String metricWithoutVal = exportedMetric.substring(0, spaceIndex);
      int braceIndex = metricWithoutVal.indexOf('{');

      if (braceIndex != -1) {
        String metricName = metricWithoutVal.substring(0, braceIndex);
        String labelsString = metricWithoutVal.substring(braceIndex + 1, metricWithoutVal.lastIndexOf('}'));
        Map<String, String> labels = parseLabels(labelsString);
        return new PromMetric(metricName, labels);
      } else {
        return new PromMetric(metricWithoutVal, new LinkedHashMap<>());
      }
    }

    private static Map<String, String> parseLabels(String labelsString) {
      return labelsString.isEmpty() ? new LinkedHashMap<>()
          : java.util.Arrays.stream(labelsString.split(",")).map(kvPair -> kvPair.split("="))
              .collect(Collectors.toMap(kv -> kv[0], kv -> removeQuotes(kv[1]), (v1, v2) -> v2, LinkedHashMap::new));
    }

    private static String removeQuotes(String value) {
      return value.startsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    public static PromMetric withName(String metricName) {
      return new PromMetric(metricName, new LinkedHashMap<>());
    }

    public static PromMetric withNameAndLabels(String metricName, List<String> labels) {
      Map<String, String> labelMap = new LinkedHashMap<>();
      for (int i = 0; i < labels.size(); i += 2) {
        labelMap.put(labels.get(i), labels.get(i + 1));
      }
      return new PromMetric(metricName, labelMap);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PromMetric that = (PromMetric) o;
      return Objects.equal(_metricName, that._metricName) && Objects.equal(_labels, that._labels);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(_metricName, _labels);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(_metricName);
      if (!_labels.isEmpty()) {
        sb.append('{');
        sb.append(_labels.entrySet().stream().map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
            .collect(Collectors.joining(",")));
        sb.append('}');
      }
      return sb.toString();
    }
  }
}
