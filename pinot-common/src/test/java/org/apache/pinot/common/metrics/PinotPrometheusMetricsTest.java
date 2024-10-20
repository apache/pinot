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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Objects;
import com.google.common.io.Resources;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.jmx.common.http.HTTPServerFactory;
import io.prometheus.jmx.shaded.io.prometheus.client.CollectorRegistry;
import io.prometheus.jmx.shaded.io.prometheus.client.exporter.HTTPServer;
import io.prometheus.jmx.shaded.io.prometheus.client.hotspot.DefaultExports;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import static org.apache.pinot.common.metrics.PinotPrometheusMetricsTest.ExportedLabelKeys.*;
import static org.apache.pinot.common.metrics.PinotPrometheusMetricsTest.ExportedLabelValues.*;
import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public abstract class PinotPrometheusMetricsTest {

  //this dir contains the JMX exporter configs for each Pinot component (broker, server, controller, minion)
  private static final String JMX_EXPORTER_CONFIG_PARENT_DIR =
      "../docker/images/pinot/etc/jmx_prometheus_javaagent/configs";

  //this map is a mapping of pinot components to their JMX exporter config files. They can be found at:
  // docker/images/pinot/etc/jmx_prometheus_javaagent/configs
  private static final Map<PinotComponent, String> PINOT_COMPONENT_CONFIG_FILE_MAP =
      Map.of(PinotComponent.CONTROLLER, "controller.yml", PinotComponent.SERVER, "server.yml", PinotComponent.MINION,
          "minion.yml", PinotComponent.BROKER, "broker.yml");
  protected HttpClient _httpClient;
  protected static final List<String> METER_TYPES =
      List.of("Count", "FiveMinuteRate", "MeanRate", "OneMinuteRate", "FifteenMinuteRate");

  protected static final List<String> TIMER_TYPES =
      List.of("Count", "FiveMinuteRate", "Max", "999thPercentile", "95thPercentile", "75thPercentile", "98thPercentile",
          "OneMinuteRate", "50thPercentile", "99thPercentile", "FifteenMinuteRate", "Mean", "StdDev", "MeanRate",
          "Min");

  protected static final String TABLE_NAME_WITH_TYPE =
      TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(ExportedLabelValues.TABLENAME);

  protected static final String KAFKA_TOPIC = "myTopic";
  protected static final String PARTITION_GROUP_ID = "partitionGroupId";
  protected static final String CLIENT_ID =
      String.format("%s-%s-%s", TABLE_NAME_WITH_TYPE, KAFKA_TOPIC, PARTITION_GROUP_ID);

  private PinotMetricsFactory _pinotMetricsFactory;

  @BeforeClass
  public void setupBase()
      throws Exception {
    JsonNode jsonNode = JsonUtils.DEFAULT_READER.readTree(loadResourceAsString("metrics/testConfig.json"));
    String pinotMetricsFactory = jsonNode.get("pinotMetricsFactory").toString();
    switch (pinotMetricsFactory) {
      case "\"YammerMetricsFactory\"":
        _pinotMetricsFactory = new YammerMetricsFactory();
        break;
      case "\"DropwizardMetricsFactory\"":
        _pinotMetricsFactory = new DropwizardMetricsFactory();
        break;
      default:
        throw new IllegalArgumentException("Unknow metrics factory specified in test config: " + pinotMetricsFactory
            + ", supported ones are: YammerMetricsFactory and DropwizardMetricsFactory");
    }
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, _pinotMetricsFactory.getClass().getCanonicalName());
    PinotMetricUtils.init(pinotConfiguration);

    _pinotMetricsFactory.makePinotJmxReporter(_pinotMetricsFactory.getPinotMetricsRegistry()).start();
  }

  private String loadResourceAsString(String resourceFileName) {
    URL url = Resources.getResource(resourceFileName);
    try {
      return Resources.toString(url, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @DataProvider
  public Object[][] configs()
      throws IOException {
    try (Stream<Path> configs = Files.list(Paths.get("src/test/resources/testConfigs/testConfig.json"))) {
      return configs.map(path -> {
        try {
          return Files.readAllBytes(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).map(config -> new Object[]{config}).toArray(Object[][]::new);
    }
  }

  /**
   * For impl, see:
   * <a href="https://github.com/prometheus/jmx_exporter/blob/a3b9443564ff5a78c25fd6566396fda2b7cbf216">...</a>
   * /jmx_prometheus_javaagent/src/main/java/io/prometheus/jmx/JavaAgent.java#L48
   * @param pinotComponent the Pinot component to start the server for
   * @return the corresponding HTTP server on a random unoccupied port
   */
  protected HTTPServer startExporter(PinotComponent pinotComponent) {
    String args = String.format("%s:%s/%s", 0, JMX_EXPORTER_CONFIG_PARENT_DIR,
        PINOT_COMPONENT_CONFIG_FILE_MAP.get(pinotComponent));
    try {
      JMXExporterConfig config = parseExporterConfig(args, "0.0.0.0");
      CollectorRegistry registry = new CollectorRegistry();
      JmxCollector jmxCollector = new JmxCollector(new File(config._file), JmxCollector.Mode.AGENT);
      jmxCollector.register(registry);
      DefaultExports.register(registry);
      return (new HTTPServerFactory()).createHTTPServer(config._socket, registry, true, new File(config._file));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertGaugeExportedCorrectly(String exportedGaugePrefix, String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
      PromMetric expectedMetric = PromMetric.withName(exportedMetricPrefix + exportedGaugePrefix + "_" + "Value");
      Assert.assertTrue(promMetrics.contains(expectedMetric),
          "Cannot find gauge: " + expectedMetric + " in exported metrics");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertGaugeExportedCorrectly(String exportedGaugePrefix, List<String> labels,
      String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
      PromMetric expectedGauge =
          PromMetric.withNameAndLabels(exportedMetricPrefix + exportedGaugePrefix + "_" + "Value", labels);
      Assert.assertTrue(promMetrics.contains(expectedGauge),
          "Cannot find gauge: " + expectedGauge + " in exported metrics");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertTimerExportedCorrectly(String exportedTimerPrefix, String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
      for (String meterType : TIMER_TYPES) {
        PromMetric expectedTimer = PromMetric.withName(exportedMetricPrefix + exportedTimerPrefix + "_" + meterType);
        Assert.assertTrue(promMetrics.contains(expectedTimer),
            "Cannot find timer: " + expectedTimer + " in exported metrics");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertTimerExportedCorrectly(String exportedTimerPrefix, List<String> labels,
      String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
      for (String meterType : METER_TYPES) {
        Assert.assertTrue(promMetrics.contains(
                PromMetric.withNameAndLabels(exportedMetricPrefix + exportedTimerPrefix + "_" + meterType, labels)),
            exportedTimerPrefix);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertMeterExportedCorrectly(String exportedMeterPrefix, String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    for (String meterType : METER_TYPES) {
      PromMetric expectedMetric = PromMetric.withName(exportedMetricPrefix + exportedMeterPrefix + "_" + meterType);
      Assert.assertTrue(promMetrics.contains(expectedMetric),
          "Cannot find metric: " + expectedMetric + " in the exported metrics");
    }
  }

  protected void assertMeterExportedCorrectly(String exportedMeterPrefix, List<String> labels,
      String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
      for (String meterType : METER_TYPES) {
        PromMetric expectedMetric =
            PromMetric.withNameAndLabels(exportedMetricPrefix + exportedMeterPrefix + "_" + meterType, labels);
        Assert.assertTrue(promMetrics.contains(expectedMetric),
            "Cannot find metric: " + expectedMetric + " in the exported metrics");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected List<PromMetric> parseExportedPromMetrics(String response)
      throws IOException {

    List<PromMetric> exportedPromMetrics = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("pinot_")) {
          exportedPromMetrics.add(PromMetric.fromExportedMetric(line));
        }
      }
      reader.close();
      return exportedPromMetrics;
    }
  }

  protected abstract SimpleHttpResponse getExportedPromMetrics();

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
      return metricNamesAreSimilar(that) && Objects.equal(_labels, that._labels);
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

    private boolean metricNamesAreSimilar(PromMetric that) {
      String processedMetricNameThis = StringUtils.remove(_metricName, "_");
      String processedMetricNameThat = StringUtils.remove(that._metricName, "_");
      return StringUtils.equalsIgnoreCase(processedMetricNameThis, processedMetricNameThat);
    }

    private static Map<String, String> parseLabels(String labelsString) {
      return labelsString.isEmpty() ? new LinkedHashMap<>()
          : java.util.Arrays.stream(labelsString.split(",")).map(kvPair -> kvPair.split("="))
              .collect(Collectors.toMap(kv -> kv[0], kv -> removeQuotes(kv[1]), (v1, v2) -> v2, LinkedHashMap::new));
    }

    private static String removeQuotes(String value) {
      return value.startsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }
  }

  /*
  Implementation copied from: https://github
  .com/prometheus/jmx_exporter/blob/a3b9443564ff5a78c25fd6566396fda2b7cbf216/jmx_prometheus_javaagent/src/main/java
  /io/prometheus/jmx/JavaAgent.java#L88
   */
  private static JMXExporterConfig parseExporterConfig(String args, String ifc) {
    Pattern pattern = Pattern.compile("^(?:((?:[\\w.-]+)|(?:\\[.+])):)?(\\d{1,5}):(.+)");
    Matcher matcher = pattern.matcher(args);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Malformed arguments - " + args);
    } else {
      String givenHost = matcher.group(1);
      String givenPort = matcher.group(2);
      String givenConfigFile = matcher.group(3);
      int port = Integer.parseInt(givenPort);
      InetSocketAddress socket;
      if (givenHost != null && !givenHost.isEmpty()) {
        socket = new InetSocketAddress(givenHost, port);
      } else {
        socket = new InetSocketAddress(ifc, port);
        givenHost = ifc;
      }

      return new JMXExporterConfig(givenHost, port, givenConfigFile, socket);
    }
  }

  static class JMXExporterConfig {
    String _host;
    int _port;
    String _file;
    InetSocketAddress _socket;

    JMXExporterConfig(String host, int port, String file, InetSocketAddress socket) {
      _host = host;
      _port = port;
      _file = file;
      _socket = socket;
    }
  }

  public enum PinotComponent {
    SERVER, BROKER, CONTROLLER, MINION
  }

  public static class ExportedLabels {
    public static final List<String> TABLENAME_TABLETYPE =
        List.of(TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TABLETYPE_REALTIME);

    public static final List<String> TABLENAME = List.of(TABLE, ExportedLabelValues.TABLENAME);

    public static final List<String> CLIENT_ID =
        List.of(PARTITION, PARTITION_GROUP_ID, TABLE, ExportedLabelValues.TABLENAME, TABLETYPE,
            TableType.REALTIME.toString(), TOPIC, KAFKA_TOPIC);

    public static final List<String> PARTITION_TABLE_NAME_AND_TYPE =
        List.of(PARTITION, "3", TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TableType.REALTIME.toString());

    public static final List<String> TABLENAME_TABLETYPE_TASKTYPE =
        List.of(TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TABLETYPE_REALTIME, TASKTYPE,
            MINION_TASK_SEGMENT_IMPORT);

    public static final List<String> TABLENAME_WITHTYPE_TASKTYPE =
        List.of(TABLE, TABLENAME_WITH_TYPE_REALTIME, TASKTYPE, MINION_TASK_SEGMENT_IMPORT);

    public static final List<String> STATUS_TASKTYPE =
        List.of(STATUS, IN_PROGRESS, TASKTYPE, MINION_TASK_SEGMENT_IMPORT);

    public static final List<String> PERIODIC_TASK_TABLE_TABLETYPE =
        List.of(PERIODIC_TASK, MINION_TASK_SEGMENT_IMPORT, TABLE, ExportedLabelValues.TABLENAME, TABLETYPE,
            TABLETYPE_REALTIME);

    public static final List<String> EXPORTED_LABELS_TABLENAME_TYPE_TASKTYPE =
        List.of(ExportedLabelKeys.TABLE, ExportedLabelValues.TABLENAME, ExportedLabelKeys.TABLETYPE,
            ExportedLabelValues.TABLETYPE_REALTIME, ExportedLabelKeys.TASKTYPE,
            ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT);
  }

  public static class ExportedLabelKeys {
    public static final String TABLE = "table";
    public static final String ID = "id";
    public static final String TABLETYPE = "tableType";
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String TASKTYPE = "taskType";
    public static final String PERIODIC_TASK = "periodicTask";
    public static final String STATUS = "status";
    public static final String DATABASE = "database";
  }

  public static class ExportedLabelValues {
    public static final String TABLENAME = "myTable";
    public static final String TABLETYPE_REALTIME = "REALTIME";
    public static final String TABLENAME_WITH_TYPE_REALTIME =
        TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(TABLENAME);
    public static final String CONTROLLER_PERIODIC_TASK_CHC = "ClusterHealthCheck";
    public static final String MINION_TASK_SEGMENT_IMPORT = "SegmentImportTask";
    public static final String IN_PROGRESS = "IN_PROGRESS";
  }
}
