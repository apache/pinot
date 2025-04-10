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
package org.apache.pinot.common.metrics.prometheus;

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
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.apache.pinot.common.metrics.prometheus.PinotPrometheusMetricsTest.ExportedLabelKeys.*;
import static org.apache.pinot.common.metrics.prometheus.PinotPrometheusMetricsTest.ExportedLabelValues.CONTROLLER_PERIODIC_TASK_CHC;
import static org.apache.pinot.common.metrics.prometheus.PinotPrometheusMetricsTest.ExportedLabelValues.IN_PROGRESS;
import static org.apache.pinot.common.metrics.prometheus.PinotPrometheusMetricsTest.ExportedLabelValues.TABLENAME_WITH_TYPE_REALTIME;
import static org.apache.pinot.common.metrics.prometheus.PinotPrometheusMetricsTest.ExportedLabelValues.TABLETYPE_REALTIME;
import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public abstract class PinotPrometheusMetricsTest {

  protected static final String TABLE_NAME_WITH_TYPE =
      TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(ExportedLabelValues.TABLENAME);
  protected static final String KAFKA_TOPIC = "myTopic";
  protected static final String PARTITION_GROUP_ID = "partitionGroupId";
  protected static final String CLIENT_ID =
      String.format("%s-%s-%s", TABLE_NAME_WITH_TYPE, KAFKA_TOPIC, PARTITION_GROUP_ID);
  protected static final String REBALANCE_JOB_ID = UUID.randomUUID().toString();

  protected HttpClient _httpClient;

  protected PinotMetricsFactory _pinotMetricsFactory;

  //each meter defined in code is exported with these measurements
  private static final List<String> METER_TYPES =
      List.of("Count", "FiveMinuteRate", "MeanRate", "OneMinuteRate", "FifteenMinuteRate");

  //each timer defined in code is exported with these measurements
  private static final List<String> TIMER_TYPES =
      List.of("Count", "FiveMinuteRate", "Max", "999thPercentile", "95thPercentile", "75thPercentile", "98thPercentile",
          "OneMinuteRate", "50thPercentile", "99thPercentile", "FifteenMinuteRate", "Mean", "StdDev", "MeanRate",
          "Min");

  //each gauge defined in code is exported with these measurements
  private static final List<String> GAUGE_TYPES = List.of("Value");

  private HTTPServer _httpServer;

  @BeforeClass
  public void setupTest() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();

    _pinotMetricsFactory = getPinotMetricsFactory();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        _pinotMetricsFactory.getClass().getCanonicalName());
    PinotMetricUtils.init(pinotConfiguration);

    _pinotMetricsFactory.makePinotJmxReporter(_pinotMetricsFactory.getPinotMetricsRegistry()).start();
    _httpClient = new HttpClient();
    _httpServer = startExporter();
  }

  @AfterClass
  public void cleanup() {
    _httpServer.close();
  }

  /**
   * Pinot currently uses the JMX->Prom exporter to export metrics to Prometheus. Normally, this runs as an agent in the
   * JVM. In this case however, we've got tests using four different config files (server.yml, broker.yml,
   * controller.yml and minion.yml). Loading the same agent in the same JVM multiple times isn't allowed, we are copying
   * the agent's code to some degree and starting up the HTTP servers manually. For impl, see:
   * <a href="https://github.com/prometheus/jmx_exporter/blob/a3b9443564ff5a78c25fd6566396fda2b7cbf216">...</a>
   * /jmx_prometheus_javaagent/src/main/java/io/prometheus/jmx/JavaAgent.java#L48
   *
   * @return the corresponding HTTP server on a random unoccupied port
   */
  protected HTTPServer startExporter() {
    String args = String.format("%s:%s", 0, getConfigFile());
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
      for (String gaugeType : GAUGE_TYPES) {
        PromMetric expectedMetric = PromMetric.withName(exportedMetricPrefix + exportedGaugePrefix + "_" + gaugeType);
        Assert.assertTrue(promMetrics.contains(expectedMetric),
            "Cannot find gauge: " + expectedMetric + " in exported metrics");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertGaugeExportedCorrectly(String exportedGaugePrefix, List<String> labels,
      String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
      for (String gaugeType : GAUGE_TYPES) {
        PromMetric expectedGauge =
            PromMetric.withNameAndLabels(exportedMetricPrefix + exportedGaugePrefix + "_" + gaugeType, labels);
        Assert.assertTrue(promMetrics.contains(expectedGauge),
            "Cannot find gauge: " + expectedGauge + " in exported metrics");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void assertTimerExportedCorrectly(String exportedTimerPrefix, String exportedMetricPrefix) {
    List<PromMetric> promMetrics;
    try {
      promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
      for (String timerType : TIMER_TYPES) {
        PromMetric expectedTimer = PromMetric.withName(exportedMetricPrefix + exportedTimerPrefix + "_" + timerType);
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
      for (String timerType : TIMER_TYPES) {
        PromMetric expectedTimer =
            PromMetric.withNameAndLabels(exportedMetricPrefix + exportedTimerPrefix + "_" + timerType, labels);
        Assert.assertTrue(promMetrics.contains(expectedTimer),
            "Cannot find timer: " + expectedTimer + " in exported metrics");
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

  protected SimpleHttpResponse getExportedPromMetrics() {
    try {
      return _httpClient.sendGetRequest(new URI("http://localhost:" + _httpServer.getPort() + "/metrics"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract PinotMetricsFactory getPinotMetricsFactory();

  protected abstract String getConfigFile();

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

  private static class JMXExporterConfig {
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

  private String loadResourceAsString(String resourceFileName) {
    URL url = Resources.getResource(resourceFileName);
    try {
      return Resources.toString(url, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class ExportedLabels {

    public static final List<String> TABLENAME = List.of(TABLE, ExportedLabelValues.TABLENAME);
    public static final List<String> TABLENAME_TABLETYPE =
        List.of(TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TABLETYPE_REALTIME);
    public static final List<String> PARTITION_TABLENAME_TABLETYPE_KAFKATOPIC =
        List.of(PARTITION, PARTITION_GROUP_ID, TABLE, ExportedLabelValues.TABLENAME, TABLETYPE,
            TableType.REALTIME.toString(), TOPIC, KAFKA_TOPIC);
    public static final List<String> PARTITION_TABLENAME_TABLETYPE =
        List.of(PARTITION, "3", TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TableType.REALTIME.toString());

    public static final List<String> TABLENAME_TABLETYPE_CONTROLLER_TASKTYPE =
        List.of(TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TABLETYPE_REALTIME, TASKTYPE,
            CONTROLLER_PERIODIC_TASK_CHC);

    public static final List<String> TABLENAMEWITHTYPE_CONTROLLER_TASKTYPE =
        List.of(TABLE, TABLENAME_WITH_TYPE_REALTIME, TASKTYPE, CONTROLLER_PERIODIC_TASK_CHC);

    public static final List<String> JOBSTATUS_CONTROLLER_TASKTYPE =
        List.of(STATUS, IN_PROGRESS, TASKTYPE, CONTROLLER_PERIODIC_TASK_CHC);

    public static final List<String> CONTROLLER_TASKTYPE_TABLENAME_TABLETYPE =
        List.of(PERIODIC_TASK, CONTROLLER_PERIODIC_TASK_CHC, TABLE, ExportedLabelValues.TABLENAME, TABLETYPE,
            TABLETYPE_REALTIME);

    public static final List<String> TABLENAME_TABLETYPE_MINION_TASKTYPE =
        List.of(ExportedLabelKeys.TABLE, ExportedLabelValues.TABLENAME, ExportedLabelKeys.TABLETYPE,
            ExportedLabelValues.TABLETYPE_REALTIME, ExportedLabelKeys.TASKTYPE,
            ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT);

    public static final List<String> JOBSTATUS_TABLENAME_TABLETYPE =
        List.of(STATUS, ExportedLabelValues.DONE, TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TABLETYPE_REALTIME);

    public static final List<String> TASKTYPE_TABLENAME_TABLETYPE =
        List.of(TASKTYPE, ExportedLabelValues.MINION_TASK_SEGMENT_IMPORT, TABLE, ExportedLabelValues.TABLENAME,
            TABLETYPE, TABLETYPE_REALTIME);

    public static final List<String> JOBID_TABLENAME_TABLETYPE =
        List.of(JOBID, REBALANCE_JOB_ID, TABLE, ExportedLabelValues.TABLENAME, TABLETYPE, TABLETYPE_REALTIME);
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
    public static final String JOBID = "jobId";
  }

  public static class ExportedLabelValues {
    public static final String TABLENAME = "myTable";
    public static final String TABLETYPE_REALTIME = "REALTIME";
    public static final String TABLENAME_WITH_TYPE_REALTIME =
        TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(TABLENAME);
    public static final String CONTROLLER_PERIODIC_TASK_CHC = "ClusterHealthCheck";
    public static final String MINION_TASK_SEGMENT_IMPORT = "SegmentImportTask";
    public static final String IN_PROGRESS = "IN_PROGRESS";
    public static final String DONE = "DONE";
  }

  /*
   * Represents an exported Prometheus metric. A Prom metric looks like:
   * pinot_server_realtimeRowsSanitized_Count{app="pinot", cluster_name="pinot",
   * component="pinot-server", component_name="server-default-tenant-1"}
   */
  private static class PromMetric {
    private final String _metricName;
    private final Map<String, String> _labels;

    public String getMetricName() {
      return _metricName;
    }

    public Map<String, String> getLabels() {
      return _labels;
    }

    //make constructor private so that it can be instantiated only with the factory methods
    private PromMetric(String metricName, Map<String, String> labels) {
      _metricName = metricName;
      _labels = labels;
    }

    /**
     * Create an instance of {@link PromMetric} from an exported Prometheus metric
     *
     * @param exportedMetric the exported Prom metric (name + labels)
     * @return the corresponding {@link PromMetric}
     */
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

    /**
     * Creates an instance of {@link PromMetric} with a name and an empty label list
     *
     * @param metricName the metric name
     * @return the corresponding PromMetric
     */
    public static PromMetric withName(String metricName) {
      return new PromMetric(metricName, new LinkedHashMap<>());
    }

    /**
     * Creates an instance of {@link PromMetric} with a name and an label list
     *
     * @param metricName the metric name
     * @param labels     the labels
     * @return the corresponding PromMetric
     */
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
}
