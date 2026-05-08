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

import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


/**
 * Verifies that the Prometheus JMX template regexp patterns defined in the docker config YAML files
 * are valid Java regexps and match expected JMX metric name strings with correct capture groups.
 *
 * Config files under test: docker/images/pinot/etc/jmx_prometheus_javaagent/configs/
 *
 * @see <a href="https://github.com/apache/pinot/issues/13588">Issue #13588</a>
 */
public class PrometheusTemplateRegexpTest {

  private static final String CONFIG_BASE_PATH =
      "../docker/images/pinot/etc/jmx_prometheus_javaagent/configs";

  @DataProvider(name = "configFiles")
  public Object[][] configFiles() {
    return new Object[][]{
        {"broker.yml"},
        {"server.yml"},
        {"controller.yml"},
        {"minion.yml"},
        {"pinot.yml"}
    };
  }

  /**
   * Verifies every pattern in each YAML config file compiles as a valid Java regexp.
   */
  @Test(dataProvider = "configFiles")
  public void testAllPatternsAreValidRegexp(String configFile)
      throws Exception {
    List<String> patterns = extractPatterns(CONFIG_BASE_PATH + "/" + configFile);
    Assert.assertFalse(patterns.isEmpty(),
        "Expected at least one rule pattern in " + configFile);
    for (String patternStr : patterns) {
      try {
        Pattern.compile(patternStr);
      } catch (PatternSyntaxException e) {
        Assert.fail(
            "Invalid regexp in " + configFile + ": [" + patternStr + "] - " + e.getDescription());
      }
    }
  }

  // ---- Broker patterns ----

  /**
   * broker.yml: meters/timers scoped to tableNameWithType.
   * e.g. pinot.broker.myTable_REALTIME.queries
   */
  @Test
  public void testBrokerTableWithTypeMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("broker.yml", "pinot_$1_$6_$7");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"BrokerMetrics\", "
            + "name=\"pinot.broker.myTable_REALTIME.queries\"><>Count");
    Assert.assertTrue(m.matches(), "Pattern should match broker table-scoped meter");
    Assert.assertEquals(m.group(1), "broker");
    Assert.assertEquals(m.group(4), "myTable");
    Assert.assertEquals(m.group(5), "REALTIME");
    Assert.assertEquals(m.group(6), "queries");
    Assert.assertEquals(m.group(7), "Count");
  }

  /**
   * broker.yml: meters/timers scoped to tableNameWithType with database prefix.
   * e.g. pinot.broker.myDb.myTable_OFFLINE.queries
   */
  @Test
  public void testBrokerTableWithTypeMeterPatternWithDatabase()
      throws Exception {
    String pattern = loadPatternByName("broker.yml", "pinot_$1_$6_$7");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"BrokerMetrics\", "
            + "name=\"pinot.broker.myDb.myTable_OFFLINE.queries\"><>Count");
    Assert.assertTrue(m.matches(), "Pattern should match broker table-scoped meter with database prefix");
    Assert.assertEquals(m.group(1), "broker");
    Assert.assertEquals(m.group(3), "myDb");
    Assert.assertEquals(m.group(4), "myTable");
    Assert.assertEquals(m.group(5), "OFFLINE");
    Assert.assertEquals(m.group(6), "queries");
    Assert.assertEquals(m.group(7), "Count");
  }

  /**
   * broker.yml: meters/timers scoped to rawTableName.
   * e.g. pinot.broker.myTable.queries
   */
  @Test
  public void testBrokerRawTableNameMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("broker.yml", "pinot_$1_$5_$6");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"BrokerMetrics\", "
            + "name=\"pinot.broker.myTable.queries\"><>Count");
    Assert.assertTrue(m.matches(), "Pattern should match broker raw-table-name meter");
    Assert.assertEquals(m.group(1), "broker");
    Assert.assertEquals(m.group(4), "myTable");
    Assert.assertEquals(m.group(5), "queries");
    Assert.assertEquals(m.group(6), "Count");
  }

  /**
   * broker.yml: global gauge/meter/timer (no table scope).
   * e.g. pinot.broker.totalDocuments
   */
  @Test
  public void testBrokerGlobalMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("broker.yml", "pinot_broker_$1_$2");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"BrokerMetrics\", "
            + "name=\"pinot.broker.totalDocuments\"><>Value");
    Assert.assertTrue(m.matches(), "Pattern should match global broker gauge");
    Assert.assertEquals(m.group(1), "totalDocuments");
    Assert.assertEquals(m.group(2), "Value");
  }

  // ---- Server patterns ----

  /**
   * server.yml: meters/timers scoped to tableNameWithType.
   * e.g. pinot.server.myTable_OFFLINE.segmentUploadFailure
   */
  @Test
  public void testServerTableWithTypeMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("server.yml", "pinot_server_$5_$6");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
            + "name=\"pinot.server.myTable_OFFLINE.segmentUploadFailure\"><>Count");
    Assert.assertTrue(m.matches(), "Pattern should match server table-scoped meter");
    Assert.assertEquals(m.group(3), "myTable");
    Assert.assertEquals(m.group(4), "OFFLINE");
    Assert.assertEquals(m.group(5), "segmentUploadFailure");
    Assert.assertEquals(m.group(6), "Count");
  }

  /**
   * server.yml: gauge scoped to tableNameWithType with partition.
   * e.g. pinot.server.queries.myTable_REALTIME.3
   */
  @Test
  public void testServerTableWithTypeAndPartitionGaugePattern()
      throws Exception {
    String pattern = loadPatternByName("server.yml", "pinot_server_$1_$7");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
            + "name=\"pinot.server.queries.myTable_REALTIME.3\"><>Value");
    Assert.assertTrue(m.matches(), "Pattern should match server table-scoped gauge with partition");
    Assert.assertEquals(m.group(1), "queries");
    Assert.assertEquals(m.group(4), "myTable");
    Assert.assertEquals(m.group(5), "REALTIME");
    Assert.assertEquals(m.group(6), "3");
    Assert.assertEquals(m.group(7), "Value");
  }

  // ---- Controller patterns ----

  /**
   * controller.yml: minion task-type gauge.
   * e.g. pinot.controller.numMinionTasksInProgress.SegmentGenerationAndPush
   */
  @Test
  public void testControllerTaskTypeGaugePattern()
      throws Exception {
    String pattern = loadPatternByName("controller.yml", "pinot_controller_$1_$3");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"ControllerMetrics\", "
            + "name=\"pinot.controller.numMinionTasksInProgress.SegmentGenerationAndPush\"><>Value");
    Assert.assertTrue(m.matches(), "Pattern should match controller task-type gauge");
    Assert.assertEquals(m.group(1), "numMinionTasksInProgress");
    Assert.assertEquals(m.group(2), "SegmentGenerationAndPush");
    Assert.assertEquals(m.group(3), "Value");
  }

  /**
   * controller.yml: meters/timers scoped to tableNameWithType.
   * e.g. pinot.controller.myTable_OFFLINE.segmentUploadFailure
   */
  @Test
  public void testControllerTableWithTypeMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("controller.yml", "pinot_$1_$6_$7");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"ControllerMetrics\", "
            + "name=\"pinot.controller.myTable_OFFLINE.segmentUploadFailure\"><>Count");
    Assert.assertTrue(m.matches(), "Pattern should match controller table-scoped meter");
    Assert.assertEquals(m.group(1), "controller");
    Assert.assertEquals(m.group(4), "myTable");
    Assert.assertEquals(m.group(5), "OFFLINE");
    Assert.assertEquals(m.group(6), "segmentUploadFailure");
    Assert.assertEquals(m.group(7), "Count");
  }

  // ---- Minion patterns ----

  /**
   * minion.yml: meters/timers scoped to tableNameWithType and taskType.
   * e.g. pinot.minion.myTable_REALTIME.SegmentGenerationAndPush.segmentUploadFailure
   */
  @Test
  public void testMinionTableWithTypeAndTaskTypeMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("minion.yml", "pinot_minion_$6_$7");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"MinionMetrics\", "
            + "name=\"pinot.minion.myTable_REALTIME.SegmentGenerationAndPush.segmentUploadFailure\"><>Count");
    Assert.assertTrue(m.matches(), "Pattern should match minion table + taskType scoped meter");
    Assert.assertEquals(m.group(3), "myTable");
    Assert.assertEquals(m.group(4), "REALTIME");
    Assert.assertEquals(m.group(5), "SegmentGenerationAndPush");
    Assert.assertEquals(m.group(6), "segmentUploadFailure");
    Assert.assertEquals(m.group(7), "Count");
  }

  /**
   * minion.yml: meters/timers accepting either rawTableName or tableNameWithType.
   * e.g. pinot.minion.myTable.queries
   */
  @Test
  public void testMinionTableOrIdScopedMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("minion.yml", "pinot_minion_$2_$3");
    Matcher m = Pattern.compile(pattern).matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"MinionMetrics\", "
            + "name=\"pinot.minion.myTable.numberOfSegmentsQueued\"><>Value");
    Assert.assertTrue(m.matches(), "Pattern should match minion table/id scoped meter");
    Assert.assertEquals(m.group(1), "myTable");
    Assert.assertEquals(m.group(2), "numberOfSegmentsQueued");
    Assert.assertEquals(m.group(3), "Value");
  }

  /**
   * Returns the pattern string for the rule whose {@code name} field equals {@code ruleName}.
   * Keying off the rule name survives YAML rule reorderings — inserting or moving a rule in
   * the config file will not silently shift the index and cause this test to assert against
   * the wrong pattern.
   */
  @SuppressWarnings("unchecked")
  private String loadPatternByName(String configFile, String ruleName)
      throws Exception {
    Yaml yaml = new Yaml();
    try (FileReader reader = new FileReader(CONFIG_BASE_PATH + "/" + configFile)) {
      Map<String, Object> config = yaml.load(reader);
      List<Map<String, Object>> rules = (List<Map<String, Object>>) config.get("rules");
      return rules.stream()
          .filter(rule -> ruleName.equals(rule.get("name")))
          .map(rule -> (String) rule.get("pattern"))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              "No rule with name '" + ruleName + "' found in " + configFile));
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> extractPatterns(String filePath)
      throws Exception {
    Yaml yaml = new Yaml();
    try (FileReader reader = new FileReader(filePath)) {
      Map<String, Object> config = yaml.load(reader);
      List<Map<String, Object>> rules = (List<Map<String, Object>>) config.get("rules");
      return rules.stream()
          .filter(rule -> rule.containsKey("pattern"))
          .map(rule -> (String) rule.get("pattern"))
          .collect(Collectors.toList());
    }
  }
}
