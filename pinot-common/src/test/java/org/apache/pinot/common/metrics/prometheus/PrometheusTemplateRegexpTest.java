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


/// Verifies that the Prometheus JMX template regexp patterns defined in the docker config YAML files
/// are valid Java regexps and match expected JMX metric name strings with correct capture groups.
///
/// Config files under test: docker/images/pinot/etc/jmx_prometheus_javaagent/configs/
///
/// @see <a href="https://github.com/apache/pinot/issues/13588">Issue #13588</a>
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

  /// Verifies every pattern in each YAML config file compiles as a valid Java regexp.
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

  /// broker.yml: meters/timers scoped to tableNameWithType.
  /// e.g. pinot.broker.myTable_REALTIME.queries
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

  /// broker.yml: meters/timers scoped to tableNameWithType with database prefix.
  /// e.g. pinot.broker.myDb.myTable_OFFLINE.queries
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

  /// broker.yml: meters/timers scoped to rawTableName.
  /// e.g. pinot.broker.myTable.queries
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

  /// broker.yml: global gauge/meter/timer (no table scope). The catch-all is group-flexible at the
  /// prefix so non-broker MBean groups registered in the broker JVM (e.g. pinot.mse.\*) are also
  /// exported with this rule.
  /// e.g. pinot.broker.totalDocuments, pinot.mse.queries
  @Test
  public void testBrokerGlobalMeterPattern()
      throws Exception {
    String pattern = loadPatternByName("broker.yml", "pinot_$1_$2_$3");
    Pattern compiled = Pattern.compile(pattern);

    Matcher brokerMatch = compiled.matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"BrokerMetrics\", "
            + "name=\"pinot.broker.totalDocuments\"><>Value");
    Assert.assertTrue(brokerMatch.matches(), "Pattern should match global broker gauge");
    Assert.assertEquals(brokerMatch.group(1), "broker");
    Assert.assertEquals(brokerMatch.group(2), "totalDocuments");
    Assert.assertEquals(brokerMatch.group(3), "Value");

    Matcher mseMatch = compiled.matcher(
        "\"org.apache.pinot.common.metrics\"<type=\"MseMetrics\", name=\"pinot.mse.queries\"><>Count");
    Assert.assertTrue(mseMatch.matches(), "Pattern should also match pinot.mse.* mbeans on broker JVMs");
    Assert.assertEquals(mseMatch.group(1), "mse");
    Assert.assertEquals(mseMatch.group(2), "queries");
    Assert.assertEquals(mseMatch.group(3), "Count");
  }

  // ---- Server patterns ----

  /// server.yml: meters/timers scoped to tableNameWithType.
  /// e.g. pinot.server.myTable_OFFLINE.segmentUploadFailure
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

  /// server.yml: gauge scoped to tableNameWithType with partition.
  /// e.g. pinot.server.queries.myTable_REALTIME.3
  @Test
  public void testServerTableWithTypeAndPartitionGaugePattern()
      throws Exception {
    String pattern = loadPatternByName("server.yml", "pinot_server_$1_$7", "pinot\\.server\\.(\\w+)\\.");
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

  // ---- OPEN_STRUCT server patterns ----

  /// server.yml: per-key OPEN_STRUCT gauge. The JMX name embeds the raw user-supplied JSON key
  /// after the `$` separator, so the key group must survive characters the generic `\w+` rules
  /// reject: `$`, `.`, `-` and spaces are all legal in a Prometheus label value.
  @Test
  public void testServerOpenStructPerKeyGaugePattern()
      throws Exception {
    Pattern compiled = Pattern.compile(
        loadPatternByName("server.yml", "pinot_server_$1_$8", "openStructLastSegmentKeyDocCount"));

    Matcher plain = compiled.matcher("\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
        + "name=\"pinot.server.openStructLastSegmentKeyDocCount.myTable_REALTIME.metrics$clicks\"><>Value");
    Assert.assertTrue(plain.matches(), "Pattern should match per-key OPEN_STRUCT gauge");
    Assert.assertEquals(plain.group(1), "openStructLastSegmentKeyDocCount");
    Assert.assertEquals(plain.group(4), "myTable");
    Assert.assertEquals(plain.group(5), "REALTIME");
    Assert.assertEquals(plain.group(6), "metrics");
    Assert.assertEquals(plain.group(7), "clicks");
    Assert.assertEquals(plain.group(8), "Value");

    // A key containing '.' would be swallowed by the generic rules; the column group is ([^.$]+)
    // so the split stays unambiguous and the whole remainder lands in the key label.
    Matcher dotted = compiled.matcher("\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
        + "name=\"pinot.server.openStructLastSegmentKeyDocCount.myDb.myTable_OFFLINE.metrics$user.id\"><>Value");
    Assert.assertTrue(dotted.matches(), "Pattern should match a key containing '.'");
    Assert.assertEquals(dotted.group(3), "myDb");
    Assert.assertEquals(dotted.group(4), "myTable");
    Assert.assertEquals(dotted.group(6), "metrics");
    Assert.assertEquals(dotted.group(7), "user.id");

    // A key containing '$' — greedy (.+) puts the split at the first '$', which is the separator
    // the splitter emitted, so the trailing '$' stays part of the key.
    Matcher dollar = compiled.matcher("\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
        + "name=\"pinot.server.openStructLastSegmentKeyDocCount.myTable_OFFLINE.metrics$a$b\"><>Value");
    Assert.assertTrue(dollar.matches(), "Pattern should match a key containing '$'");
    Assert.assertEquals(dollar.group(6), "metrics");
    Assert.assertEquals(dollar.group(7), "a$b");

    // The sparse catch-all column is named with the reserved __sparse__ suffix rather than a key.
    Matcher sparse = compiled.matcher("\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
        + "name=\"pinot.server.openStructLastSegmentKeyDocCount.myTable_OFFLINE.metrics$__sparse__\"><>Value");
    Assert.assertTrue(sparse.matches(), "Pattern should match the sparse column");
    Assert.assertEquals(sparse.group(7), "__sparse__");
  }

  /// server.yml: column-level OPEN_STRUCT gauges. These must be matched here rather than by the
  /// generic "tableNameWithType + partitionId" rule, which would export the column as
  /// partition="<column>". Every branch of the alternation is exercised: a typo in one of them is
  /// still a valid regexp, so it would pass testAllPatternsAreValidRegexp and then silently fail to
  /// scrape in production.
  @Test
  public void testServerOpenStructColumnGaugePattern()
      throws Exception {
    Pattern compiled =
        Pattern.compile(loadPatternByName("server.yml", "pinot_server_$1_$7", "openStructLastSegmentDenseKeyCount"));
    for (String metric : List.of("openStructLastSegmentDenseKeyCount", "openStructLastSegmentSparseKeyCount",
        "openStructLastSegmentKeyCount", "openStructLastSegmentDocCount")) {
      Matcher m = compiled.matcher("\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
          + "name=\"pinot.server." + metric + ".myTable_OFFLINE.metrics\"><>Value");
      Assert.assertTrue(m.matches(), "Pattern should match column-level OPEN_STRUCT gauge " + metric);
      Assert.assertEquals(m.group(1), metric);
      Assert.assertEquals(m.group(4), "myTable");
      Assert.assertEquals(m.group(5), "OFFLINE");
      Assert.assertEquals(m.group(6), "metrics");
      Assert.assertEquals(m.group(7), "Value");
    }
  }

  /// server.yml: column-level OPEN_STRUCT meters. Meter JMX names put the metric name last,
  /// unlike gauges, so these need their own rule ahead of the generic rawTableName meter rule
  /// (which would otherwise export table="<table>.<column>").
  @Test
  public void testServerOpenStructColumnMeterPattern()
      throws Exception {
    Pattern compiled = Pattern.compile(
        loadPatternByName("server.yml", "pinot_server_$6_$7", "openStructTypeCoercionFailures"));
    for (String metric : List.of("openStructTypeCoercionFailures", "openStructTypeInferenceFailures")) {
      Matcher m = compiled.matcher("\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
          + "name=\"pinot.server.myTable_REALTIME.metrics." + metric + "\"><>Count");
      Assert.assertTrue(m.matches(), "Pattern should match column-level OPEN_STRUCT meter " + metric);
      Assert.assertEquals(m.group(3), "myTable");
      Assert.assertEquals(m.group(4), "REALTIME");
      Assert.assertEquals(m.group(5), "metrics");
      Assert.assertEquals(m.group(6), metric);
      Assert.assertEquals(m.group(7), "Count");
    }
  }

  /// jmx_exporter evaluates rules in file order and stops at the first match, so the OPEN_STRUCT
  /// rules are only correct because they precede the generic ones. Asserting the pattern in
  /// isolation does not cover that: the generic "tableNameWithType + partitionId" gauge rule and
  /// the generic rawTableName meter rule both full-match these names too, and would export the
  /// column as partition="metrics" and table="myTable.metrics" respectively. This test evaluates
  /// the whole ordered list so a future reordering of server.yml fails here rather than in prod.
  @Test
  public void testServerOpenStructRulesPrecedeGenericRules()
      throws Exception {
    List<String> ordered = extractPatterns(CONFIG_BASE_PATH + "/server.yml");
    assertFirstMatchingPatternContains(ordered,
        "\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
            + "name=\"pinot.server.openStructLastSegmentKeyDocCount.myTable_OFFLINE.metrics$clicks\"><>Value",
        "openStructLastSegmentKeyDocCount");
    assertFirstMatchingPatternContains(ordered,
        "\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
            + "name=\"pinot.server.openStructLastSegmentDenseKeyCount.myTable_OFFLINE.metrics\"><>Value",
        "openStructLastSegmentDenseKeyCount");
    assertFirstMatchingPatternContains(ordered,
        "\"org.apache.pinot.common.metrics\"<type=\"ServerMetrics\", "
            + "name=\"pinot.server.myTable_REALTIME.metrics.openStructTypeInferenceFailures\"><>Count",
        "openStructTypeInferenceFailures");
  }

  /// Asserts the first rule in file order that matches `jmxName` is one whose pattern contains
  /// `expectedInPattern`, mirroring jmx_exporter's first-match-wins evaluation.
  private void assertFirstMatchingPatternContains(List<String> orderedPatterns, String jmxName,
      String expectedInPattern) {
    for (String pattern : orderedPatterns) {
      if (Pattern.compile(pattern).matcher(jmxName).matches()) {
        Assert.assertTrue(pattern.contains(expectedInPattern),
            "First rule matching [" + jmxName + "] was [" + pattern + "], expected one containing '"
                + expectedInPattern + "'. OPEN_STRUCT rules must precede the generic rules in server.yml.");
        return;
      }
    }
    Assert.fail("No rule in server.yml matches [" + jmxName + "]");
  }

  // ---- Controller patterns ----

  /// controller.yml: minion task-type gauge.
  /// e.g. pinot.controller.numMinionTasksInProgress.SegmentGenerationAndPush
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

  /// controller.yml: meters/timers scoped to tableNameWithType.
  /// e.g. pinot.controller.myTable_OFFLINE.segmentUploadFailure
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

  /// minion.yml: meters/timers scoped to tableNameWithType and taskType.
  /// e.g. pinot.minion.myTable_REALTIME.SegmentGenerationAndPush.segmentUploadFailure
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

  /// minion.yml: meters/timers accepting either rawTableName or tableNameWithType.
  /// e.g. pinot.minion.myTable.queries
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

  /// Returns the pattern string for the rule whose `name` field equals `ruleName`. Fails when more
  /// than one rule shares that name — use [#loadPatternByName(String,String,String)] instead.
  ///
  /// Keying off the rule name survives YAML rule reorderings — inserting or moving a rule in
  /// the config file will not silently shift the index and cause this test to assert against
  /// the wrong pattern.
  private String loadPatternByName(String configFile, String ruleName)
      throws Exception {
    return loadPatternByName(configFile, ruleName, "");
  }

  /// Same as [#loadPatternByName(String,String)], but narrowed to the rule whose pattern also
  /// contains `patternDiscriminator`. Rule names are only the exported metric-name template
  /// (e.g. `pinot_server_$1_$7`), so several rules can legitimately share one; the discriminator
  /// picks the intended rule instead of silently taking whichever comes first in the file.
  @SuppressWarnings("unchecked")
  private String loadPatternByName(String configFile, String ruleName, String patternDiscriminator)
      throws Exception {
    Yaml yaml = new Yaml();
    try (FileReader reader = new FileReader(CONFIG_BASE_PATH + "/" + configFile)) {
      Map<String, Object> config = yaml.load(reader);
      List<Map<String, Object>> rules = (List<Map<String, Object>>) config.get("rules");
      List<String> matches = rules.stream()
          .filter(rule -> ruleName.equals(rule.get("name")))
          .map(rule -> (String) rule.get("pattern"))
          .filter(pattern -> pattern != null && pattern.contains(patternDiscriminator))
          .collect(Collectors.toList());
      Assert.assertFalse(matches.isEmpty(),
          "No rule named '" + ruleName + "' containing '" + patternDiscriminator + "' in " + configFile);
      Assert.assertEquals(matches.size(), 1,
          "Ambiguous rule name '" + ruleName + "' in " + configFile
              + "; pass a patternDiscriminator to select one of: " + matches);
      return matches.get(0);
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
