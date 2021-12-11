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

package org.apache.pinot.controller.recommender.rules.impl;

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class AggregateMetricsRuleTest {

  @Test
  public void testRun()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input =
        createInput(metrics, "select sum(a), sum(b), sum(c) from tableT", "select sum(a) from tableT2");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertTrue(output.isAggregateMetrics());
  }

  @Test
  public void testRunNonAggregate()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input =
        createInput(metrics, "select sum(a), sum(b), sum(c) from tableT", "select sum(a), b from tableT2");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }

  @Test
  public void testRunNonAggregateWithNonSumFunction()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input = createInput(metrics, "select sum(a), sum(b), max(c) from tableT");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }

  @Test
  public void testRunNonMetricColumnInSum()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input = createInput(metrics, "select sum(a), sum(b), sum(X) from tableT");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }

  @Test
  public void testRunComplexExpressionInSumWithMetricColumns()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input = createInput(metrics, "select sum(a), sum(b), sum(2 * a + 3 * b + c) from tableT");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertTrue(output.isAggregateMetrics());
  }

  @Test
  public void testRunComplexExpressionInSumWithSomeNonMetricColumns()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input = createInput(metrics, "select sum(a), sum(b), sum(2 * a + 3 * b + X) from tableT");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }

  @Test
  public void testRunWithGroupBy()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input = createInput(metrics, "select d1, d2, sum(a), sum(b) from tableT group by d1, d2");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertTrue(output.isAggregateMetrics());
  }

  @Test
  public void testRunWithTransformationFunctionInGroupBy()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input = createInput(metrics,
        "select d, dateTimeConvert(t, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES'), sum(a), sum(b)"
            + " from tableT"
            + " group by d, dateTimeConvert(t, '1:MILLISECONDS:EPOCH', '1:SECONDS:EPOCH', '15:MINUTES')");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertTrue(output.isAggregateMetrics());
  }

  @Test
  public void testRunOfflineTable()
      throws Exception {
    Set<String> metrics = ImmutableSet.of("a", "b", "c");
    InputManager input = createInput(metrics, "select sum(a), sum(b), sum(c) from tableT");
    input.setTableType("OFFLINE");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }

  private InputManager createInput(Set<String> metricNames, String... queries)
      throws Exception {
    InputManager input = new InputManager();
    Map<String, Double> queryWithWeight = new HashMap<>();
    for (String query : queries) {
      queryWithWeight.put(query, 1.0);
    }
    input.setQueryWeightMap(queryWithWeight);
    input.setTableType("Realtime");
    metricNames.forEach(metric -> input.getSchema().addField(new MetricFieldSpec(metric, FieldSpec.DataType.INT)));
    input.init();
    return input;
  }
}
