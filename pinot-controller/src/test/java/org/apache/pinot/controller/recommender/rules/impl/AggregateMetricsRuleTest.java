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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AggregateMetricsRuleTest {

  @Test
  public void testRun()
      throws Exception {
    InputManager input = createInput("select sum(a), sum(b), sum(c) from tableT", "select sum(a) from tableT2");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertTrue(output.isAggregateMetrics());
  }

  @Test
  public void testRun_nonAggregate()
      throws Exception {
    InputManager input = createInput("select sum(a), sum(b), sum(c) from tableT", "select sum(a), b from tableT2");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }

  @Test
  public void testRun_nonAggregate_withNonSumFunction()
      throws Exception {
    InputManager input = createInput("select sum(a), sum(b), max(c) from tableT");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }

  @Test
  public void testRun_offlineTable()
      throws Exception {
    InputManager input = createInput("select sum(a), sum(b), sum(c) from tableT");
    input.setTableType("OFFLINE");
    ConfigManager output = new ConfigManager();
    AggregateMetricsRule rule = new AggregateMetricsRule(input, output);
    rule.run();
    assertFalse(output.isAggregateMetrics());
  }


  private InputManager createInput(String... queries)
      throws Exception {
    InputManager input = new InputManager();
    Map<String, Double> queryWithWeight = new HashMap<>();
    for (String query : queries) {
      queryWithWeight.put(query, 1.0);
    }
    input.setQueryWeightMap(queryWithWeight);
    input.setTableType("Realtime");
    input.init();
    return input;
  }
}
