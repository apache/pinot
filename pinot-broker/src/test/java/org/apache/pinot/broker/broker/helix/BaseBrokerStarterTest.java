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
package org.apache.pinot.broker.broker.helix;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS;
import static org.testng.Assert.assertEquals;


/// Tests the list configs that [BaseBrokerStarter] reads. The cluster config test sets the value with
/// [PinotConfiguration#setProperty], as `ServiceStartableUtils.applyClusterConfig` does. That value is not split on
/// commas.
public class BaseBrokerStarterTest {

  @Test
  public void testAllowedTablesForEmittingMetricsFromClusterConfig() {
    PinotConfiguration brokerConf = new PinotConfiguration();
    brokerConf.setProperty(CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS, "table1, table2");
    assertEquals(BaseBrokerStarter.getAllowedTablesForEmittingMetrics(brokerConf), List.of("table1", "table2"));
  }

  @Test
  public void testAllowedTablesForEmittingMetricsFromBrokerConfig() {
    PinotConfiguration brokerConf =
        new PinotConfiguration(Map.of(CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS, "table1, table2"));
    assertEquals(BaseBrokerStarter.getAllowedTablesForEmittingMetrics(brokerConf), List.of("table1", "table2"));
  }

  @Test
  public void testAllowedTablesForEmittingMetricsUnset() {
    assertEquals(BaseBrokerStarter.getAllowedTablesForEmittingMetrics(new PinotConfiguration()), List.of());
  }
}
