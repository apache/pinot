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
package org.apache.pinot.server.conf;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS;
import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_TRANSFORM_FUNCTIONS;
import static org.testng.Assert.assertEquals;


/// Tests the list configs of [ServerConf]. The cluster config tests set the value with
/// [PinotConfiguration#setProperty], as `ServiceStartableUtils.applyClusterConfig` does. That value is not split on
/// commas.
public class ServerConfTest {
  private static final String TRANSFORM_FUNCTIONS = "com.example.FirstFunction, com.example.SecondFunction";
  private static final List<String> EXPECTED_TRANSFORM_FUNCTIONS =
      List.of("com.example.FirstFunction", "com.example.SecondFunction");
  private static final String ALLOWED_TABLES = "table1, table2";
  private static final List<String> EXPECTED_ALLOWED_TABLES = List.of("table1", "table2");

  @Test
  public void testTransformFunctionsFromClusterConfig() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CONFIG_OF_TRANSFORM_FUNCTIONS, TRANSFORM_FUNCTIONS);
    assertEquals(new ServerConf(config).getTransformFunctions(), EXPECTED_TRANSFORM_FUNCTIONS);
  }

  @Test
  public void testAllowedTablesForEmittingMetricsFromClusterConfig() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS, ALLOWED_TABLES);
    assertEquals(new ServerConf(config).getAllowedTablesForEmittingMetrics(), EXPECTED_ALLOWED_TABLES);
  }

  @Test
  public void testListConfigsFromServerConfig() {
    ServerConf serverConf = new ServerConf(new PinotConfiguration(
        Map.of(CONFIG_OF_TRANSFORM_FUNCTIONS, TRANSFORM_FUNCTIONS, CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS,
            ALLOWED_TABLES)));
    assertEquals(serverConf.getTransformFunctions(), EXPECTED_TRANSFORM_FUNCTIONS);
    assertEquals(serverConf.getAllowedTablesForEmittingMetrics(), EXPECTED_ALLOWED_TABLES);
  }

  @Test
  public void testListConfigsUnset() {
    ServerConf serverConf = new ServerConf(new PinotConfiguration());
    assertEquals(serverConf.getTransformFunctions(), List.of());
    assertEquals(serverConf.getAllowedTablesForEmittingMetrics(), List.of());
  }
}
