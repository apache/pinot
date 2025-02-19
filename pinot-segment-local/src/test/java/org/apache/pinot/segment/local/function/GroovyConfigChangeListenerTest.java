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
package org.apache.pinot.segment.local.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class GroovyConfigChangeListenerTest {

  @BeforeMethod
  public void setUp()
      throws JsonProcessingException {
    assertNull(GroovyFunctionEvaluator.getConfig());
    GroovyStaticAnalyzerConfig groovyConfig = new GroovyStaticAnalyzerConfig(
        List.of("java.lang.Math"),
        List.of("java.lang.Math"),
        List.of("java.lang.Math"),
        List.of("invoke", "execute"),
        false);
    GroovyFunctionEvaluator.setConfig(groovyConfig);
  }

  @AfterMethod
  public void tearDown()
      throws JsonProcessingException {
    GroovyFunctionEvaluator.setConfig(null);
  }

  @Test
  public void testGroovyConfigIsUpdated() {
    Set<String> changedConfigs = Set.of(CommonConstants.GROOVY_STATIC_ANALYZER_CONFIG);
    String updatedConfig = "{\"allowedReceivers\" : [\"java.lang.String\"]}";
    Map<String, String> clusterConfigs = Map.of(CommonConstants.GROOVY_STATIC_ANALYZER_CONFIG, updatedConfig);
    assertEquals(GroovyFunctionEvaluator.getConfig().getAllowedReceivers(), List.of("java.lang.Math"));
    GroovyConfigChangeListener groovyConfigChangeListener = new GroovyConfigChangeListener();
    groovyConfigChangeListener.onChange(changedConfigs, clusterConfigs);
    assertNotNull(GroovyFunctionEvaluator.getConfig());
    assertEquals(GroovyFunctionEvaluator.getConfig().getAllowedReceivers(), List.of("java.lang.String"));
  }

  @Test
  public void testGroovyConfigIsDeleted() {
    Set<String> changedConfigs = Set.of(CommonConstants.GROOVY_STATIC_ANALYZER_CONFIG);
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(CommonConstants.GROOVY_STATIC_ANALYZER_CONFIG, null);
    assertEquals(GroovyFunctionEvaluator.getConfig().getAllowedReceivers(), List.of("java.lang.Math"));

    GroovyConfigChangeListener groovyConfigChangeListener = new GroovyConfigChangeListener();
    groovyConfigChangeListener.onChange(changedConfigs, clusterConfigs);
    assertNull(GroovyFunctionEvaluator.getConfig());
  }

  @Test
  public void testGroovyConfigIsUnchanged() {
    Set<String> changedConfigs = Set.of("random.test.key");
    Map<String, String> clusterConfigs = Map.of("random.test.key", "random.test.value");
    GroovyConfigChangeListener groovyConfigChangeListener = new GroovyConfigChangeListener();
    groovyConfigChangeListener.onChange(changedConfigs, clusterConfigs);
    assertNotNull(GroovyFunctionEvaluator.getConfig());
    assertEquals(GroovyFunctionEvaluator.getConfig().getAllowedReceivers(), List.of("java.lang.Math"));
  }
}
