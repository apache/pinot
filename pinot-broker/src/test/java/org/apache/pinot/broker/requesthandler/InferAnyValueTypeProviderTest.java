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
package org.apache.pinot.broker.requesthandler;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InferAnyValueTypeProviderTest {
  private static final String KEY = Broker.CONFIG_OF_BROKER_QUERY_INFER_ANY_VALUE_TYPE;

  @Test
  public void testBrokerConfProvidesTheDefault() {
    assertFalse(newProvider(Map.of()).isEnabled());
    assertTrue(newProvider(Map.of(KEY, "true")).isEnabled());
  }

  @Test
  public void testClusterConfigWinsOverBrokerConfAndReloads() {
    InferAnyValueTypeProvider provider = newProvider(Map.of());
    provider.onChange(Set.of(KEY), Map.of(KEY, "true"));
    assertTrue(provider.isEnabled());

    // Another key changing leaves the value as it is.
    provider.onChange(Set.of("pinot.broker.other"), Map.of());
    assertTrue(provider.isEnabled());

    // Removing the key from the cluster config falls back to the broker conf.
    provider.onChange(Set.of(KEY), Map.of());
    assertFalse(provider.isEnabled());

    provider = newProvider(Map.of(KEY, "true"));
    provider.onChange(Set.of(KEY), Map.of(KEY, " FALSE "));
    assertFalse(provider.isEnabled());
  }

  @Test
  public void testInvalidClusterConfigFallsBackToBrokerConf() {
    InferAnyValueTypeProvider provider = newProvider(Map.of(KEY, "true"));
    provider.onChange(Set.of(KEY), Map.of(KEY, "flase"));
    assertTrue(provider.isEnabled());
  }

  private static InferAnyValueTypeProvider newProvider(Map<String, Object> properties) {
    return new InferAnyValueTypeProvider(new PinotConfiguration(properties));
  }
}
