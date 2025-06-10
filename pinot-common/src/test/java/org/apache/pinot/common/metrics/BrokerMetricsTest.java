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

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Unit tests for {@link BrokerMetrics}
 */
public class BrokerMetricsTest {

  @Test
  public void testGetTagForPreferredGroup() {
    // Test case 1: queryOption is null
    assertEquals(BrokerMetrics.getTagForPreferredGroup(null), "preferredGroupOptUnset",
        "Should return preferredGroupOptUnset when queryOption is null");

    // Test case 2: queryOption is empty
    Map<String, String> emptyQueryOption = new HashMap<>();
    assertEquals(BrokerMetrics.getTagForPreferredGroup(emptyQueryOption), "preferredGroupOptUnset",
        "Should return preferredGroupOptUnset when queryOption is empty");

    // Test case 3: queryOption does not contain ORDERED_PREFERRED_REPLICAS
    Map<String, String> queryOptionWithoutPreferredGroup = new HashMap<>();
    queryOptionWithoutPreferredGroup.put("someOtherOption", "value");
    assertEquals(BrokerMetrics.getTagForPreferredGroup(queryOptionWithoutPreferredGroup),
        "preferredGroupOptUnset",
        "Should return preferredGroupOptUnset when queryOption does not contain ORDERED_PREFERRED_REPLICAS");

    // Test case 4: queryOption contains ORDERED_PREFERRED_REPLICAS
    Map<String, String> queryOptionWithPreferredGroup = new HashMap<>();
    queryOptionWithPreferredGroup.put("orderedPreferredReplicas", "0");
    assertEquals(BrokerMetrics.getTagForPreferredGroup(queryOptionWithPreferredGroup), "preferredGroupOptSet",
        "Should return preferredGroupOptSet when queryOption contains ORDERED_PREFERRED_REPLICAS");
  }
}
