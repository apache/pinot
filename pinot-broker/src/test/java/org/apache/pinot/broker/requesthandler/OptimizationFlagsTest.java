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

import java.util.Collections;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.requesthandler.FlattenNestedPredicatesFilterQueryTreeOptimizer;
import org.apache.pinot.core.requesthandler.OptimizationFlags;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the optimization flags;
 */
public class OptimizationFlagsTest {
  @Test
  public void testOptimizationName() {
    Assert.assertEquals(OptimizationFlags.optimizationName(FlattenNestedPredicatesFilterQueryTreeOptimizer.class),
        "flattenNestedPredicates");
  }

  @Test
  public void testEnableOptimizations() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setDebugOptions(Collections.singletonMap("optimizationFlags", "+foo, +bar"));

    OptimizationFlags optimizationFlags = OptimizationFlags.getOptimizationFlags(brokerRequest);
    Assert.assertTrue(optimizationFlags.isOptimizationEnabled("foo"));
    Assert.assertTrue(optimizationFlags.isOptimizationEnabled("bar"));
    Assert.assertFalse(optimizationFlags.isOptimizationEnabled("baz"));
  }

  @Test
  public void testDisableOptimizations() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setDebugOptions(Collections.singletonMap("optimizationFlags", "-foo"));

    OptimizationFlags optimizationFlags = OptimizationFlags.getOptimizationFlags(brokerRequest);
    Assert.assertFalse(optimizationFlags.isOptimizationEnabled("foo"));
    Assert.assertTrue(optimizationFlags.isOptimizationEnabled("bar"));
    Assert.assertTrue(optimizationFlags.isOptimizationEnabled("baz"));
  }

  @Test
  public void testForbidEnableAndDisableTogether() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setDebugOptions(Collections.singletonMap("optimizationFlags", "-foo, +bar"));

    try {
      OptimizationFlags optimizationFlags = OptimizationFlags.getOptimizationFlags(brokerRequest);
      Assert.fail("Expected exception");
    } catch (Exception e) {
      // Expected
    }
  }
}
