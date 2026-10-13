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
package org.apache.pinot.integration.tests;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/// Holds fixtures outside the test class so TestNG does not recursively discover them as part of the outer test run.
/// Each nested fixture instance is confined to its own runner.
public final class MultiNodesOfflineSuiteFixtures {
  private MultiNodesOfflineSuiteFixtures() {
  }

  /// Represents the inherited offline fixture and its already-covered tests; each runner creates its own instance.
  public static class OfflineScenarios {
    protected int _setups;
    protected int _teardowns;

    @BeforeClass
    public void setUp() {
      _setups++;
    }

    @AfterClass
    public void tearDown() {
      _teardowns++;
    }

    @Test
    public void inheritedScenario() {
      fail("The focused suite must not repeat inherited offline tests");
    }
  }

  /// Represents new multi-node tests, discovered without a method allowlist; instances are confined to one runner.
  public static class Scenarios extends OfflineScenarios {
    @Test
    public void existingScenario() {
      assertEquals(_setups, 1);
    }

    @Test
    public void additionalScenario() {
      assertEquals(_setups, 1);
    }

    @Test(enabled = false)
    public void disabledScenario() {
      fail("Disabled tests must stay disabled");
    }
  }
}
