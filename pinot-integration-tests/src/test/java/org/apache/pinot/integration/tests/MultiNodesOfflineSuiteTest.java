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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.integration.tests.MultiNodesOfflineSuiteFixtures.Scenarios;
import org.apache.pinot.integration.tests.suites.MultiNodesOfflineSuite;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.SelectClasses;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Verifies the focused suite's automatic test selection without starting a Pinot cluster.
/// Each test owns its TestNG runner and fixtures; no state is shared between runs.
public class MultiNodesOfflineSuiteTest {
  @Test
  public void testSuiteSelectsClassAndRegistersFilter() {
    assertEquals(MultiNodesOfflineSuite.class.getAnnotation(SelectClasses.class).value(),
        new Class<?>[]{MultiNodesOfflineClusterIntegrationTest.class});
    assertTrue(Arrays.stream(MultiNodesOfflineSuite.class.getAnnotationsByType(ConfigurationParameter.class))
        .anyMatch(parameter -> parameter.key().equals("testng.listeners")
            && parameter.value().equals(MultiNodesOfflineSuite.class.getName())));
  }

  @Test
  public void testDeclaredTestsKeepInheritedFixture() {
    TestNG testNG = new TestNG(false);
    testNG.setTestClasses(new Class<?>[]{Scenarios.class});
    testNG.setVerbose(0);
    testNG.addListener(new MultiNodesOfflineSuite());
    TestListenerAdapter results = new TestListenerAdapter();
    testNG.addListener(results);
    testNG.run();

    assertFalse(testNG.hasFailure());
    assertEquals(results.getPassedTests().stream().map(result -> result.getMethod().getMethodName())
        .collect(Collectors.toSet()), Set.of("existingScenario", "additionalScenario"));
    assertTrue(results.getSkippedTests().isEmpty());
    Scenarios scenarios = (Scenarios) results.getPassedTests().getFirst().getInstance();
    assertEquals(scenarios._setups, 1);
    assertEquals(scenarios._teardowns, 1);
  }
}
