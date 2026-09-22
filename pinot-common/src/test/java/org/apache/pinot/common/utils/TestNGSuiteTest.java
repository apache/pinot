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
package org.apache.pinot.common.utils;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.SelectMethod;
import org.junit.platform.suite.api.Suite;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.launcher.EngineFilter.includeEngines;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Verifies the native JUnit Platform suites used to group TestNG tests without XML suite files.
/// Not thread-safe: the fixtures share lifecycle events and must execute sequentially.
public class TestNGSuiteTest {
  private static final List<String> EVENTS = new ArrayList<>();

  @BeforeEach
  public void resetEvents() {
    EVENTS.clear();
  }

  @Test
  public void testSharedLifecycleAndClassOrder() {
    TestExecutionSummary summary = execute(SharedSuite.class);
    assertEquals(summary.getTotalFailureCount(), 0L, summary.getFailures().toString());
    assertEquals(summary.getTestsSucceededCount(), 2L);
    assertEquals(EVENTS, List.of("start", "first", "second", "finish"));
  }

  @Test
  public void testExcludedGroups() {
    TestExecutionSummary summary = execute(StatefulSuite.class);
    assertEquals(summary.getTotalFailureCount(), 0L, summary.getFailures().toString());
    assertEquals(summary.getTestsSucceededCount(), 1L);
    assertEquals(EVENTS, List.of("start", "stateful", "finish"));
  }

  @Test
  public void testIncludedGroupsAndRepeatedDataProviderInvocations() {
    TestExecutionSummary summary = execute(StatelessSuite.class);
    assertEquals(summary.getTotalFailureCount(), 0L, summary.getFailures().toString());
    assertEquals(summary.getTestsSucceededCount(), 4L);
    assertEquals(EVENTS, List.of("start", "data-1", "data-2", "data-1", "data-2", "finish"));
  }

  @Test
  public void testMethodSelection() {
    TestExecutionSummary summary = execute(MethodSuite.class);
    assertEquals(summary.getTotalFailureCount(), 0L, summary.getFailures().toString());
    assertEquals(summary.getTestsSucceededCount(), 1L);
    assertEquals(EVENTS, List.of("start", "stateful", "finish"));
  }

  @Test
  public void testFailurePropagationAndTeardown() {
    TestExecutionSummary summary = execute(FailingSuite.class);
    assertEquals(summary.getTestsFailedCount(), 1L);
    assertEquals(summary.getTotalFailureCount(), 1L);
    assertTrue(summary.getFailures().get(0).getException().getMessage().contains("expected fixture failure"));
    assertEquals(EVENTS, List.of("start", "finish"));
  }

  private static TestExecutionSummary execute(Class<?> suite) {
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    LauncherFactory.create().execute(LauncherDiscoveryRequestBuilder.request()
        .selectors(selectClass(suite)).filters(includeEngines("junit-platform-suite")).build(), listener);
    return listener.getSummary();
  }

  /// Stateless suite declaration that preserves the shared lifecycle and class order.
  @Suite
  @IncludeEngines("testng")
  @SelectClasses({ZFirstFixture.class, ASecondFixture.class})
  public static class SharedSuite {
  }

  /// Stateless suite declaration excluding the stateless group.
  @Suite
  @IncludeEngines("testng")
  @SelectClasses(GroupedFixture.class)
  @ConfigurationParameter(key = "testng.excludedGroups", value = "stateless")
  public static class StatefulSuite {
  }

  /// Stateless suite declaration including only the stateless group.
  @Suite
  @IncludeEngines("testng")
  @SelectClasses(GroupedFixture.class)
  @ConfigurationParameter(key = "testng.groups", value = "stateless")
  public static class StatelessSuite {
  }

  /// Stateless suite declaration selecting one method from a class.
  @Suite
  @IncludeEngines("testng")
  @SelectMethod(type = GroupedFixture.class, name = "stateful")
  public static class MethodSuite {
  }

  /// Stateless suite declaration containing an intentional failure.
  @Suite
  @IncludeEngines("testng")
  @SelectClasses(FailingFixture.class)
  public static class FailingSuite {
  }

  /// Shared lifecycle fixture; accesses the enclosing test's sequential event log.
  public static class SharedFixture {
    @BeforeSuite(alwaysRun = true)
    public void start() {
      assertTrue(EVENTS.isEmpty(), "Suite setup must run exactly once");
      EVENTS.add("start");
    }

    @AfterSuite(alwaysRun = true)
    public void finish() {
      EVENTS.add("finish");
    }
  }

  /// First sequential fixture sharing its suite's lifecycle.
  public static class ZFirstFixture extends SharedFixture {
    @org.testng.annotations.Test
    public void first() {
      assertEquals(EVENTS, List.of("start"));
      EVENTS.add("first");
    }
  }

  /// Second sequential fixture sharing its suite's lifecycle.
  public static class ASecondFixture extends SharedFixture {
    @org.testng.annotations.Test
    public void second() {
      assertEquals(EVENTS, List.of("start", "first"));
      EVENTS.add("second");
    }
  }

  /// Sequential fixture with distinct groups and repeated parameterized invocations.
  public static class GroupedFixture extends SharedFixture {
    @org.testng.annotations.Test
    public void stateful() {
      EVENTS.add("stateful");
    }

    @org.testng.annotations.Test(groups = "stateless", dataProvider = "values", invocationCount = 2)
    public void stateless(int value) {
      EVENTS.add("data-" + value);
    }

    @DataProvider
    public Object[][] values() {
      return new Object[][]{{1}, {2}};
    }
  }

  /// Sequential fixture verifying that native test failures reach the platform launcher.
  public static class FailingFixture extends SharedFixture {
    @org.testng.annotations.Test
    public void failure() {
      fail("expected fixture failure");
    }
  }
}
