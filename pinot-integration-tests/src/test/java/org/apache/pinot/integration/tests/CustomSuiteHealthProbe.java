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
import java.util.List;
import org.apache.pinot.integration.tests.custom.CustomSuiteHealthCheckedIntegrationTest;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Subprocess-only TestNG fixture used to test custom-suite health handling.
public final class CustomSuiteHealthProbe {
  private CustomSuiteHealthProbe() {
  }

  private static RuntimeException _cleanupFailure;
  private static boolean _laterMethodRan;
  private static boolean _laterClassRan;
  private static boolean _laterClassSetupRan;
  private static boolean _suiteTearDownRan;
  private static boolean _failureVisibleDuringTearDown;

  public static void main(String[] args) {
    assertEquals(args.length, 1, "Expected a failure policy");
    switch (args[0]) {
      case "continue":
        verifyCleanupFailure(XmlSuite.FailurePolicy.CONTINUE);
        break;
      case "default":
        verifyCleanupFailure(null);
        break;
      default:
        throw new IllegalArgumentException("Unexpected failure policy: " + args[0]);
    }
  }

  private static void verifyCleanupFailure(XmlSuite.FailurePolicy policy) {
    _cleanupFailure = new RuntimeException("injected cleanup failure");
    _laterMethodRan = false;
    _laterClassRan = false;
    _laterClassSetupRan = false;
    _suiteTearDownRan = false;
    _failureVisibleDuringTearDown = false;
    XmlSuite suite = suite("injected-cleanup-failure", FailingParticipant.class, LaterParticipant.class);
    if (policy != null) {
      suite.setConfigFailurePolicy(policy);
    }
    TestListenerAdapter listener = run(suite);

    assertTrue(listener.getConfigurationFailures().stream()
        .anyMatch(result -> result.getThrowable() == _cleanupFailure), "Original teardown failure must be reported");
    assertTrue(listener.getConfigurationFailures().stream()
        .anyMatch(result -> result.getThrowable().getCause() == _cleanupFailure),
        "A health check should retain the cleanup failure as its cause");
    assertTrue(listener.getSkippedTests().stream().anyMatch(result -> result.getName().equals("laterMethod")));
    assertTrue(listener.getSkippedTests().stream().anyMatch(result -> result.getName().equals("laterClass")));
    assertFalse(_laterMethodRan);
    assertFalse(_laterClassRan);
    assertFalse(_laterClassSetupRan, "The next class must not mutate the contaminated cluster during setup");
    assertTrue(_suiteTearDownRan, "Suite teardown must run even after cleanup fails");
    assertTrue(_failureVisibleDuringTearDown, "The guard must remain active until suite teardown");
    FailingParticipant.assertHealthyAfterSuite();

    // A later, independent suite in the same JVM must not inherit the previous failure.
    TestListenerAdapter next = run(suite("independent-suite", LaterParticipant.class));
    assertTrue(next.getConfigurationFailures().isEmpty());
    assertTrue(next.getFailedTests().isEmpty());
    assertTrue(_laterClassRan, "The next suite must start with a healthy guard");
  }

  private static XmlSuite suite(String name, Class<?>... classes) {
    XmlSuite suite = new XmlSuite();
    suite.setName(name);
    suite.setPreserveOrder(true);
    XmlTest test = new XmlTest(suite);
    test.setName(name);
    test.setXmlClasses(Arrays.stream(classes).map(XmlClass::new).toList());
    return suite;
  }

  private static TestListenerAdapter run(XmlSuite suite) {
    TestListenerAdapter listener = new TestListenerAdapter();
    TestNG testng = new TestNG(false);
    testng.setUseDefaultListeners(false);
    testng.setVerbose(0);
    testng.setXmlSuites(List.of(suite));
    testng.addListener(listener);
    testng.run();
    return listener;
  }

  public static class FailingParticipant extends CustomSuiteHealthCheckedIntegrationTest {
    @Test
    public void firstMethod() {
    }

    @AfterMethod(alwaysRun = true)
    public void failCleanup(ITestResult result) {
      if (result.getMethod().getMethodName().equals("firstMethod")) {
        recordCustomSuiteCleanupFailure(_cleanupFailure);
        throw _cleanupFailure;
      }
    }

    @Test(dependsOnMethods = "firstMethod", alwaysRun = true)
    public void laterMethod() {
      _laterMethodRan = true;
    }

    @AfterSuite(alwaysRun = true)
    public void finishSuite() {
      _suiteTearDownRan = true;
      try {
        assertCustomSuiteHealthy();
      } catch (IllegalStateException e) {
        _failureVisibleDuringTearDown = e.getCause() == _cleanupFailure;
      }
    }

    public static void assertHealthyAfterSuite() {
      assertCustomSuiteHealthy();
    }
  }

  public static class LaterParticipant extends CustomSuiteHealthCheckedIntegrationTest {
    @BeforeClass
    public void prepareLaterClass() {
      _laterClassSetupRan = true;
    }

    @Test
    public void laterClass() {
      _laterClassRan = true;
    }
  }
}
