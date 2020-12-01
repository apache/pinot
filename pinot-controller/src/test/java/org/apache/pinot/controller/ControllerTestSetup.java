package org.apache.pinot.controller;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import static org.apache.pinot.controller.ControllerTestUtils.*;


/**
 * All test cases in {@link org.apache.pinot.controller} package are run as part the Default TestNG suite. This class
 * is used to initialize the test state in the beginning (before any test cases have run) and to deinitialize the state
 * in the end (after all the test cases have run).
 */
public class ControllerTestSetup {
  @BeforeSuite
  public void suiteSetup() throws Exception {
    startSuiteRun();
  }

  @AfterSuite
  public void tearDownSuite() {
    stopSuiteRun();
  }
}
