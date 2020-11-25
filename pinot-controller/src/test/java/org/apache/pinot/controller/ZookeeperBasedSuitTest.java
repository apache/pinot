package org.apache.pinot.controller;

import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;


public class ZookeeperBasedSuitTest extends ControllerTest {
  @BeforeSuite
  public void suiteSetup() {
    startZk();
  }

  @AfterSuite
  public void tearDownSuite() {
    stopZk();
  }
}
