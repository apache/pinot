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
package org.apache.pinot.controller;

import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;


/**
 * All test cases in {@link org.apache.pinot.controller} package are run as part a TestNG group (see testng_*.xml).
 * This helps to setup (see {@link #setUpGroup()} and tear down (see {@link #tearDownGroup()} the shared state before
 * and after all tests are run. Each test case class should implement a @BeforeClass method, which would call
 * @link ControllerTest.getInstance()#validate()} method to validate shared state. Each test case class should also
 * implement @AfterClass method where {@link ControllerTest#cleanup()} would be called to cleanup shared state.
 * Additional cleanup may be needed depending upon test functionality.
 */
public class ControllerTestSetup {
  /**
   * TestNG will run this method once before all the test cases are run. We use this method to initialize
   * common state for all the test cases.
   */
  @BeforeGroups
  public void setUpGroup()
      throws Exception {
    ControllerTest.getInstance().startSharedTestSetup();
  }

  /**
   * TestNG will run this method once after all the test cases have run. We use this method to de-initialize
   * common state used by all the test cases.
   */
  @AfterGroups
  public void tearDownGroup() {
    ControllerTest.getInstance().stopSharedTestSetup();
  }
}
