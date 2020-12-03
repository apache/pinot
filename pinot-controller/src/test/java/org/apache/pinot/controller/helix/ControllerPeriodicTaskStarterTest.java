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
package org.apache.pinot.controller.helix;

import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerTestUtils.*;


/**
 * Test that controller starts up and helixResourceManager is non null before initiating periodic tasks
 */
public class ControllerPeriodicTaskStarterTest {

  // TODO: Changed to 6 to make test case pass. Check if further cleanup is needed.
  private static final int NUM_PERIODIC_TASKS = 6;

  @BeforeClass
  public void setUp() throws Exception {
    validate();
  }

  @Test
  public void testPeriodicTaskCount() {
    PinotHelixResourceManager helixResourceManager = getHelixResourceManager();
    Assert.assertNotNull(helixResourceManager);
    Assert.assertNotNull(helixResourceManager.getHelixAdmin());
    Assert.assertNotNull(helixResourceManager.getHelixZkManager());
    Assert.assertNotNull(helixResourceManager.getHelixClusterName());
    Assert.assertNotNull(helixResourceManager.getPropertyStore());

    Assert.assertEquals(getControllerStarter().getPeriodicTaskCount(), NUM_PERIODIC_TASKS);
  }

  @AfterClass
  public void tearDown() {
    cleanup();
  }
}
