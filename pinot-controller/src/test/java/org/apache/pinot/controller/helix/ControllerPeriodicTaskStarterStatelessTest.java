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

import java.util.List;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "stateless")
public class ControllerPeriodicTaskStarterStatelessTest extends ControllerTest {

  @BeforeClass
  public void setup() {
    startZk();
  }

  /**
   * Test that controller starts up and helixResourceManager is non null before initiating periodic tasks
   */
  @Test
  public void testHelixResourceManagerDuringControllerStart()
      throws Exception {
    startController();
    stopController();
  }

  @AfterClass
  public void teardown() {
    stopZk();
  }

  @Override
  public ControllerStarter getControllerStarter() {
    return new MockControllerStarter();
  }

  private class MockControllerStarter extends ControllerStarter {
    private static final int NUM_PERIODIC_TASKS = 11;

    public MockControllerStarter() {
      super();
    }

    @Override
    protected List<PeriodicTask> setupControllerPeriodicTasks() {
      PinotHelixResourceManager helixResourceManager = getHelixResourceManager();
      Assert.assertNotNull(helixResourceManager);
      Assert.assertNotNull(helixResourceManager.getHelixAdmin());
      Assert.assertNotNull(helixResourceManager.getHelixZkManager());
      Assert.assertNotNull(helixResourceManager.getHelixClusterName());
      Assert.assertNotNull(helixResourceManager.getPropertyStore());

      List<PeriodicTask> controllerPeriodicTasks = super.setupControllerPeriodicTasks();
      Assert.assertNotNull(controllerPeriodicTasks);
      Assert.assertEquals(controllerPeriodicTasks.size(), NUM_PERIODIC_TASKS);
      return controllerPeriodicTasks;
    }
  }
}
