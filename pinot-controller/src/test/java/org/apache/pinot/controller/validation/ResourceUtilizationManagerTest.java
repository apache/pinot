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
package org.apache.pinot.controller.validation;

import org.apache.pinot.controller.ControllerConf;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ResourceUtilizationManagerTest {

  private DiskUtilizationChecker _diskUtilizationChecker;
  private ControllerConf _controllerConf;
  private ResourceUtilizationManager _resourceUtilizationManager;
  private final String _testTable = "myTable_OFFLINE";

  @BeforeMethod
  public void setUp() {
    _diskUtilizationChecker = Mockito.mock(DiskUtilizationChecker.class);
    _controllerConf = Mockito.mock(ControllerConf.class);
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckIsDisabled() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(false);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _diskUtilizationChecker);

    boolean result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable);
    Assert.assertTrue(result, "Resource utilization should be within limits when the check is disabled");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithNullTableName() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _diskUtilizationChecker);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithEmptyTableName() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _diskUtilizationChecker);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits("");
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckIsEnabled() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isDiskUtilizationWithinLimits(_testTable)).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _diskUtilizationChecker);

    boolean result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable);
    Assert.assertTrue(result, "Resource utilization should be within limits when disk check returns true");
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckFails() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isDiskUtilizationWithinLimits(_testTable)).thenReturn(false);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _diskUtilizationChecker);

    boolean result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable);
    Assert.assertFalse(result, "Resource utilization should not be within limits when disk check returns false");
  }
}
