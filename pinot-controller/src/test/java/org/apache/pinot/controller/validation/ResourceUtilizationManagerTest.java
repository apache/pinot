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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.controller.ControllerConf;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ResourceUtilizationManagerTest {

  private DiskUtilizationChecker _diskUtilizationChecker;
  private PrimaryKeyCountChecker _primaryKeyCountChecker;
  private List<UtilizationChecker> _utilizationCheckerList;
  private ControllerConf _controllerConf;
  private ResourceUtilizationManager _resourceUtilizationManager;
  private final String _testTable = "myTable_OFFLINE";

  @BeforeMethod
  public void setUp() {
    _diskUtilizationChecker = Mockito.mock(DiskUtilizationChecker.class);
    _primaryKeyCountChecker = Mockito.mock(PrimaryKeyCountChecker.class);
    _utilizationCheckerList = Arrays.asList(_diskUtilizationChecker, _primaryKeyCountChecker);
    _controllerConf = Mockito.mock(ControllerConf.class);
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckIsDisabled() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(false);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    boolean result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, false);
    Assert.assertTrue(result, "Resource utilization should be within limits when the check is disabled");

    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, true);
    Assert.assertTrue(result, "Resource utilization should be within limits when the check is disabled");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithNullTableName() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits(null, false);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithNullTableNameSkipRealtimeIngestionFalse() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits(null, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithEmptyTableName() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits("", false);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithEmptyTableNameSkipRealtimeIngestionFalse() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits("", true);
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckIsEnabled() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable, false)).thenReturn(true);
    Mockito.when(_primaryKeyCountChecker.isResourceUtilizationWithinLimits(_testTable, false)).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    boolean result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, false);
    Assert.assertTrue(result, "Resource utilization should be within limits when disk check and primary key count "
        + "check returns true");

    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable, true)).thenReturn(true);
    Mockito.when(_primaryKeyCountChecker.isResourceUtilizationWithinLimits(_testTable, true)).thenReturn(true);
    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, true);
    Assert.assertTrue(result, "Resource utilization should be within limits when disk check and primary key count "
        + "check returns true");
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckFails() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable, false)).thenReturn(false);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    boolean result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, false);
    Assert.assertFalse(result, "Resource utilization should not be within limits when disk check returns false");

    Mockito.when(_primaryKeyCountChecker.isResourceUtilizationWithinLimits(_testTable, true)).thenReturn(true);
    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, true);
    Assert.assertFalse(result, "Resource utilization should not be within limits when disk check returns false");
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckFailsPrimaryKey() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable, false)).thenReturn(true);
    Mockito.when(_primaryKeyCountChecker.isResourceUtilizationWithinLimits(_testTable, false)).thenReturn(false);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckerList);

    boolean result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, false);
    Assert.assertFalse(result, "Resource utilization should not be within limits when primary key count check returns "
        + "false");

    Mockito.when(_primaryKeyCountChecker.isResourceUtilizationWithinLimits(_testTable, true)).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable, true)).thenReturn(true);
    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable, true);
    Assert.assertTrue(result, "Resource utilization should be within limits when primary key count check returns "
        + "true for isForMinion = true");
  }
}
