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

import java.util.List;
import org.apache.pinot.controller.ControllerConf;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ResourceUtilizationManagerTest {

  private DiskUtilizationChecker _diskUtilizationChecker;
  private List<UtilizationChecker> _utilizationCheckers;
  private ControllerConf _controllerConf;
  private ResourceUtilizationManager _resourceUtilizationManager;
  private final String _testTable = "myTable_OFFLINE";

  @BeforeMethod
  public void setUp() {
    _diskUtilizationChecker = Mockito.mock(DiskUtilizationChecker.class);
    _utilizationCheckers = List.of(_diskUtilizationChecker);
    _controllerConf = Mockito.mock(ControllerConf.class);
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckIsDisabled() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(false);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    UtilizationChecker.CheckResult result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.PASS,
        "Resource utilization should be within limits when the check is disabled");

    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.TASK_GENERATION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.PASS,
        "Resource utilization should be within limits when the check is disabled");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithNullTableName() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits(null,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithNullTableNameIsForMinionTrue() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits(null,
        UtilizationChecker.CheckPurpose.TASK_GENERATION);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithEmptyTableName() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits("",
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIsResourceUtilizationWithinLimitsWithEmptyTableNameIsForMinionTrue() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    _resourceUtilizationManager.isResourceUtilizationWithinLimits("",
        UtilizationChecker.CheckPurpose.TASK_GENERATION);
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckIsEnabled() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION)).thenReturn(UtilizationChecker.CheckResult.PASS);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    UtilizationChecker.CheckResult result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.PASS,
        "Resource utilization should be within limits when disk check and primary key count check returns true");

    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.TASK_GENERATION)).thenReturn(UtilizationChecker.CheckResult.PASS);
    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.TASK_GENERATION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.PASS,
        "Resource utilization should be within limits when disk check and primary key count check returns true");
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckFails() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION)).thenReturn(UtilizationChecker.CheckResult.FAIL);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.TASK_GENERATION)).thenReturn(UtilizationChecker.CheckResult.FAIL);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    UtilizationChecker.CheckResult result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.FAIL,
        "Resource utilization should not be within limits when disk check returns false");

    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.TASK_GENERATION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.FAIL,
        "Resource utilization should not be within limits when disk check returns false");
  }

  @Test
  public void testIsResourceUtilizationWithinLimitsWhenCheckStale() {
    Mockito.when(_controllerConf.isResourceUtilizationCheckEnabled()).thenReturn(true);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION)).thenReturn(UtilizationChecker.CheckResult.UNDETERMINED);
    Mockito.when(_diskUtilizationChecker.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.TASK_GENERATION)).thenReturn(UtilizationChecker.CheckResult.UNDETERMINED);
    _resourceUtilizationManager = new ResourceUtilizationManager(_controllerConf, _utilizationCheckers);

    UtilizationChecker.CheckResult result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.UNDETERMINED,
        "Resource utilization should return STALE when the diskUtilization returns STALE");

    result = _resourceUtilizationManager.isResourceUtilizationWithinLimits(_testTable,
        UtilizationChecker.CheckPurpose.TASK_GENERATION);
    Assert.assertEquals(result, UtilizationChecker.CheckResult.UNDETERMINED,
        "Resource utilization should return STALE when the diskUtilization returns STALE");
  }
}
