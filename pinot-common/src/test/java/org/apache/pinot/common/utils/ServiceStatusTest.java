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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Test for the service status.
 */
public class ServiceStatusTest {
  private static final ServiceStatus.ServiceStatusCallback ALWAYS_GOOD = new ServiceStatus.ServiceStatusCallback() {
    @Override
    public ServiceStatus.Status getServiceStatus() {
      return ServiceStatus.Status.GOOD;
    }

    @Override
    public String getStatusDescription() {
      return ServiceStatus.STATUS_DESCRIPTION_NONE;
    }
  };

  private static final ServiceStatus.ServiceStatusCallback ALWAYS_STARTING = new ServiceStatus.ServiceStatusCallback() {
    @Override
    public ServiceStatus.Status getServiceStatus() {
      return ServiceStatus.Status.STARTING;
    }

    @Override
    public String getStatusDescription() {
      return ServiceStatus.STATUS_DESCRIPTION_NONE;
    }
  };

  private static final ServiceStatus.ServiceStatusCallback ALWAYS_BAD = new ServiceStatus.ServiceStatusCallback() {
    @Override
    public ServiceStatus.Status getServiceStatus() {
      return ServiceStatus.Status.BAD;
    }

    @Override
    public String getStatusDescription() {
      return ServiceStatus.STATUS_DESCRIPTION_NONE;
    }
  };

  public static final String TABLE_NAME = "myTable_OFFLINE";
  public static final String INSTANCE_NAME = "Server_1.2.3.4_1234";

  private static final String CHARS_IN_RANDOM_TABLE_NAME =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private static Random _random;

  @BeforeClass
  public void setUp() {
    long seed = System.currentTimeMillis();
    _random = new Random(seed);
    // Printing to sysout so that we can re-generate failure cases.
    System.out.println(ServiceStatusTest.class.getSimpleName() + ":Using random number seed " + seed);
  }

  @Test
  public void testMultipleServiceStatusCallback() {
    // Only good should return good
    ServiceStatus.MultipleCallbackServiceStatusCallback onlyGood =
        new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(ALWAYS_GOOD));

    assertEquals(onlyGood.getServiceStatus(), ServiceStatus.Status.GOOD);

    // Only bad should return bad
    ServiceStatus.MultipleCallbackServiceStatusCallback onlyBad =
        new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(ALWAYS_BAD));

    assertEquals(onlyBad.getServiceStatus(), ServiceStatus.Status.BAD);

    // Only starting should return starting
    ServiceStatus.MultipleCallbackServiceStatusCallback onlyStarting =
        new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(ALWAYS_STARTING));

    assertEquals(onlyStarting.getServiceStatus(), ServiceStatus.Status.STARTING);

    // Good + starting = starting
    ServiceStatus.MultipleCallbackServiceStatusCallback goodAndStarting =
        new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(ALWAYS_GOOD, ALWAYS_STARTING));

    assertEquals(goodAndStarting.getServiceStatus(), ServiceStatus.Status.STARTING);

    // Good + starting + bad = starting (check for left-to-right evaluation)
    ServiceStatus.MultipleCallbackServiceStatusCallback goodStartingAndBad =
        new ServiceStatus.MultipleCallbackServiceStatusCallback(
            ImmutableList.of(ALWAYS_GOOD, ALWAYS_STARTING, ALWAYS_BAD));

    assertEquals(goodStartingAndBad.getServiceStatus(), ServiceStatus.Status.STARTING);
  }

  @Test
  public void testIdealStateMatch() {
    TestIdealStateAndExternalViewMatchServiceStatusCallback callback;

    // No ideal state = GOOD
    callback = buildTestISEVCallback();
    callback.setExternalView(new ExternalView(TABLE_NAME));
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.GOOD);

    // No external view = STARTING
    callback = buildTestISEVCallback();
    callback.setIdealState(new IdealState(TABLE_NAME));
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.STARTING);

    // Empty ideal state + empty external view = GOOD
    callback = buildTestISEVCallback();
    callback.setIdealState(new IdealState(TABLE_NAME));
    callback.setExternalView(new ExternalView(TABLE_NAME));
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.GOOD);

    // Once the status is GOOD, it should keep on reporting GOOD no matter what
    callback.setIdealState(null);
    callback.setExternalView(null);
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.GOOD);

    // Non empty ideal state + empty external view = STARTING
    callback = buildTestISEVCallback();
    IdealState idealState = new IdealState(TABLE_NAME);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setPartitionState("mySegment", INSTANCE_NAME, "ONLINE");
    callback.setIdealState(idealState);
    callback.setExternalView(new ExternalView(TABLE_NAME));
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.STARTING);

    // Should be good if the only ideal state is disabled
    callback.getResourceIdealState(TABLE_NAME).enable(false);
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.GOOD);

    // Should ignore offline segments in ideal state
    callback = buildTestISEVCallback();
    idealState = new IdealState(TABLE_NAME);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setPartitionState("mySegment_1", INSTANCE_NAME, "ONLINE");
    idealState.setPartitionState("mySegment_2", INSTANCE_NAME, "OFFLINE");
    callback.setIdealState(idealState);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    externalView.setState("mySegment_1", INSTANCE_NAME, "ONLINE");
    callback.setExternalView(externalView);
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.GOOD);

    // Should ignore segments in error state in external view
    callback = buildTestISEVCallback();
    idealState = new IdealState(TABLE_NAME);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setPartitionState("mySegment_1", INSTANCE_NAME, "ONLINE");
    idealState.setPartitionState("mySegment_2", INSTANCE_NAME, "OFFLINE");
    callback.setIdealState(idealState);
    externalView = new ExternalView(TABLE_NAME);
    externalView.setState("mySegment_1", INSTANCE_NAME, "ERROR");
    callback.setExternalView(externalView);
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.GOOD);

    // Should ignore other instances
    callback = buildTestISEVCallback();
    idealState = new IdealState(TABLE_NAME);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setPartitionState("mySegment_1", INSTANCE_NAME, "ONLINE");
    idealState.setPartitionState("mySegment_2", INSTANCE_NAME + "2", "ONLINE");
    callback.setIdealState(idealState);
    externalView = new ExternalView(TABLE_NAME);
    externalView.setState("mySegment_1", INSTANCE_NAME, "ONLINE");
    externalView.setState("mySegment_2", INSTANCE_NAME + "2", "OFFLINE");
    callback.setExternalView(externalView);
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.GOOD);
  }

  private TestIdealStateAndExternalViewMatchServiceStatusCallback buildTestISEVCallback() {
    return new TestIdealStateAndExternalViewMatchServiceStatusCallback("potato", INSTANCE_NAME,
        Collections.singletonList(TABLE_NAME));
  }

  private String generateRandomString(int len) {
    final int numChars = CHARS_IN_RANDOM_TABLE_NAME.length();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < len; i++) {
      builder.append(CHARS_IN_RANDOM_TABLE_NAME.charAt(_random.nextInt(numChars)));
    }
    return builder.toString();
  }

  // Create a ServiceStatus object that monitors about 2500 tables
  // and returns
  @Test
  public void testMultipleResourcesAndPercent()
      throws Exception {
    testMultipleResourcesAndPercent(98.6);
    testMultipleResourcesAndPercent(99.3);
    testMultipleResourcesAndPercent(99.5);
    testMultipleResourcesAndPercent(99.7);
    testMultipleResourcesAndPercent(99.95);
  }

  private void testMultipleResourcesAndPercent(double percentReady) {
    final long now = System.currentTimeMillis();
    _random = new Random(now);
    final String clusterName = "noSuchCluster";
    final List<String> tables = new ArrayList<>();
    final int tableCount = 2500 + _random.nextInt(100);
    int readyTables = 0;
    Map<String, IdealState> idealStates = new HashMap<>();
    Map<String, ExternalView> externalViews = new HashMap<>();

    for (int i = 1; i <= tableCount; i++) {
      final String tableName = generateRandomString(10) + String.valueOf(i);
      tables.add(tableName);
      final String segmentName = "segment1";
      String evState;
      if (_random.nextDouble() * 100 < percentReady) {
        evState = "ONLINE";
        readyTables++;
      } else {
        evState = "OFFLINE";
      }
      IdealState idealState = new IdealState(tableName);
      idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
      idealState.setPartitionState(segmentName, INSTANCE_NAME, "ONLINE");
      ExternalView externalView = new ExternalView(tableName);
      externalView.setState(segmentName, INSTANCE_NAME, evState);
      idealStates.put(tableName, idealState);
      externalViews.put(tableName, externalView);
    }

    final double actualReadyPercent = (double) readyTables * 100 / tableCount;
    double lowestReadyPercent = (int) Math.round(actualReadyPercent);
    lowestReadyPercent = lowestReadyPercent > 2 ? lowestReadyPercent - 2 : 1;  // Should be 2 below  percentReady

    // Create ServiceCallback objects with minReadyPercent set to values between lowestReadyPercent and 100.
    // Call getServiceStatus() enough number of times so that we are only left with the tables
    // that are not ready yet. We need to call getServiceStatus() at most tableCount times.
    for (double minReadyPercent = lowestReadyPercent; minReadyPercent <= 100; minReadyPercent += 0.1) {
      TestMultiResourceISAndEVMatchCB callback =
          new TestMultiResourceISAndEVMatchCB(clusterName, INSTANCE_NAME, tables, minReadyPercent);
      callback.setIdealStates(idealStates);
      callback.setExternalViews(externalViews);

      ServiceStatus.Status status = callback.getServiceStatus();
      // we need to call getServiceStatus() at most the number of bad tables plus 1,
      // to get a STARTED condition if we can. After that, the return value should
      // never change.
      final int nBadTables = tableCount - readyTables;
      for (int i = 0; i <= nBadTables; i++) {
        status = callback.getServiceStatus();
      }

      ServiceStatus.Status expected =
          minReadyPercent > actualReadyPercent ? ServiceStatus.Status.STARTING : ServiceStatus.Status.GOOD;
      String errorMsg = "Mismatch at " + minReadyPercent + "%, tableCount=" + tableCount + ", percentTablesReady="
          + actualReadyPercent + ":" + callback.getStatusDescription();
      Assert.assertEquals(status, expected, errorMsg);

      // The status should never change going forward from here.
      for (int i = nBadTables + 1; i < tableCount; i++) {
        ServiceStatus.Status laterStatus = callback.getServiceStatus();
        String msg = "Mismatch at " + minReadyPercent + "%, tableCount=" + tableCount + ", percentTablesReady="
            + actualReadyPercent + ", i=" + i + ":" + callback.getStatusDescription();
        Assert.assertEquals(laterStatus, status, msg);
      }
    }
  }

  private static class TestIdealStateAndExternalViewMatchServiceStatusCallback extends ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback {
    private IdealState _idealState;
    private ExternalView _externalView;

    public TestIdealStateAndExternalViewMatchServiceStatusCallback(String clusterName, String instanceName,
        List<String> resourcesToMonitor) {
      super(Mockito.mock(HelixManager.class), clusterName, instanceName, resourcesToMonitor, 100.0);
    }

    @Override
    public IdealState getResourceIdealState(String resourceName) {
      return _idealState;
    }

    @Override
    public ExternalView getState(String resourceName) {
      return _externalView;
    }

    public void setIdealState(IdealState idealState) {
      _idealState = idealState;
    }

    public void setExternalView(ExternalView externalView) {
      _externalView = externalView;
    }
  }

  private static class TestMultiResourceISAndEVMatchCB extends ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback {
    public Map<String, IdealState> _idealStates = new HashMap<>();
    public Map<String, ExternalView> _externalViews = new HashMap<>();

    public TestMultiResourceISAndEVMatchCB(String clusterName, String instanceName, List<String> resourcesToMonitor,
        double minResourcesPercent) {
      super(Mockito.mock(HelixManager.class), clusterName, instanceName, resourcesToMonitor, minResourcesPercent);
    }

    @Override
    public IdealState getResourceIdealState(String resourceName) {
      return _idealStates.get(resourceName);
    }

    @Override
    public ExternalView getState(String resourceName) {
      return _externalViews.get(resourceName);
    }

    public void setIdealStates(Map<String, IdealState> idealStates) {
      _idealStates = idealStates;
    }

    public void setExternalViews(Map<String, ExternalView> externalViews) {
      _externalViews = externalViews;
    }
  }
}
