/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.utils;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


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

  @Test
  public void testMultipleServiceStatusCallback() {
    // Only good should return good
    ServiceStatus.MultipleCallbackServiceStatusCallback onlyGood = new ServiceStatus.MultipleCallbackServiceStatusCallback(
        ImmutableList.of(ALWAYS_GOOD)
    );

    assertEquals(onlyGood.getServiceStatus(), ServiceStatus.Status.GOOD);

    // Only bad should return bad
    ServiceStatus.MultipleCallbackServiceStatusCallback onlyBad = new ServiceStatus.MultipleCallbackServiceStatusCallback(
        ImmutableList.of(ALWAYS_BAD)
    );

    assertEquals(onlyBad.getServiceStatus(), ServiceStatus.Status.BAD);

    // Only starting should return starting
    ServiceStatus.MultipleCallbackServiceStatusCallback onlyStarting = new ServiceStatus.MultipleCallbackServiceStatusCallback(
        ImmutableList.of(ALWAYS_STARTING)
    );

    assertEquals(onlyStarting.getServiceStatus(), ServiceStatus.Status.STARTING);

    // Good + starting = starting
    ServiceStatus.MultipleCallbackServiceStatusCallback goodAndStarting = new ServiceStatus.MultipleCallbackServiceStatusCallback(
        ImmutableList.of(ALWAYS_GOOD, ALWAYS_STARTING)
    );

    assertEquals(goodAndStarting.getServiceStatus(), ServiceStatus.Status.STARTING);

    // Good + starting + bad = starting (check for left-to-right evaluation)
    ServiceStatus.MultipleCallbackServiceStatusCallback goodStartingAndBad = new ServiceStatus.MultipleCallbackServiceStatusCallback(
        ImmutableList.of(ALWAYS_GOOD, ALWAYS_STARTING, ALWAYS_BAD)
    );

    assertEquals(goodStartingAndBad.getServiceStatus(), ServiceStatus.Status.STARTING);
  }

  @Test
  public void testIdealStateMatch() {
    TestIdealStateAndExternalViewMatchServiceStatusCallback callback;

    // No ideal state = STARTING
    callback = buildTestISEVCallback();
    callback.setExternalView(new ExternalView(TABLE_NAME));
    assertEquals(callback.getServiceStatus(), ServiceStatus.Status.STARTING);

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
    return new TestIdealStateAndExternalViewMatchServiceStatusCallback(null, "potato", INSTANCE_NAME,
        Collections.singletonList(TABLE_NAME));
  }

  private static class TestIdealStateAndExternalViewMatchServiceStatusCallback extends ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback {
    private IdealState _idealState;
    private ExternalView _externalView;

    public TestIdealStateAndExternalViewMatchServiceStatusCallback(HelixManager helixManager, String clusterName,
        String instanceName) {
      super(helixManager, clusterName, instanceName);
    }

    public TestIdealStateAndExternalViewMatchServiceStatusCallback(HelixManager helixManager, String clusterName,
        String instanceName, List<String> resourcesToMonitor) {
      super(helixManager, clusterName, instanceName, resourcesToMonitor);
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
}