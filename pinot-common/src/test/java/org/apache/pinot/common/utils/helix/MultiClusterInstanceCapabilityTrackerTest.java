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
package org.apache.pinot.common.utils.helix;

import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class MultiClusterInstanceCapabilityTrackerTest {
  private static final InstanceCapability CAPABILITY =
      new InstanceCapability("TEST_CAPABILITY", InstanceType.SERVER, 1);

  @Test
  public void testNoClusterIsRevisionZero() {
    MultiClusterInstanceCapabilityTracker tracker =
        new MultiClusterInstanceCapabilityTracker(InstanceType.SERVER, List.of(CAPABILITY));
    assertEquals(tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testMinimumAcrossClusters() {
    MultiClusterInstanceCapabilityTracker tracker =
        new MultiClusterInstanceCapabilityTracker(InstanceType.SERVER, List.of(CAPABILITY));
    InstanceCapabilityTracker local = clusterTracker("local", 2);
    InstanceCapabilityTracker remote = clusterTracker("remote", 3);
    tracker.addCluster(local);
    tracker.addCluster(remote);
    assertEquals(tracker.getMinRevision(CAPABILITY), 2);

    when(remote.getMinRevision(CAPABILITY)).thenReturn(1);
    assertEquals(tracker.getMinRevision(CAPABILITY), 1);

    when(local.getMinRevision(CAPABILITY)).thenReturn(0);
    assertEquals(tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testUnavailableClusterIsRevisionZero()
      throws Exception {
    MultiClusterInstanceCapabilityTracker tracker =
        new MultiClusterInstanceCapabilityTracker(InstanceType.SERVER, List.of(CAPABILITY));
    tracker.addCluster(clusterTracker("local", 1));
    assertEquals(tracker.getMinRevision(CAPABILITY), 1);
    tracker.addUnavailableCluster("remote");
    assertEquals(tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testFailedRegistrationIsRevisionZero()
      throws Exception {
    MultiClusterInstanceCapabilityTracker tracker =
        new MultiClusterInstanceCapabilityTracker(InstanceType.SERVER, List.of(CAPABILITY));
    tracker.addCluster(clusterTracker("local", 1));
    InstanceCapabilityTracker remote = clusterTracker("remote", 1);
    HelixManager remoteHelixManager = remote.getHelixManager();
    doThrow(new IllegalStateException("Not connected")).when(remoteHelixManager)
        .addInstanceConfigChangeListener(any(InstanceConfigChangeListener.class));
    tracker.addCluster(remote);
    assertEquals(tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testRegistersAndRemovesTheListeners()
      throws Exception {
    MultiClusterInstanceCapabilityTracker tracker =
        new MultiClusterInstanceCapabilityTracker(InstanceType.SERVER, List.of(CAPABILITY));
    InstanceCapabilityTracker local = clusterTracker("local", 1);
    InstanceCapabilityTracker remote = clusterTracker("remote", 1);
    tracker.addCluster(local);
    tracker.addCluster(remote);
    verify(local.getHelixManager()).addInstanceConfigChangeListener(local);
    verify(remote.getHelixManager()).addInstanceConfigChangeListener(remote);

    tracker.close();
    verify(local.getHelixManager()).removeListener(instanceConfigsKey("local"), eq(local));
    verify(remote.getHelixManager()).removeListener(instanceConfigsKey("remote"), eq(remote));
    // A closed tracker is fail-safe.
    assertEquals(tracker.getMinRevision(CAPABILITY), 0);
  }

  private static PropertyKey instanceConfigsKey(String clusterName) {
    String path = new PropertyKey.Builder(clusterName).instanceConfigs().getPath();
    return argThat(key -> key != null && key.getPath().equals(path));
  }

  /// A mocked tracker of one cluster with its own mocked Helix manager.
  private static InstanceCapabilityTracker clusterTracker(String clusterName, int minRevision) {
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getClusterName()).thenReturn(clusterName);
    InstanceCapabilityTracker tracker = mock(InstanceCapabilityTracker.class);
    when(tracker.getClusterName()).thenReturn(clusterName);
    when(tracker.getHelixManager()).thenReturn(helixManager);
    when(tracker.getMinRevision(CAPABILITY)).thenReturn(minRevision);
    return tracker;
  }
}
