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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class InstanceCapabilityTrackerTest {
  private static final String CLUSTER = "testCluster";
  private static final String SERVER_1 = "Server_host1_8098";
  private static final String SERVER_2 = "Server_host2_8098";
  private static final String BROKER = "Broker_host3_8099";
  private static final String VERSION = "1.6.0";
  private static final String OLD_VERSION = "1.5.0";
  private static final String CAPABILITY_NAME = "TEST_CAPABILITY";
  private static final InstanceCapability CAPABILITY =
      new InstanceCapability(CAPABILITY_NAME, InstanceType.SERVER, 1);

  private final Map<String, InstanceConfig> _instanceConfigs = new HashMap<>();
  private InstanceCapabilityTracker _tracker;

  @BeforeMethod
  public void setUp() {
    _instanceConfigs.clear();
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getInstancesInCluster(CLUSTER)).thenAnswer(
        invocation -> new ArrayList<>(_instanceConfigs.keySet()));
    when(helixAdmin.getInstanceConfig(eq(CLUSTER), anyString())).thenAnswer(invocation -> {
      InstanceConfig instanceConfig = _instanceConfigs.get(invocation.<String>getArgument(1));
      if (instanceConfig == null) {
        throw new IllegalStateException("No instance config");
      }
      return instanceConfig;
    });
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getClusterName()).thenReturn(CLUSTER);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    _tracker = new InstanceCapabilityTracker(helixManager, InstanceType.SERVER, List.of(CAPABILITY));
  }

  @Test
  public void testRevisionZeroBeforeInitAndWithoutServers() {
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
    putInstance(BROKER, Map.of(CAPABILITY_NAME, "1"));
    init();
    // Brokers are ignored, and a cluster without servers supports nothing.
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testAllServersAdvertiseTheCapability() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    // A broker without the capability does not count.
    putInstance(BROKER, null);
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
  }

  @Test
  public void testMissingMapOrKeyIsRevisionZero() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, null);
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    putInstance(SERVER_2, Map.of("SOME_FUTURE_CAPABILITY", "3"));
    instanceChanged(SERVER_2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testMixedRevisionsMinWins() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "3"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "2"));
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 2);
  }

  @Test
  public void testMalformedRevisionIsZero() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "one"));
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "-1"));
    instanceChanged(SERVER_2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testConfigChangeOfOneServer() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, null);
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The old server is upgraded and writes the capability.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    instanceChanged(SERVER_2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // The capability disappears from the instance config of a server.
    putInstance(SERVER_1, null);
    instanceChanged(SERVER_1);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testServerAddedAndRemoved() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // A new server without the capability joins.
    putInstance(SERVER_2, null);
    childChanged();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The server is removed.
    _instanceConfigs.remove(SERVER_2);
    childChanged();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
  }

  @Test
  public void testMatchingVersionStampGivesTheAdvertisedRevision() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "2"), VERSION, VERSION);
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 2);
  }

  @Test
  public void testRollbackIgnoresStaleCapabilities() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // SERVER_2 is rolled back to a version that predates the capabilities. That version rewrites the simple field
    // pinotVersion at startup but keeps the capabilities map, so the stamp no longer matches.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"), OLD_VERSION, VERSION);
    instanceChanged(SERVER_2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
    assertEquals(_tracker.getStaleInstances(), List.of(SERVER_2));

    // Not an ordering of versions: a newer version in the simple field does not match either.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"), "9.9.9", VERSION);
    instanceChanged(SERVER_2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The server is upgraded again and rewrites the capabilities with its version.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    instanceChanged(SERVER_2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
    assertEquals(_tracker.getStaleInstances(), List.of());
  }

  @Test
  public void testMissingVersionStampIsRevisionZero() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"), VERSION, null);
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The stamp without the simple field does not match either.
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"), null, VERSION);
    instanceChanged(SERVER_1);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testFailedReadOnCallbackKeepsTheInstanceAsUnsupported() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    init();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // The read of SERVER_2 fails. It must not raise the minimum: SERVER_2 counts as revision 0.
    _instanceConfigs.remove(SERVER_2);
    instanceChanged(SERVER_2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The child change notification removes the instance that is gone.
    childChanged();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
  }

  @Test
  public void testIgnoredNotificationType() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    init();
    putInstance(SERVER_1, null);
    NotificationContext context = new NotificationContext(null);
    context.setType(NotificationContext.Type.FINALIZE);
    _tracker.onInstanceConfigChange(List.of(), context);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
  }

  /// Puts an instance config whose capabilities were written by the Pinot version that the instance runs.
  private void putInstance(String instanceId, @Nullable Map<String, String> capabilities) {
    putInstance(instanceId, capabilities, VERSION, VERSION);
  }

  /// Puts an instance config with the given simple field `pinotVersion` and the given `pinotVersion` entry of the
  /// capabilities map. A null value leaves the field or the entry out.
  private void putInstance(String instanceId, @Nullable Map<String, String> capabilities,
      @Nullable String instanceVersion, @Nullable String capabilitiesVersion) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    if (instanceVersion != null) {
      instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, instanceVersion);
    }
    if (capabilities != null) {
      Map<String, String> mapField = new HashMap<>(capabilities);
      if (capabilitiesVersion != null) {
        mapField.put(CommonConstants.Helix.Instance.CAPABILITIES_PINOT_VERSION_KEY, capabilitiesVersion);
      }
      instanceConfig.getRecord().setMapField(CommonConstants.Helix.Instance.CAPABILITIES_KEY, mapField);
    }
    _instanceConfigs.put(instanceId, instanceConfig);
  }

  private void init() {
    NotificationContext context = new NotificationContext(null);
    context.setType(NotificationContext.Type.INIT);
    _tracker.onInstanceConfigChange(List.of(), context);
  }

  private void childChanged() {
    NotificationContext context = new NotificationContext(null);
    context.setType(NotificationContext.Type.CALLBACK);
    context.setIsChildChange(true);
    context.setPathChanged("/" + CLUSTER + "/CONFIGS/PARTICIPANT");
    _tracker.onInstanceConfigChange(List.of(), context);
  }

  private void instanceChanged(String instanceId) {
    NotificationContext context = new NotificationContext(null);
    context.setType(NotificationContext.Type.CALLBACK);
    context.setIsChildChange(false);
    context.setPathChanged("/" + CLUSTER + "/CONFIGS/PARTICIPANT/" + instanceId);
    _tracker.onInstanceConfigChange(List.of(), context);
  }
}
