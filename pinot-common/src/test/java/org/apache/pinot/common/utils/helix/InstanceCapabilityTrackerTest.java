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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class InstanceCapabilityTrackerTest {
  private static final String CLUSTER = "testCluster";
  private static final String SERVER_1 = "Server_host1_8098";
  private static final String SERVER_2 = "Server_host2_8098";
  private static final String BROKER_1 = "Broker_host3_8099";
  private static final String BROKER_2 = "Broker_host4_8099";
  private static final String VERSION = "1.6.0";
  private static final String OLD_VERSION = "1.5.0";
  private static final String CAPABILITY_NAME = "TEST_CAPABILITY";
  private static final InstanceCapability CAPABILITY =
      new InstanceCapability(CAPABILITY_NAME, InstanceType.SERVER, 1);

  private final Map<String, ZNRecord> _records = new HashMap<>();
  private InstanceCapabilityTracker _tracker;

  @BeforeMethod
  public void setUp() {
    _records.clear();
    _tracker = new InstanceCapabilityTracker(CLUSTER, InstanceType.SERVER, List.of(CAPABILITY));
  }

  @Test
  public void testRevisionZeroBeforeUpdateAndWithoutServers() {
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
    putInstance(BROKER_1, Map.of(CAPABILITY_NAME, "1"));
    update();
    // Brokers are ignored, and a set without servers supports nothing.
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testAllServersAdvertiseTheCapability() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    // A broker without the capability does not count.
    putInstance(BROKER_1, null);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
  }

  @Test
  public void testMissingMapOrKeyIsRevisionZero() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, null);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    putInstance(SERVER_2, Map.of("SOME_FUTURE_CAPABILITY", "3"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testMixedRevisionsMinWins() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "3"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "2"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 2);
  }

  @Test
  public void testMalformedRevisionIsZero() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "one"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "-1"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, " 1 "));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
  }

  @Test
  public void testConfigChangeOfOneServer() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, null);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The old server is upgraded and writes the capability.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // The capability disappears from the instance config of a server.
    putInstance(SERVER_1, null);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testServerAddedAndRemoved() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // A new server without the capability joins.
    putInstance(SERVER_2, null);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The server is removed.
    _records.remove(SERVER_2);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // The last server is removed.
    _records.remove(SERVER_1);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testExtensionCapabilityIsTracked() {
    // The tracker reads every capability that the instances advertise, also one it was not given.
    InstanceCapability extensionCapability = new InstanceCapability("VENDOR_CAPABILITY", InstanceType.SERVER, 1);
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1", "VENDOR_CAPABILITY", "2"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1", "VENDOR_CAPABILITY", "3"));
    update();
    assertEquals(_tracker.getMinRevision(extensionCapability), 2);
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
  }

  @Test
  public void testBrokerTracker() {
    InstanceCapability brokerCapability = new InstanceCapability("TEST_BROKER_CAPABILITY", InstanceType.BROKER, 1);
    InstanceCapabilityTracker tracker =
        new InstanceCapabilityTracker(CLUSTER, InstanceType.BROKER, List.of(brokerCapability));
    putInstance(BROKER_1, Map.of("TEST_BROKER_CAPABILITY", "1"));
    putInstance(BROKER_2, Map.of("TEST_BROKER_CAPABILITY", "1"));
    // A server without the capability does not count.
    putInstance(SERVER_1, null);
    tracker.update(_records.values());
    assertEquals(tracker.getMinRevision(brokerCapability), 1);

    // The owner decides which instances count: a set without BROKER_2 still gives the minimum over BROKER_1.
    putInstance(BROKER_2, null);
    tracker.update(List.of(_records.get(BROKER_1)));
    assertEquals(tracker.getMinRevision(brokerCapability), 1);
    tracker.update(_records.values());
    assertEquals(tracker.getMinRevision(brokerCapability), 0);
  }

  @Test
  public void testMatchingVersionStampGivesTheAdvertisedRevision() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "2"), VERSION, VERSION);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 2);
  }

  @Test
  public void testRollbackIgnoresStaleCapabilities() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"));
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);

    // SERVER_2 is rolled back to a version that predates the capabilities. That version rewrites the simple field
    // pinotVersion at startup but keeps the capabilities map, so the stamp no longer matches.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"), OLD_VERSION, VERSION);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
    assertEquals(_tracker.getStaleInstances(), List.of(SERVER_2));

    // Not an ordering of versions: a newer version in the simple field does not match either.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"), "9.9.9", VERSION);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The server is upgraded again and rewrites the capabilities with its version.
    putInstance(SERVER_2, Map.of(CAPABILITY_NAME, "1"));
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 1);
    assertEquals(_tracker.getStaleInstances(), List.of());
  }

  @Test
  public void testMissingVersionStampIsRevisionZero() {
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"), VERSION, null);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);

    // The stamp without the simple field does not match either.
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "1"), null, VERSION);
    update();
    assertEquals(_tracker.getMinRevision(CAPABILITY), 0);
  }

  @Test
  public void testReadRevisions() {
    assertEquals(InstanceCapabilityTracker.readRevisions(null), Map.of());
    putInstance(SERVER_1, Map.of(CAPABILITY_NAME, "2", "OTHER", "x"));
    // The version stamp is not a capability, and a malformed value is stored below 0.
    assertEquals(InstanceCapabilityTracker.readRevisions(_records.get(SERVER_1)), Map.of(CAPABILITY_NAME, 2, "OTHER",
        -1));
  }

  private void update() {
    _tracker.update(_records.values());
  }

  /// Puts an instance config whose capabilities were written by the Pinot version that the instance runs.
  private void putInstance(String instanceId, @Nullable Map<String, String> capabilities) {
    putInstance(instanceId, capabilities, VERSION, VERSION);
  }

  /// Puts an instance config with the given simple field `pinotVersion` and the given `pinotVersion` entry of the
  /// capabilities map. A null value leaves the field or the entry out.
  private void putInstance(String instanceId, @Nullable Map<String, String> capabilities,
      @Nullable String instanceVersion, @Nullable String capabilitiesVersion) {
    ZNRecord record = new ZNRecord(instanceId);
    if (instanceVersion != null) {
      record.setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, instanceVersion);
    }
    if (capabilities != null) {
      Map<String, String> mapField = new HashMap<>(capabilities);
      if (capabilitiesVersion != null) {
        mapField.put(CommonConstants.Helix.Instance.CAPABILITIES_PINOT_VERSION_KEY, capabilitiesVersion);
      }
      record.setMapField(CommonConstants.Helix.Instance.CAPABILITIES_KEY, mapField);
    }
    _records.put(instanceId, record);
  }
}
