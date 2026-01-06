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
package org.apache.pinot.controller.helix.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for minion drain functionality in PinotHelixResourceManager
 */
public class PinotHelixResourceManagerMinionDrainTest extends ControllerTest {
  private List<HelixManager> _fakeMinionHelixManagers = new java.util.ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
  }

  /**
   * Creates a fake minion HelixManager that connects to the cluster, creating a LiveInstance entry.
   * This is needed for enableInstance to work in tests.
   */
  private void createFakeMinionLiveInstance(String instanceId)
      throws Exception {
    HelixManager helixManager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(),
            instanceId, org.apache.helix.InstanceType.PARTICIPANT, getZkUrl());
    helixManager.connect();
    _fakeMinionHelixManagers.add(helixManager);
  }

  @Test
  public void testDrainMinionInstanceBasic() {
    // Create a minion instance with minion_untagged tag
    String minionHost = "minion-test-1.example.com";
    int minionPort = 9514;
    Instance minionInstance =
        new Instance(minionHost, minionPort, InstanceType.MINION, Collections.singletonList(
            Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful(), "Failed to add minion instance");

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Verify initial state has minion_untagged tag
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    assertNotNull(instanceConfig);
    List<String> initialTags = instanceConfig.getTags();
    assertEquals(initialTags.size(), 1);
    assertTrue(initialTags.contains(Helix.UNTAGGED_MINION_INSTANCE));

    // Drain the minion
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful(), "Failed to drain minion: " + drainResponse.getMessage());

    // Verify tags were updated
    instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 1);
    assertFalse(drainedTags.contains(Helix.UNTAGGED_MINION_INSTANCE));
    assertTrue(drainedTags.contains(Helix.DRAINED_MINION_INSTANCE));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainMinionInstanceWithCustomTags() {
    // Create a minion with custom tags - should succeed and replace ALL tags
    String minionHost = "minion-test-2.example.com";
    int minionPort = 9515;
    List<String> tags = Arrays.asList(Helix.UNTAGGED_MINION_INSTANCE, "custom_tag_1", "custom_tag_2");
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
        tags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Drain the minion - should succeed
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify ALL tags were replaced with just minion_drained
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 1);
    assertEquals(drainedTags.get(0), Helix.DRAINED_MINION_INSTANCE);

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainMinionInstanceWithOnlyCustomTag() {
    // Create a minion with only custom tag - should succeed and replace it
    String minionHost = "minion-test-3.example.com";
    int minionPort = 9516;
    List<String> tags = Arrays.asList("custom_tag");
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
        tags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Drain the minion - should succeed
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify tag was replaced with minion_drained
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 1);
    assertEquals(drainedTags.get(0), Helix.DRAINED_MINION_INSTANCE);

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainMinionInstanceAlreadyDrainedFails() {
    // Create a minion that's already drained - should throw exception
    String minionHost = "minion-test-4.example.com";
    int minionPort = 9517;
    List<String> tags = Arrays.asList(Helix.DRAINED_MINION_INSTANCE);
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
        tags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    try {
      // Attempt to drain the already drained minion - should throw UnsupportedOperationException
      PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
      assertFalse(drainResponse.isSuccessful(), "Draining an already drained minion should have failed");
    } finally {
      // Cleanup
      _helixResourceManager.dropInstance(minionInstanceId);
    }
  }

  @Test
  public void testDrainMinionWithEmptyTags() {
    // Create a minion with no tags
    String minionHost = "minion-test-5.example.com";
    int minionPort = 9518;
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION, null, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Drain the minion
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify minion_drained tag was added
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 1);
    assertTrue(drainedTags.contains(Helix.DRAINED_MINION_INSTANCE));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainNonExistentMinionFails() {
    // Try to drain a non-existent minion
    String nonExistentMinionId = "Minion_nonexistent.example.com_9999";
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(nonExistentMinionId);
    assertFalse(drainResponse.isSuccessful());
    assertTrue(drainResponse.getMessage().contains("not found"));
  }

  @Test
  public void testDrainMinionPreservesOtherInstanceConfigFields() {
    // Create a minion with various config fields set
    String minionHost = "minion-test-6.example.com";
    int minionPort = 9519;
    int grpcPort = 8090;
    int adminPort = 8091;
    Instance minionInstance =
        new Instance(minionHost, minionPort, InstanceType.MINION, Collections.singletonList(
            Helix.UNTAGGED_MINION_INSTANCE), null, grpcPort, adminPort, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Verify initial config
    InstanceConfig initialConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    assertEquals(initialConfig.getHostName(), minionHost);
    assertEquals(Integer.parseInt(initialConfig.getPort()), minionPort);
    assertEquals(initialConfig.getRecord().getSimpleField(Helix.Instance.GRPC_PORT_KEY), String.valueOf(grpcPort));
    assertEquals(initialConfig.getRecord().getSimpleField(Helix.Instance.ADMIN_PORT_KEY), String.valueOf(adminPort));

    // Drain the minion
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify other config fields remain unchanged
    InstanceConfig drainedConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    assertEquals(drainedConfig.getHostName(), minionHost);
    assertEquals(Integer.parseInt(drainedConfig.getPort()), minionPort);
    assertEquals(drainedConfig.getRecord().getSimpleField(Helix.Instance.GRPC_PORT_KEY), String.valueOf(grpcPort));
    assertEquals(drainedConfig.getRecord().getSimpleField(Helix.Instance.ADMIN_PORT_KEY), String.valueOf(adminPort));

    // Verify only tags changed
    List<String> drainedTags = drainedConfig.getTags();
    assertEquals(drainedTags.size(), 1);
    assertTrue(drainedTags.contains(Helix.DRAINED_MINION_INSTANCE));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testMultipleDrainOperationsOnSameMinion() {
    // Create a minion
    String minionHost = "minion-test-7.example.com";
    int minionPort = 9520;
    Instance minionInstance =
        new Instance(minionHost, minionPort, InstanceType.MINION, Collections.singletonList(
            Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // First drain should succeed
    PinotResourceManagerResponse firstDrainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(firstDrainResponse.isSuccessful(), "First drain operation failed");

    // Verify state after first drain
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> tags = instanceConfig.getTags();
    assertEquals(tags.size(), 1);
    assertTrue(tags.contains(Helix.DRAINED_MINION_INSTANCE));
    assertFalse(tags.contains(Helix.UNTAGGED_MINION_INSTANCE));

    // Subsequent drain attempts should throw UnsupportedOperationException (already drained)
    for (int i = 1; i < 3; i++) {
        PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
        assertFalse(drainResponse.isSuccessful(), "Subsequent drain operation " + (i + 1) + " should have failed");
    }

    // Verify final state remains unchanged
    instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    tags = instanceConfig.getTags();
    assertEquals(tags.size(), 1);
    assertTrue(tags.contains(Helix.DRAINED_MINION_INSTANCE));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainMinionInstanceConcurrentSafety()
      throws InterruptedException {
    // Create a minion with only standard tag
    String minionHost = "minion-test-8.example.com";
    int minionPort = 9521;
    List<String> tags = Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE);
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION, tags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Drain from multiple threads concurrently
    Thread[] threads = new Thread[5];
    final boolean[] results = new boolean[5];

    for (int i = 0; i < 5; i++) {
      final int index = i;
      threads[i] = new Thread(() -> {
        try {
          PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
          results[index] = drainResponse.isSuccessful();
        } catch (UnsupportedOperationException e) {
          // Expected after first drain succeeds (minion will have minion_drained tag)
          results[index] = false;
        }
      });
      threads[i].start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // At least one operation should succeed, others may throw exception after first succeeds
    // (because minion will have minion_drained tag which is considered custom)
    int successCount = 0;
    for (int i = 0; i < 5; i++) {
      if (results[i]) {
        successCount++;
      }
    }
    assertTrue(successCount >= 1, "At least one concurrent drain operation should succeed");

    // Verify final state - should have minion_drained tag
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> finalTags = instanceConfig.getTags();
    assertEquals(finalTags.size(), 1);
    assertTrue(finalTags.contains(Helix.DRAINED_MINION_INSTANCE));
    assertFalse(finalTags.contains(Helix.UNTAGGED_MINION_INSTANCE));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainThenEnableRestoresOriginalTags() throws Exception {
    // Create a minion with minion_untagged tag
    String minionHost = "minion-test-9.example.com";
    int minionPort = 9522;
    List<String> originalTags = Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE);
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
        originalTags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Create a fake minion live instance (simulate minion joining) - needed for enableInstance to work
    createFakeMinionLiveInstance(minionInstanceId);

    // Drain the minion
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify it's drained
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 1);
    assertEquals(drainedTags.get(0), Helix.DRAINED_MINION_INSTANCE);

    // Verify pre-drain tags were stored
    List<String> storedPreDrainTags = instanceConfig.getRecord().getListField(Helix.PREVIOUS_TAGS);
    assertNotNull(storedPreDrainTags);
    assertEquals(storedPreDrainTags, originalTags);

    // Enable the minion - should restore original tags
    PinotResourceManagerResponse enableResponse = _helixResourceManager.enableInstance(minionInstanceId);
    assertTrue(enableResponse.isSuccessful());

    // Verify tags were restored
    instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> restoredTags = instanceConfig.getTags();
    assertEquals(restoredTags.size(), 1);
    assertEquals(restoredTags.get(0), Helix.UNTAGGED_MINION_INSTANCE);

    // Verify pre-drain tags were cleared
    List<String> clearedPreDrainTags = instanceConfig.getRecord().getListField(Helix.PREVIOUS_TAGS);
    assertTrue(clearedPreDrainTags == null || clearedPreDrainTags.isEmpty());

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainThenEnableRestoresCustomTags() throws Exception {
    // Create a minion with custom tags
    String minionHost = "minion-test-10.example.com";
    int minionPort = 9523;
    List<String> originalTags = Arrays.asList("minion_partition_A", "custom_tag");
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
        originalTags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Create a fake minion live instance (simulate minion joining) - needed for enableInstance to work
    createFakeMinionLiveInstance(minionInstanceId);

    // Drain the minion
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify it's drained
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    assertEquals(instanceConfig.getTags().size(), 1);
    assertEquals(instanceConfig.getTags().get(0), Helix.DRAINED_MINION_INSTANCE);

    // Enable the minion - should restore custom tags
    PinotResourceManagerResponse enableResponse = _helixResourceManager.enableInstance(minionInstanceId);
    assertTrue(enableResponse.isSuccessful());

    // Verify custom tags were restored
    instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> restoredTags = instanceConfig.getTags();
    assertEquals(restoredTags.size(), 2);
    assertTrue(restoredTags.contains("minion_partition_A"));
    assertTrue(restoredTags.contains("custom_tag"));
    assertFalse(restoredTags.contains(Helix.DRAINED_MINION_INSTANCE));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testEnableNonDrainedMinionDoesNotModifyTags() throws Exception {
    // Create a normal minion
    String minionHost = "minion-test-11.example.com";
    int minionPort = 9524;
    List<String> originalTags = Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE);
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
        originalTags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Create a fake minion live instance (simulate minion joining) - needed for enableInstance to work
    createFakeMinionLiveInstance(minionInstanceId);

    // Enable the minion (not drained) - tags should remain unchanged
    PinotResourceManagerResponse enableResponse = _helixResourceManager.enableInstance(minionInstanceId);
    assertTrue(enableResponse.isSuccessful());

    // Verify tags were not modified
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> tags = instanceConfig.getTags();
    assertEquals(tags.size(), 1);
    assertEquals(tags.get(0), Helix.UNTAGGED_MINION_INSTANCE);

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @AfterClass
  public void tearDown() {
    // Disconnect fake minion managers
    for (HelixManager manager : _fakeMinionHelixManagers) {
      if (manager != null && manager.isConnected()) {
        manager.disconnect();
      }
    }
    _fakeMinionHelixManagers.clear();
    stopController();
    stopZk();
  }
}
