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

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
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
  public void testDrainMinionInstanceWithMultipleTags() {
    // Create a minion with multiple tags
    String minionHost = "minion-test-2.example.com";
    int minionPort = 9515;
    List<String> tags = Arrays.asList(Helix.UNTAGGED_MINION_INSTANCE, "custom_tag_1", "custom_tag_2");
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION, tags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Drain the minion
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify tags: minion_untagged removed, minion_drained added, custom tags remain
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 3);
    assertFalse(drainedTags.contains(Helix.UNTAGGED_MINION_INSTANCE));
    assertTrue(drainedTags.contains(Helix.DRAINED_MINION_INSTANCE));
    assertTrue(drainedTags.contains("custom_tag_1"));
    assertTrue(drainedTags.contains("custom_tag_2"));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainMinionInstanceWithoutUntaggedTag() {
    // Create a minion without minion_untagged tag
    String minionHost = "minion-test-3.example.com";
    int minionPort = 9516;
    List<String> tags = Arrays.asList("custom_tag");
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION, tags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Drain the minion
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify minion_drained was added, custom tag remains
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 2);
    assertTrue(drainedTags.contains(Helix.DRAINED_MINION_INSTANCE));
    assertTrue(drainedTags.contains("custom_tag"));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainMinionInstanceAlreadyDrained() {
    // Create a minion that's already drained
    String minionHost = "minion-test-4.example.com";
    int minionPort = 9517;
    List<String> tags = Arrays.asList(Helix.DRAINED_MINION_INSTANCE, "custom_tag");
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION, tags, null, 0, 0, 0, 0, false);

    PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
    assertTrue(addResponse.isSuccessful());

    String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;

    // Drain the already drained minion (should be idempotent)
    PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
    assertTrue(drainResponse.isSuccessful());

    // Verify no duplicate minion_drained tags
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> drainedTags = instanceConfig.getTags();
    assertEquals(drainedTags.size(), 2);
    assertTrue(drainedTags.contains(Helix.DRAINED_MINION_INSTANCE));
    assertTrue(drainedTags.contains("custom_tag"));
    // Verify no duplicates
    long drainedTagCount = drainedTags.stream().filter(t -> t.equals(Helix.DRAINED_MINION_INSTANCE)).count();
    assertEquals(drainedTagCount, 1);

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
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

    // Drain the minion multiple times (should be idempotent)
    for (int i = 0; i < 3; i++) {
      PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
      assertTrue(drainResponse.isSuccessful(), "Drain operation " + i + " failed");

      // Verify state after each drain
      InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
      List<String> tags = instanceConfig.getTags();
      assertEquals(tags.size(), 1, "Unexpected tag count after drain operation " + i);
      assertTrue(tags.contains(Helix.DRAINED_MINION_INSTANCE));
      assertFalse(tags.contains(Helix.UNTAGGED_MINION_INSTANCE));
    }

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @Test
  public void testDrainMinionInstanceConcurrentSafety()
      throws InterruptedException {
    // Create a minion
    String minionHost = "minion-test-8.example.com";
    int minionPort = 9521;
    List<String> tags = Arrays.asList(Helix.UNTAGGED_MINION_INSTANCE, "tag1", "tag2");
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
        PinotResourceManagerResponse drainResponse = _helixResourceManager.drainMinionInstance(minionInstanceId);
        results[index] = drainResponse.isSuccessful();
      });
      threads[i].start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // All drain operations should succeed (idempotent)
    for (int i = 0; i < 5; i++) {
      assertTrue(results[i], "Concurrent drain operation " + i + " failed");
    }

    // Verify final state
    InstanceConfig instanceConfig = _helixResourceManager.getHelixInstanceConfig(minionInstanceId);
    List<String> finalTags = instanceConfig.getTags();
    assertEquals(finalTags.size(), 3);
    assertTrue(finalTags.contains(Helix.DRAINED_MINION_INSTANCE));
    assertFalse(finalTags.contains(Helix.UNTAGGED_MINION_INSTANCE));
    assertTrue(finalTags.contains("tag1"));
    assertTrue(finalTags.contains("tag2"));

    // Cleanup
    _helixResourceManager.dropInstance(minionInstanceId);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
