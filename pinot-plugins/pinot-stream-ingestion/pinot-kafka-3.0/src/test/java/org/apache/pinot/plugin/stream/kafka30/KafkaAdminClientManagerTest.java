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
package org.apache.pinot.plugin.stream.kafka30;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.plugin.stream.kafka.KafkaAdminClientManager;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class KafkaAdminClientManagerTest {

  @Test
  public void testGetInstance() {
    KafkaAdminClientManager instance1 = KafkaAdminClientManager.getInstance();
    KafkaAdminClientManager instance2 = KafkaAdminClientManager.getInstance();
    assertSame(instance1, instance2, "Should return the same singleton instance");
  }

  @Test
  public void testCreateCacheKey() {
    Properties props1 = new Properties();
    props1.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props1.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

    Properties props2 = new Properties();
    props2.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props2.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

    Properties props3 = new Properties();
    props3.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
    props3.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

    KafkaAdminClientManager manager = KafkaAdminClientManager.getInstance();

    // Use reflection to access private method for testing
    try {
      java.lang.reflect.Method createCacheKeyMethod =
          KafkaAdminClientManager.class.getDeclaredMethod("createCacheKey", Properties.class);
      createCacheKeyMethod.setAccessible(true);

      String key1 = (String) createCacheKeyMethod.invoke(manager, props1);
      String key2 = (String) createCacheKeyMethod.invoke(manager, props2);
      String key3 = (String) createCacheKeyMethod.invoke(manager, props3);

      assertEquals(key1, key2, "Same properties should produce same cache key");
      assertNotEquals(key1, key3, "Different bootstrap servers should produce different cache keys");
    } catch (Exception e) {
      fail("Failed to test createCacheKey method", e);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetOrCreateAdminClientWithoutBootstrapServers() {
    Properties props = new Properties();
    // No bootstrap servers set
    KafkaAdminClientManager.getInstance().getOrCreateAdminClient(props);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetOrCreateAdminClientWithEmptyBootstrapServers() {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "");
    KafkaAdminClientManager.getInstance().getOrCreateAdminClient(props);
  }

  @Test
  public void testAdminClientReferenceClosing() {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    KafkaAdminClientManager manager = KafkaAdminClientManager.getInstance();

    // This test focuses on the reference mechanism without actually connecting to Kafka
    // We test that multiple calls with same properties don't throw exceptions
    try {
      KafkaAdminClientManager.AdminClientReference ref1 = manager.getOrCreateAdminClient(props);
      assertNotNull(ref1, "Reference should not be null");

      // Closing a reference should not throw an exception
      ref1.close();

      // Closing again should not throw an exception
      ref1.close();
    } catch (Exception e) {
      // This is expected since we're not connecting to a real Kafka cluster
      // The test is mainly to verify the reference counting mechanism doesn't crash
      assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
          || e.getMessage().contains("timeout") || e.getMessage().contains("refused"));
    }
  }

  @Test
  public void testAdminClientReferenceStateAfterClose() {
    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    KafkaAdminClientManager manager = KafkaAdminClientManager.getInstance();

    try {
      KafkaAdminClientManager.AdminClientReference ref = manager.getOrCreateAdminClient(props);
      ref.close();

      // Should throw IllegalStateException when trying to use closed reference
      assertThrows(IllegalStateException.class, () -> ref.getAdminClient());
    } catch (Exception e) {
      // Expected when no real Kafka cluster is available
      assertTrue(e.getMessage().contains("Connection") || e.getMessage().contains("Kafka")
          || e.getMessage().contains("timeout") || e.getMessage().contains("refused"));
    }
  }
}
