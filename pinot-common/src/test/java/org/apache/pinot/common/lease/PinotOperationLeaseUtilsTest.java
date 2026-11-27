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
package org.apache.pinot.common.lease;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.ZkStarter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Runs operation leases against an embedded ZooKeeper, with one ZooKeeper session per simulated instance.
public class PinotOperationLeaseUtilsTest {
  private static final String PROPERTY_STORE_ROOT = "/PinotOperationLeaseUtilsTest/PROPERTYSTORE";
  private static final String TABLE = "myTable_OFFLINE";

  private ZkStarter.ZookeeperInstance _zk;
  private final List<ZkClient> _clients = new ArrayList<>();
  private Instance _instanceA;
  private Instance _instanceB;

  @BeforeClass
  public void setUp() {
    _zk = ZkStarter.startLocalZkServer();
  }

  @AfterClass
  public void tearDown() {
    ZkStarter.stopLocalZkServer(_zk);
  }

  @AfterMethod
  public void cleanUp() {
    for (ZkClient client : _clients) {
      if (!client.isClosed()) {
        client.close();
      }
    }
    _clients.clear();
    _instanceA = null;
    _instanceB = null;
  }

  private Instance instanceA() {
    if (_instanceA == null) {
      _instanceA = newInstance("instanceA");
    }
    return _instanceA;
  }

  private Instance instanceB() {
    if (_instanceB == null) {
      _instanceB = newInstance("instanceB");
    }
    return _instanceB;
  }

  @Test
  public void testOnlyOneInstanceHoldsTheLease() {
    String key = "exclusive";
    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA");
    assertNotNull(lease);
    assertTrue(lease.isHeld());
    assertEquals(lease.getKey(), key);
    assertEquals(lease.getPath(), PinotOperationLeaseUtils.getLeasePath(TABLE, key));
    assertNull(PinotOperationLeaseUtils.tryAcquire(instanceB()._manager, TABLE, key, "instanceB"));
    // The same instance cannot acquire it twice either.
    assertNull(PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA"));

    PinotOperationLeaseUtils.LeaseInfo info = PinotOperationLeaseUtils.getLeaseInfo(instanceB()._manager, TABLE, key);
    assertNotNull(info);
    assertEquals(info.getKey(), key);
    assertEquals(info.getOwner(), "instanceA");
    assertEquals(info.getSessionId(), lease.getSessionId());
    assertEquals(info.getAcquireTimeMs(), lease.getAcquireTimeMs());
  }

  @Test
  public void testReleaseLetsAnotherInstanceAcquire() {
    String key = "release";
    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA");
    assertNotNull(lease);
    assertTrue(lease.release());
    assertFalse(lease.isHeld());
    assertThrows(LeaseLostException.class, lease::checkOwnership);
    // Idempotent.
    assertTrue(lease.release());
    assertNull(PinotOperationLeaseUtils.getLeaseInfo(instanceA()._manager, TABLE, key));

    PinotOperationLease leaseB = PinotOperationLeaseUtils.tryAcquire(instanceB()._manager, TABLE, key, "instanceB");
    assertNotNull(leaseB);
    leaseB.close();
  }

  @Test
  public void testReleaseNeverDeletesAnotherSessionsLease() {
    String key = "ownerCheckedRelease";
    PinotOperationLease staleLease =
        PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA");
    assertNotNull(staleLease);
    // Instance A loses the lease (as on a session expiry), and instance B takes it over.
    assertTrue(PinotOperationLeaseUtils.forceRelease(instanceB()._manager, TABLE, key));
    PinotOperationLease leaseB = PinotOperationLeaseUtils.tryAcquire(instanceB()._manager, TABLE, key, "instanceB");
    assertNotNull(leaseB);

    // A's late release must leave B's lease alone.
    assertTrue(staleLease.release());
    PinotOperationLeaseUtils.LeaseInfo info = PinotOperationLeaseUtils.getLeaseInfo(instanceA()._manager, TABLE, key);
    assertNotNull(info);
    assertEquals(info.getOwner(), "instanceB");
    assertTrue(leaseB.isHeld());
    leaseB.close();
  }

  @Test
  public void testSessionEndReleasesTheLease()
      throws Exception {
    String key = "sessionEnd";
    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA");
    assertNotNull(lease);

    CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(() -> awaitRelease(instanceB(), key, 30_000));
    instanceA()._client.close();
    assertTrue(waiter.get(30, TimeUnit.SECONDS));
    assertFalse(lease.isHeld());
    assertThrows(LeaseLostException.class, lease::checkOwnership);
    PinotOperationLease leaseB = PinotOperationLeaseUtils.tryAcquire(instanceB()._manager, TABLE, key, "instanceB");
    assertNotNull(leaseB);
    leaseB.close();
  }

  @Test
  public void testAwaitReleaseWakesUpOnRelease()
      throws Exception {
    String key = "awaitRelease";
    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA");
    assertNotNull(lease);
    // Times out while the lease is held.
    assertFalse(PinotOperationLeaseUtils.awaitRelease(instanceB()._manager, TABLE, key, Duration.ofMillis(200)));

    AtomicReference<Long> wokeUpAtMs = new AtomicReference<>();
    CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(() -> {
      boolean released = awaitRelease(instanceB(), key, 30_000);
      wokeUpAtMs.set(System.currentTimeMillis());
      return released;
    });
    Thread.sleep(200);
    assertFalse(waiter.isDone());
    long releasedAtMs = System.currentTimeMillis();
    lease.release();
    assertTrue(waiter.get(30, TimeUnit.SECONDS));
    // Woken by the watch, well before the 30s timeout.
    assertTrue(wokeUpAtMs.get() - releasedAtMs < 10_000);
  }

  @Test
  public void testAwaitReleaseReturnsAtOnceWhenFree()
      throws Exception {
    assertTrue(PinotOperationLeaseUtils.awaitRelease(instanceA()._manager, TABLE, "free", Duration.ofSeconds(30)));
  }

  @Test
  public void testLeaseEndsWhenSessionChanges() {
    Instance instance = instanceA();
    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instance._manager, TABLE, "sessionChange",
        "instanceA");
    assertNotNull(lease);
    when(instance._manager.getSessionId()).thenReturn(Long.toHexString(lease.getSessionId() + 1));
    assertFalse(lease.isHeld());
    assertThrows(LeaseLostException.class, lease::checkOwnership);
  }

  @Test
  public void testTryAcquireFailsWhenDisconnected() {
    Instance instance = instanceA();
    when(instance._manager.isConnected()).thenReturn(false);
    assertThrows(IllegalStateException.class,
        () -> PinotOperationLeaseUtils.tryAcquire(instance._manager, TABLE, "disconnected", "instanceA"));
  }

  @Test
  public void testForceReleaseReportsMissingLease() {
    assertFalse(PinotOperationLeaseUtils.forceRelease(instanceA()._manager, TABLE, "missing"));
  }

  @Test
  public void testTableDeletionRemovesHeldLeases() {
    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, "tableDeletion",
        "instanceA");
    assertNotNull(lease);
    ZkHelixPropertyStore<ZNRecord> propertyStore = instanceB()._manager.getHelixPropertyStore();
    ZKMetadataProvider.removeOperationLeasesFromPropertyStore(propertyStore, TABLE);
    assertFalse(propertyStore.exists(ZKMetadataProvider.constructPropertyStorePathForOperationLeases(TABLE),
        AccessOption.PERSISTENT));
    // The holder's late release is a no-op.
    assertTrue(lease.release());
  }

  @Test
  public void testLeaseAtCustomPath() {
    // A caller that keeps an existing ZNode passes its path instead of a key.
    String path = "/CUSTOM_LOCKS/" + TABLE + "-Lock";
    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, path, "instanceA");
    assertNotNull(lease);
    assertNull(lease.getKey());
    assertNull(PinotOperationLeaseUtils.tryAcquire(instanceB()._manager, path, "instanceB"));
    PinotOperationLeaseUtils.LeaseInfo info = PinotOperationLeaseUtils.getLeaseInfo(instanceB()._manager, path);
    assertNotNull(info);
    assertNull(info.getKey());
    assertEquals(info.getOwner(), "instanceA");
    lease.close();
    assertNull(PinotOperationLeaseUtils.getLeaseInfo(instanceB()._manager, path));
  }

  @Test
  public void testKeysThatAreNotValidZNodeNamesAreRejected() {
    for (String key : new String[]{
        "", ".", "..", "a/b", "a\u0000b", "a\u001fb", "a\u007fb", "a😀b", "a￾b",
        PinotOperationLeaseUtils.HASHED_KEY_PREFIX + "abc"
    }) {
      assertThrows(IllegalArgumentException.class, () -> PinotOperationLeaseUtils.getLeasePath(TABLE, key));
      assertThrows(IllegalArgumentException.class,
          () -> PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA"));
    }
    // Dots, dashes and non-ASCII letters are fine.
    for (String key : new String[]{"...", "a.b", "seg__1__tier-prepare__s3__123_456", "ségment"}) {
      assertEquals(PinotOperationLeaseUtils.getLeasePath(TABLE, key),
          ZKMetadataProvider.constructPropertyStorePathForOperationLease(TABLE, key));
    }
  }

  @Test
  public void testLongKeyIsHashed() {
    String key = "segment_".repeat(200);
    assertTrue(key.length() > PinotOperationLeaseUtils.MAX_KEY_LENGTH_BYTES);
    String path = PinotOperationLeaseUtils.getLeasePath(TABLE, key);
    String zNodeName = path.substring(path.lastIndexOf('/') + 1);
    assertTrue(zNodeName.startsWith(PinotOperationLeaseUtils.HASHED_KEY_PREFIX));
    assertEquals(zNodeName.length(), PinotOperationLeaseUtils.HASHED_KEY_PREFIX.length() + 64);

    PinotOperationLease lease = PinotOperationLeaseUtils.tryAcquire(instanceA()._manager, TABLE, key, "instanceA");
    assertNotNull(lease);
    // The same key maps to the same ZNode on every instance, and the full key stays readable.
    assertNull(PinotOperationLeaseUtils.tryAcquire(instanceB()._manager, TABLE, key, "instanceB"));
    PinotOperationLeaseUtils.LeaseInfo info = PinotOperationLeaseUtils.getLeaseInfo(instanceB()._manager, TABLE, key);
    assertNotNull(info);
    assertEquals(info.getKey(), key);
    // A key at the limit is kept as is.
    String keyAtLimit = "k".repeat(PinotOperationLeaseUtils.MAX_KEY_LENGTH_BYTES);
    assertEquals(PinotOperationLeaseUtils.getLeasePath(TABLE, keyAtLimit),
        ZKMetadataProvider.constructPropertyStorePathForOperationLease(TABLE, keyAtLimit));
    lease.close();
  }

  private static boolean awaitRelease(Instance instance, String key, long maxWaitMs) {
    try {
      return PinotOperationLeaseUtils.awaitRelease(instance._manager, TABLE, key, Duration.ofMillis(maxWaitMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /// One simulated instance: its own ZooKeeper session, property store and Helix manager.
  private Instance newInstance(String instanceId) {
    ZkClient client = new ZkClient.Builder().setZkServer(_zk.getZkUrl()).setZkSerializer(new ZNRecordSerializer())
        .build();
    assertTrue(client.waitUntilConnected(10_000, TimeUnit.MILLISECONDS));
    _clients.add(client);
    ZkHelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(client), PROPERTY_STORE_ROOT, null);
    HelixManager manager = mock(HelixManager.class);
    when(manager.getInstanceName()).thenReturn(instanceId);
    when(manager.getHelixPropertyStore()).thenReturn(propertyStore);
    when(manager.isConnected()).thenAnswer(invocation -> !client.isClosed());
    when(manager.getSessionId()).thenAnswer(invocation -> Long.toHexString(client.getSessionId()));
    return new Instance(client, manager);
  }

  private static final class Instance {
    final ZkClient _client;
    final HelixManager _manager;

    Instance(ZkClient client, HelixManager manager) {
      _client = client;
      _manager = manager;
    }
  }
}
