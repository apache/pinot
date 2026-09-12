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
package org.apache.pinot.common.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Unit tests for the table deletion marker functionality in [ZKMetadataProvider].
/// The marker provides mutual exclusion between concurrent table deletions and blocks table
/// re-creation while a deletion is in flight, so these tests cover creation, duplicate
/// rejection, removal, expiry and takeover.
public class TableDeletionMarkerTest {
  private static final String PROPERTY_STORE_ROOT = "/TableDeletionMarkerTest/PROPERTYSTORE";
  private static final String MARKER_PREFIX = "/TABLE_DELETION_IN_PROGRESS";
  private static final String START_TIME_KEY = "startTimeMs";
  private static final String CONTROLLER_ID_KEY = "controllerId";
  private static final long EXPIRY_MS = 24 * 60 * 60 * 1000L;

  private static final String TABLE_NAME = "testTable_REALTIME";
  private static final String CONTROLLER_1 = "Controller_localhost_9000";
  private static final String CONTROLLER_2 = "Controller_localhost_9001";

  private ZkStarter.ZookeeperInstance _zk;
  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  /// A second, independent property store over the same ZK, modelling a different controller process. A single
  /// ZkHelixPropertyStore serializes its own operations, so two controllers cannot be simulated through one
  /// instance.
  private ZkClient _otherZkClient;
  private ZkHelixPropertyStore<ZNRecord> _otherPropertyStore;

  @BeforeClass
  public void beforeClass() {
    _zk = ZkStarter.startLocalZkServer();
    _zkClient = newZkClient();
    _propertyStore = new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), PROPERTY_STORE_ROOT, null);
    _otherZkClient = newZkClient();
    _otherPropertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_otherZkClient), PROPERTY_STORE_ROOT, null);
  }

  private ZkClient newZkClient() {
    ZkClient client =
        new ZkClient.Builder().setZkServer(_zk.getZkUrl()).setZkSerializer(new ZNRecordSerializer()).build();
    assertTrue(client.waitUntilConnected(10_000, TimeUnit.MILLISECONDS));
    return client;
  }

  @AfterClass
  public void afterClass() {
    if (_propertyStore != null) {
      _propertyStore.stop();
    }
    if (_otherPropertyStore != null) {
      _otherPropertyStore.stop();
    }
    if (_zkClient != null) {
      _zkClient.close();
    }
    if (_otherZkClient != null) {
      _otherZkClient.close();
    }
    if (_zk != null) {
      ZkStarter.stopLocalZkServer(_zk);
    }
  }

  /// Each test starts from a clean marker tree so the tests are order independent.
  @BeforeMethod
  public void cleanMarkers() {
    if (_zkClient.exists(PROPERTY_STORE_ROOT + MARKER_PREFIX)) {
      _zkClient.deleteRecursively(PROPERTY_STORE_ROOT + MARKER_PREFIX);
    }
  }

  private static String markerPath(String tableNameWithType) {
    return MARKER_PREFIX + "/" + tableNameWithType;
  }

  /// Writes a marker whose start time is `ageMs` in the past, to simulate a marker left behind by
  /// a controller that crashed mid-deletion.
  private void writeMarkerWithAge(String tableNameWithType, String controllerId, long ageMs) {
    ZNRecord record = new ZNRecord(tableNameWithType);
    record.setSimpleField(CONTROLLER_ID_KEY, controllerId);
    record.setLongField(START_TIME_KEY, System.currentTimeMillis() - ageMs);
    assertTrue(_propertyStore.set(markerPath(tableNameWithType), record, AccessOption.PERSISTENT));
  }

  @Test
  public void testCreateMarker() {
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME));
  }

  @Test
  public void testMarkerRecordsControllerIdAndStartTime() {
    long before = System.currentTimeMillis();
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1));

    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record, "Marker znode should be readable after creation");
    assertEquals(record.getSimpleField(CONTROLLER_ID_KEY), CONTROLLER_1,
        "Marker must record the owning controller so a stale marker can be attributed");
    long startTimeMs = record.getLongField(START_TIME_KEY, -1L);
    assertTrue(startTimeMs >= before && startTimeMs <= System.currentTimeMillis(),
        "Marker start time should be the wall clock time at creation, got: " + startTimeMs);
  }

  @Test
  public void testDuplicateMarkerIsRejected() {
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1));
    assertFalse(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_2),
        "A second controller must not be able to create a marker for a table already being deleted");

    // The original owner must be preserved.
    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record);
    assertEquals(record.getSimpleField(CONTROLLER_ID_KEY), CONTROLLER_1);
  }

  @Test
  public void testNoMarkerWhenNotCreated() {
    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME));
  }

  @Test
  public void testRemoveMarker() {
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME));

    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1);
    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME));

    // A fresh deletion of the same table must be possible once the marker is gone.
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_2));
  }

  @Test
  public void testRemoveNonExistentMarkerIsNoOp() {
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1);
    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME));
  }

  /// A controller that stalled long enough to be taken over must not release the new owner's marker when it
  /// eventually reaches its finally block, or both deletions would run unguarded.
  @Test
  public void testRemoveMarkerDoesNotReleaseAnotherControllersMarker() {
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_2));

    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1);

    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME),
        "Controller 1 must not be able to release a marker owned by controller 2");
    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record);
    assertEquals(record.getSimpleField(CONTROLLER_ID_KEY), CONTROLLER_2);
  }

  @Test
  public void testExpiredMarkerIsNotValid() {
    writeMarkerWithAge(TABLE_NAME, CONTROLLER_1, EXPIRY_MS + 60_000L);
    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME),
        "A marker older than the expiry window must not block progress forever");
  }

  @Test
  public void testMarkerJustInsideExpiryWindowIsStillValid() {
    writeMarkerWithAge(TABLE_NAME, CONTROLLER_1, EXPIRY_MS - 60_000L);
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME),
        "A marker inside the expiry window must still provide mutual exclusion");
  }

  @Test
  public void testMarkerWithoutStartTimeIsNotValid() {
    ZNRecord record = new ZNRecord(TABLE_NAME);
    record.setSimpleField(CONTROLLER_ID_KEY, CONTROLLER_1);
    assertTrue(_propertyStore.set(markerPath(TABLE_NAME), record, AccessOption.PERSISTENT));

    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME),
        "A malformed marker must not permanently block deletion or re-creation");
  }

  @Test
  public void testTakeoverCreatesMarkerWhenNoneExists() {
    assertTrue(ZKMetadataProvider.createOrTakeoverTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME));
  }

  @Test
  public void testTakeoverRejectedWhenValidMarkerExists() {
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1));
    assertFalse(ZKMetadataProvider.createOrTakeoverTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_2),
        "A live deletion must not be stolen by another controller");

    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record);
    assertEquals(record.getSimpleField(CONTROLLER_ID_KEY), CONTROLLER_1);
  }

  @Test
  public void testTakeoverOfExpiredMarkerSucceedsAndReassignsOwner() {
    writeMarkerWithAge(TABLE_NAME, CONTROLLER_1, EXPIRY_MS + 60_000L);

    assertTrue(ZKMetadataProvider.createOrTakeoverTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_2),
        "An expired marker should be reclaimable so a crashed deletion can be retried");

    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record);
    assertEquals(record.getSimpleField(CONTROLLER_ID_KEY), CONTROLLER_2,
        "Takeover must record the new owning controller");
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME),
        "The refreshed marker should be valid again");
  }

  /// The version check is what makes the stale-marker takeover safe, so exercise it directly: a controller that
  /// observed the marker at one version must lose the takeover if anyone else has written to it since.
  ///
  /// This is the deterministic form of the two-controller race. The real interleaving (both controllers reading
  /// the same stale version, then both writing) cannot be forced from a test, so instead the stale version is
  /// supplied explicitly, which is exactly the state the losing controller would be in.
  @Test
  public void testTakeoverWithAStaleVersionLosesTheRace() {
    writeMarkerWithAge(TABLE_NAME, "Controller_crashed", EXPIRY_MS + 60_000L);
    Stat observed = new Stat();
    assertNotNull(_propertyStore.get(markerPath(TABLE_NAME), observed, AccessOption.PERSISTENT));
    int staleVersion = observed.getVersion();

    // Controller 1 reclaims it first, which bumps the version.
    assertTrue(ZKMetadataProvider.createOrTakeoverTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_1));

    // Controller 2 is still holding the version it read before controller 1 won, and must be rejected.
    assertFalse(
        ZKMetadataProvider.takeoverStaleTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_2, staleVersion),
        "A takeover based on a superseded version must fail, otherwise two controllers would both delete the table");

    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record, "The winner's marker must survive a losing takeover attempt");
    assertEquals(record.getSimpleField(CONTROLLER_ID_KEY), CONTROLLER_1,
        "The losing controller must not overwrite the winner's ownership");
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TABLE_NAME));
  }

  /// The matching success case: a takeover with the current version wins and reassigns ownership.
  @Test
  public void testTakeoverWithTheCurrentVersionWins() {
    writeMarkerWithAge(TABLE_NAME, "Controller_crashed", EXPIRY_MS + 60_000L);
    Stat observed = new Stat();
    assertNotNull(_propertyStore.get(markerPath(TABLE_NAME), observed, AccessOption.PERSISTENT));

    assertTrue(ZKMetadataProvider.takeoverStaleTableDeletionMarker(_propertyStore, TABLE_NAME, CONTROLLER_2,
        observed.getVersion()));

    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record);
    assertEquals(record.getSimpleField(CONTROLLER_ID_KEY), CONTROLLER_2);
  }

  /// Two controllers must never both hold the marker for one table. Uses two independent property stores because
  /// a single ZkHelixPropertyStore serializes its own operations and so cannot model two controller processes.
  ///
  /// NOTE: this asserts the invariant but is not a regression test for the non-atomic takeover it replaced. The
  /// vulnerable window in a remove-then-create takeover is sub-millisecond and could not be hit reliably from a
  /// test. The takeover's correctness rests on the ZK version check in
  /// [ZKMetadataProvider#createOrTakeoverTableDeletionMarker], not on this test.
  @Test
  public void testOnlyOneOfTwoControllersAcquiresTheMarker()
      throws Exception {
    CyclicBarrier startLine = new CyclicBarrier(2);
    List<String> contenders = List.of(CONTROLLER_1, CONTROLLER_2);
    List<ZkHelixPropertyStore<ZNRecord>> stores = List.of(_propertyStore, _otherPropertyStore);
    List<Future<Boolean>> results = new ArrayList<>(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < contenders.size(); i++) {
        String contender = contenders.get(i);
        ZkHelixPropertyStore<ZNRecord> store = stores.get(i);
        results.add(executor.submit(() -> {
          startLine.await(10, TimeUnit.SECONDS);
          return ZKMetadataProvider.createOrTakeoverTableDeletionMarker(store, TABLE_NAME, contender);
        }));
      }
      int winners = 0;
      for (Future<Boolean> result : results) {
        if (result.get(20, TimeUnit.SECONDS)) {
          winners++;
        }
      }
      assertEquals(winners, 1, "Exactly one controller may acquire the deletion marker, but " + winners + " did");
    } finally {
      executor.shutdownNow();
    }

    ZNRecord record = _propertyStore.get(markerPath(TABLE_NAME), null, AccessOption.PERSISTENT);
    assertNotNull(record);
    assertTrue(contenders.contains(record.getSimpleField(CONTROLLER_ID_KEY)));
  }

  @Test
  public void testMarkersForDifferentTablesAreIndependent() {
    String realtimeTable = "table1_REALTIME";
    String offlineTable = "table2_OFFLINE";

    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, realtimeTable, CONTROLLER_1));
    assertTrue(ZKMetadataProvider.createTableDeletionMarker(_propertyStore, offlineTable, CONTROLLER_2));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, realtimeTable));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, offlineTable));

    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, realtimeTable, CONTROLLER_1);

    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, realtimeTable));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, offlineTable),
        "Deleting one table must not clear another table's marker");
  }

  @Test
  public void testDeletionInProgressPrefixIsExposedForManualCleanup() {
    assertEquals(ZKMetadataProvider.getPropertyStoreTableDeletionInProgressPrefix(), MARKER_PREFIX);
  }
}
