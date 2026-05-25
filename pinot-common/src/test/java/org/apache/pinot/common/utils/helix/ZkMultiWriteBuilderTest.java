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

import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ZkMultiWriteBuilderTest {

  private static final String ROOT = "/ZkMultiWriteBuilderTest";

  private ZkStarter.ZookeeperInstance _zk;
  private ZkClient _client;

  @BeforeClass
  public void beforeClass() {
    _zk = ZkStarter.startLocalZkServer();
    _client = new ZkClient.Builder()
        .setZkServer(_zk.getZkUrl())
        .setZkSerializer(new ZNRecordSerializer())
        .build();
    Assert.assertTrue(_client.waitUntilConnected(10_000, TimeUnit.MILLISECONDS));
  }

  @AfterClass
  public void afterClass() {
    if (_client != null) {
      _client.close();
    }
    if (_zk != null) {
      ZkStarter.stopLocalZkServer(_zk);
    }
  }

  @BeforeMethod
  public void cleanRoot() {
    if (_client.exists(ROOT)) {
      _client.deleteRecursively(ROOT);
    }
    _client.createPersistent(ROOT, true);
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static ZNRecord record(String id, String key, String value) {
    ZNRecord r = new ZNRecord(id);
    r.setSimpleField(key, value);
    return r;
  }

  private void seed(String path, ZNRecord rec) {
    _client.createPersistent(path, rec);
  }

  private ZNRecord read(String path) {
    return _client.readData(path, true);
  }

  private int version(String path) {
    Stat s = new Stat();
    _client.readData(path, s);
    return s.getVersion();
  }

  private ZkMultiWriteBuilder builder() {
    // Tests use absolute paths under ROOT, so pass empty prefix (no property-store rebase).
    return new ZkMultiWriteBuilder(_client, "");
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------

  @Test
  public void testAllSetSuccess()
      throws KeeperException {
    String pA = ROOT + "/a";
    String pB = ROOT + "/b";
    seed(pA, record("a", "v", "1"));
    seed(pB, record("b", "v", "1"));

    builder()
        .set(pA, record("a", "v", "2"), 0)
        .set(pB, record("b", "v", "2"), 0)
        .execute();

    Assert.assertEquals(read(pA).getSimpleField("v"), "2");
    Assert.assertEquals(read(pB).getSimpleField("v"), "2");
    Assert.assertEquals(version(pA), 1);
    Assert.assertEquals(version(pB), 1);
  }

  @Test
  public void testMixedCreateSetDeleteSuccess()
      throws KeeperException {
    String pExisting = ROOT + "/existing";
    String pNew = ROOT + "/new";
    String pStale = ROOT + "/stale";
    seed(pExisting, record("existing", "v", "1"));
    seed(pStale, record("stale", "v", "x"));

    builder()
        .set(pExisting, record("existing", "v", "2"), 0)
        .create(pNew, record("new", "v", "1"))
        .delete(pStale, 0)
        .execute();

    Assert.assertEquals(read(pExisting).getSimpleField("v"), "2");
    Assert.assertEquals(read(pNew).getSimpleField("v"), "1");
    Assert.assertFalse(_client.exists(pStale));
  }

  @Test
  public void testBadVersionAtomicRollback() {
    String pA = ROOT + "/a";
    String pB = ROOT + "/b";
    seed(pA, record("a", "v", "1"));
    seed(pB, record("b", "v", "1"));

    // Bump version on pB so the expected-0 check on it will fail.
    _client.writeData(pB, record("b", "v", "bumped"));
    Assert.assertEquals(version(pB), 1);

    Assert.expectThrows(KeeperException.BadVersionException.class, () ->
        builder()
            .set(pA, record("a", "v", "2"), 0)
            .set(pB, record("b", "v", "2"), 0) // stale version -> BADVERSION
            .execute());

    // Atomic rollback — pA must NOT have been updated.
    Assert.assertEquals(read(pA).getSimpleField("v"), "1");
    Assert.assertEquals(version(pA), 0);
    Assert.assertEquals(read(pB).getSimpleField("v"), "bumped");
  }

  @Test
  public void testCheckOpGatesSet() {
    String pGate = ROOT + "/gate";
    String pTarget = ROOT + "/target";
    seed(pGate, record("gate", "v", "1"));
    seed(pTarget, record("target", "v", "1"));

    // Bump gate's version; check(gate, 0) should fail and prevent the set.
    _client.writeData(pGate, record("gate", "v", "bumped"));

    Assert.expectThrows(KeeperException.BadVersionException.class, () ->
        builder()
            .check(pGate, 0)
            .set(pTarget, record("target", "v", "2"), 0)
            .execute());

    Assert.assertEquals(read(pTarget).getSimpleField("v"), "1", "target must not have been mutated");
  }

  @Test
  public void testDeleteNonExistentRollback() {
    String pExisting = ROOT + "/existing";
    String pMissing = ROOT + "/missing";
    seed(pExisting, record("existing", "v", "1"));

    Assert.expectThrows(KeeperException.NoNodeException.class, () ->
        builder()
            .set(pExisting, record("existing", "v", "2"), 0)
            .delete(pMissing)
            .execute());

    // pExisting must NOT have been updated.
    Assert.assertEquals(read(pExisting).getSimpleField("v"), "1");
  }

  @Test
  public void testCreateExistingNodeRollback() {
    String pA = ROOT + "/a";
    String pB = ROOT + "/b";
    seed(pA, record("a", "v", "1"));
    seed(pB, record("b", "v", "existing"));

    Assert.expectThrows(KeeperException.NodeExistsException.class, () ->
        builder()
            .set(pA, record("a", "v", "2"), 0)
            .create(pB, record("b", "v", "fresh"))
            .execute());

    Assert.assertEquals(read(pA).getSimpleField("v"), "1");
    Assert.assertEquals(read(pB).getSimpleField("v"), "existing");
  }

  @Test
  public void testAnyVersionSetSucceedsRegardlessOfVersion()
      throws KeeperException {
    String pA = ROOT + "/a";
    seed(pA, record("a", "v", "1"));
    _client.writeData(pA, record("a", "v", "bumped"));
    _client.writeData(pA, record("a", "v", "bumped-again"));
    Assert.assertEquals(version(pA), 2);

    builder().set(pA, record("a", "v", "final")).execute();

    Assert.assertEquals(read(pA).getSimpleField("v"), "final");
  }

  @Test
  public void testBuilderRejectsDoubleExecute()
      throws KeeperException {
    String pA = ROOT + "/a";
    seed(pA, record("a", "v", "1"));

    // After a successful execute(), the builder rejects further calls.
    ZkMultiWriteBuilder b = builder().set(pA, record("a", "v", "2"), 0);
    b.execute();
    Assert.expectThrows(IllegalStateException.class, b::execute);
    Assert.expectThrows(IllegalStateException.class, () -> b.set(pA, record("a", "v", "3"), 1));

    // After a failed execute() (atomic rollback), the builder is also burned — no retry through
    // the same instance. Caller must obtain a fresh builder for the retry tick.
    ZkMultiWriteBuilder failed = builder().set(pA, record("a", "v", "x"), 99); // stale version
    Assert.expectThrows(KeeperException.BadVersionException.class, failed::execute);
    Assert.expectThrows(IllegalStateException.class, failed::execute);
    Assert.expectThrows(IllegalStateException.class, () -> failed.set(pA, record("a", "v", "y"), 1));

    // Empty execute() also burns the builder.
    ZkMultiWriteBuilder empty = builder();
    Assert.expectThrows(IllegalStateException.class, empty::execute);
    Assert.expectThrows(IllegalStateException.class, empty::execute);
  }
}
