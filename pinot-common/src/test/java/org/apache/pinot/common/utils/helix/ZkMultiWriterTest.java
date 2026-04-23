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


public class ZkMultiWriterTest {

  private static final String ROOT = "/ZkMultiWriterTest";

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

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------

  @Test
  public void testAllSetSuccess() {
    String pA = ROOT + "/a";
    String pB = ROOT + "/b";
    seed(pA, record("a", "v", "1"));
    seed(pB, record("b", "v", "1"));

    PinotZkMultiResult res = ZkMultiWriter.multi(_client, List.of(
        PinotZkOp.set(pA, record("a", "v", "2"), 0),
        PinotZkOp.set(pB, record("b", "v", "2"), 0)));

    Assert.assertTrue(res.isSuccess());
    Assert.assertEquals(res.getFailedOpIndex(), -1);
    Assert.assertNull(res.getFailureCode());
    Assert.assertEquals(res.getOutcomes().size(), 2);
    Assert.assertNotNull(res.getOutcomes().get(0).getStat());
    Assert.assertEquals(res.getOutcomes().get(0).getStat().getVersion(), 1);
    Assert.assertEquals(read(pA).getSimpleField("v"), "2");
    Assert.assertEquals(read(pB).getSimpleField("v"), "2");
  }

  @Test
  public void testMixedCreateSetDeleteSuccess() {
    String pExisting = ROOT + "/existing";
    String pNew = ROOT + "/new";
    String pStale = ROOT + "/stale";
    seed(pExisting, record("existing", "v", "1"));
    seed(pStale, record("stale", "v", "x"));

    PinotZkMultiResult res = ZkMultiWriter.multi(_client, List.of(
        PinotZkOp.set(pExisting, record("existing", "v", "2"), 0),
        PinotZkOp.create(pNew, record("new", "v", "1")),
        PinotZkOp.delete(pStale, 0)));

    Assert.assertTrue(res.isSuccess(), "result: " + res);
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

    PinotZkMultiResult res = ZkMultiWriter.multi(_client, List.of(
        PinotZkOp.set(pA, record("a", "v", "2"), 0),
        PinotZkOp.set(pB, record("b", "v", "2"), 0))); // stale version -> BADVERSION

    Assert.assertFalse(res.isSuccess());
    Assert.assertEquals(res.getFailedOpIndex(), 1);
    Assert.assertEquals(res.getFailureCode(), KeeperException.Code.BADVERSION);

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

    PinotZkMultiResult res = ZkMultiWriter.multi(_client, List.of(
        PinotZkOp.check(pGate, 0),
        PinotZkOp.set(pTarget, record("target", "v", "2"), 0)));

    Assert.assertFalse(res.isSuccess());
    Assert.assertEquals(res.getFailedOpIndex(), 0);
    Assert.assertEquals(res.getFailureCode(), KeeperException.Code.BADVERSION);
    Assert.assertEquals(read(pTarget).getSimpleField("v"), "1", "target must not have been mutated");
  }

  @Test
  public void testDeleteNonExistentRollback() {
    String pExisting = ROOT + "/existing";
    String pMissing = ROOT + "/missing";
    seed(pExisting, record("existing", "v", "1"));

    PinotZkMultiResult res = ZkMultiWriter.multi(_client, List.of(
        PinotZkOp.set(pExisting, record("existing", "v", "2"), 0),
        PinotZkOp.delete(pMissing, PinotZkOp.ANY_VERSION)));

    Assert.assertFalse(res.isSuccess());
    Assert.assertEquals(res.getFailedOpIndex(), 1);
    Assert.assertEquals(res.getFailureCode(), KeeperException.Code.NONODE);
    // pExisting must NOT have been updated.
    Assert.assertEquals(read(pExisting).getSimpleField("v"), "1");
  }

  @Test
  public void testCreateExistingNodeRollback() {
    String pA = ROOT + "/a";
    String pB = ROOT + "/b";
    seed(pA, record("a", "v", "1"));
    seed(pB, record("b", "v", "existing"));

    PinotZkMultiResult res = ZkMultiWriter.multi(_client, List.of(
        PinotZkOp.set(pA, record("a", "v", "2"), 0),
        PinotZkOp.create(pB, record("b", "v", "fresh"))));

    Assert.assertFalse(res.isSuccess());
    Assert.assertEquals(res.getFailedOpIndex(), 1);
    Assert.assertEquals(res.getFailureCode(), KeeperException.Code.NODEEXISTS);
    Assert.assertEquals(read(pA).getSimpleField("v"), "1");
    Assert.assertEquals(read(pB).getSimpleField("v"), "existing");
  }

  @Test
  public void testAnyVersionSetSucceedsRegardlessOfVersion() {
    String pA = ROOT + "/a";
    seed(pA, record("a", "v", "1"));
    _client.writeData(pA, record("a", "v", "bumped"));
    _client.writeData(pA, record("a", "v", "bumped-again"));
    Assert.assertEquals(version(pA), 2);

    PinotZkMultiResult res = ZkMultiWriter.multi(_client, List.of(
        PinotZkOp.set(pA, record("a", "v", "final"), PinotZkOp.ANY_VERSION)));

    Assert.assertTrue(res.isSuccess());
    Assert.assertEquals(read(pA).getSimpleField("v"), "final");
  }
}
