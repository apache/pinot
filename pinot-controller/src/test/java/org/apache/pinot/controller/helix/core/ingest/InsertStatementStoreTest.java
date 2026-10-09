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
package org.apache.pinot.controller.helix.core.ingest;

import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests property-store failures and round-trip persistence; the cluster IT covers atomic reservation races.
public class InsertStatementStoreTest {
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private InsertStatementStore _store;
  private static final String REQUEST_PATH = "/INSERT_REQUEST_IDS/t_OFFLINE/r1";
  private static final String STATEMENT_PATH = "/INSERT_STATEMENTS/t_OFFLINE/s1";

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    _propertyStore = mock(ZkHelixPropertyStore.class);
    _store = new InsertStatementStore(_propertyStore);
  }

  private static InsertStatementManifest manifest() {
    return new InsertStatementManifest("s1", "r1", "hash", "t_OFFLINE", InsertType.ROW,
        InsertStatementState.ACCEPTED, 1, 1, List.of(), null, null);
  }

  @Test
  public void testCreateOnlyReservation() {
    when(_propertyStore.create(eq(REQUEST_PATH), any(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    assertNull(_store.reserveRequestId("t_OFFLINE", "r1", "s1"));
    ZNRecord existing = new ZNRecord("r1");
    existing.setSimpleField("statementId", "s1");
    when(_propertyStore.create(eq(REQUEST_PATH), any(), eq(AccessOption.PERSISTENT))).thenReturn(false);
    when(_propertyStore.get(eq(REQUEST_PATH), isNull(), eq(AccessOption.PERSISTENT))).thenReturn(existing);
    assertEquals(_store.reserveRequestId("t_OFFLINE", "r1", "s2"), "s1");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUnreadableReservationFailsClosed() {
    _store.reserveRequestId("t_OFFLINE", "r1", "s1");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testMalformedReservationFailsClosed() {
    when(_propertyStore.get(eq(REQUEST_PATH), isNull(), eq(AccessOption.PERSISTENT)))
        .thenReturn(new ZNRecord("r1"));
    _store.reserveRequestId("t_OFFLINE", "r1", "s1");
  }

  @Test
  public void testManifestRoundTrip() throws Exception {
    ZNRecord record = new ZNRecord("s1");
    record.setSimpleField("manifest", manifest().toJsonString());
    when(_propertyStore.create(eq(STATEMENT_PATH), any(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    assertTrue(_store.createStatement(manifest()));
    when(_propertyStore.get(eq(STATEMENT_PATH), isNull(), eq(AccessOption.PERSISTENT))).thenReturn(record);
    assertEquals(_store.getStatement("t_OFFLINE", "s1").getPayloadHash(), "hash");
    when(_propertyStore.getChildren(eq("/INSERT_STATEMENTS/t_OFFLINE"), isNull(), eq(AccessOption.PERSISTENT),
        eq(0), eq(0)))
        .thenReturn(List.of(record));
    assertEquals(_store.listStatements("t_OFFLINE").size(), 1);
  }

  @Test
  public void testCannotUpdateMissingManifest() {
    assertFalse(_store.updateStatement(manifest()));
  }

  @Test
  public void testFailedUpdateIsReported() throws Exception {
    ZNRecord record = new ZNRecord("s1");
    record.setSimpleField("manifest", manifest().toJsonString());
    when(_propertyStore.get(eq(STATEMENT_PATH), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(record);
    when(_propertyStore.set(eq(STATEMENT_PATH), any(), anyInt(), eq(AccessOption.PERSISTENT))).thenReturn(false);
    assertFalse(_store.updateStatement(manifest()));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testReadFailureIsNotReportedAsMissing() {
    when(_propertyStore.get(eq(STATEMENT_PATH), isNull(), eq(AccessOption.PERSISTENT)))
        .thenThrow(new IllegalStateException("ZK down"));
    _store.getStatement("t_OFFLINE", "s1");
  }
}
