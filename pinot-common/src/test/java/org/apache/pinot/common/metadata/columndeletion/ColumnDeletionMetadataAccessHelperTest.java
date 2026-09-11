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
package org.apache.pinot.common.metadata.columndeletion;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ColumnDeletionMetadataAccessHelperTest {
  private static final String TABLE = "foo_OFFLINE";
  private static final String PATH = ZKMetadataProvider.constructPropertyStorePathForColumnDeletionMetadata(TABLE);

  @Test
  public void testMissingZnodeReturnsNull() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    assertThat(ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE)).isNull();
  }

  @Test
  public void testCreateThenRead() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata(TABLE);
    metadata.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));

    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), metadata, -1))
        .isTrue();
    assertThat(store.version()).isEqualTo(0);

    ZNRecord znRecord = ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadataZNRecord(store.propertyStore(),
        TABLE);
    assertThat(znRecord.getVersion()).isEqualTo(0);
    ColumnDeletionMetadata decoded =
        ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE);
    assertThat(decoded.getEntry("del-1").getState()).isEqualTo(ColumnDeletionState.PREPARED);
    assertThat(decoded.getEntry("del-1").getDeletionEpochMs()).isEqualTo(1000L);
  }

  @Test
  public void testCasSuccessAndConflict() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata(TABLE);
    metadata.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), metadata, -1))
        .isTrue();

    ColumnDeletionMetadata firstReader =
        ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE);
    firstReader.updateEntry(firstReader.getEntry("del-1").withState(ColumnDeletionState.RECLAIMING));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), firstReader, 0))
        .isTrue();
    assertThat(store.version()).isEqualTo(1);

    ColumnDeletionMetadata staleWriter = new ColumnDeletionMetadata(TABLE);
    staleWriter.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.FAILED));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), staleWriter, 0))
        .isFalse();

    ColumnDeletionMetadata current =
        ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE);
    assertThat(current.getEntry("del-1").getState()).isEqualTo(ColumnDeletionState.RECLAIMING);
    assertThat(current.getEntry("del-1").getDeletionEpochMs()).isEqualTo(1000L);
  }

  @Test
  public void testRestartReconcilesPrepared() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata prepared = new ColumnDeletionMetadata(TABLE);
    prepared.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), prepared, -1))
        .isTrue();

    // Crash after PREPARED. Schema write landed, so the column is already absent.
    ColumnDeletionMetadata afterRestart =
        ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE);
    afterRestart.reconcilePreparedEntry("del-1", false);
    ZNRecord znRecord = ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadataZNRecord(store.propertyStore(),
        TABLE);
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), afterRestart,
        znRecord.getVersion())).isTrue();

    ColumnDeletionMetadata recovered =
        ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE);
    assertThat(recovered.getEntry("del-1").getState()).isEqualTo(ColumnDeletionState.RECLAIMING);
    assertThat(recovered.blocksReAdd("city", false)).isTrue();
  }

  @Test
  public void testRestartAbortsPreparedWhenSchemaUnchanged() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata prepared = new ColumnDeletionMetadata(TABLE);
    prepared.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), prepared, -1))
        .isTrue();

    ColumnDeletionMetadata afterRestart =
        ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE);
    afterRestart.reconcilePreparedEntry("del-1", true);
    ZNRecord znRecord = ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadataZNRecord(store.propertyStore(),
        TABLE);
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), afterRestart,
        znRecord.getVersion())).isTrue();

    ColumnDeletionMetadata recovered =
        ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE);
    assertThat(recovered.getEntry("del-1")).isNull();
    assertThat(recovered.blocksReAdd("city", false)).isFalse();
  }

  @Test
  public void testWriteRefusesNewerInMemoryFormat() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata newer =
        new ColumnDeletionMetadata(TABLE, ColumnDeletionMetadata.CURRENT_FORMAT_VERSION + 1, Map.of());
    assertThatThrownBy(
        () -> ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), newer, -1))
        .isInstanceOf(ColumnDeletionUnsupportedFormatException.class);
    assertThat(store.record()).isNull();
  }

  @Test
  public void testReadRefusesNewerStoredFormat() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ZNRecord newer = new ZNRecord(TABLE);
    newer.setSimpleField("formatVersion", Integer.toString(ColumnDeletionMetadata.CURRENT_FORMAT_VERSION + 1));
    store.seed(newer);

    assertThatThrownBy(
        () -> ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE))
        .isInstanceOf(ColumnDeletionUnsupportedFormatException.class);
  }

  @Test
  public void testWriteSpecificVersionOnMissingZnodeReturnsFalse() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata(TABLE);
    metadata.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), metadata, 0))
        .isFalse();
    assertThat(store.record()).isNull();
  }

  @Test
  public void testWriteWildcardVersionOnExistingCurrentFormatUsesObservedVersion() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata created = new ColumnDeletionMetadata(TABLE);
    created.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), created, -1))
        .isTrue();

    ColumnDeletionMetadata updated = new ColumnDeletionMetadata(TABLE);
    updated.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.RECLAIMING));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), updated, -1))
        .isTrue();
    assertThat(store.version()).isEqualTo(1);
    assertThat(ColumnDeletionMetadataAccessHelper.getColumnDeletionMetadata(store.propertyStore(), TABLE)
        .getEntry("del-1").getState()).isEqualTo(ColumnDeletionState.RECLAIMING);
  }

  @Test
  public void testWriteWildcardVersionLosesCasWhenVersionMoves() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ColumnDeletionMetadata created = new ColumnDeletionMetadata(TABLE);
    created.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), created, -1))
        .isTrue();

    store.bumpVersionAfterNextGet();
    ColumnDeletionMetadata updated = new ColumnDeletionMetadata(TABLE);
    updated.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.RECLAIMING));
    assertThat(ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), updated, -1))
        .isFalse();
    assertThat(ColumnDeletionMetadata.fromZNRecord(store.record()).getEntry("del-1").getState()).isEqualTo(
        ColumnDeletionState.PREPARED);
  }

  @Test
  public void testWriteWildcardVersionDoesNotClobberNewerStoredFormat() {
    InMemoryLedgerStore store = new InMemoryLedgerStore();
    ZNRecord newer = new ZNRecord(TABLE);
    newer.setSimpleField("formatVersion", Integer.toString(ColumnDeletionMetadata.CURRENT_FORMAT_VERSION + 1));
    newer.setSimpleField("futureField", "keep-me");
    store.seed(newer);

    ColumnDeletionMetadata v1 = new ColumnDeletionMetadata(TABLE);
    v1.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 5, ColumnDeletionState.PREPARED));
    assertThatThrownBy(
        () -> ColumnDeletionMetadataAccessHelper.writeColumnDeletionMetadata(store.propertyStore(), v1, -1))
        .isInstanceOf(ColumnDeletionUnsupportedFormatException.class);
    assertThat(store.record().getSimpleField("formatVersion")).isEqualTo(
        Integer.toString(ColumnDeletionMetadata.CURRENT_FORMAT_VERSION + 1));
    assertThat(store.record().getSimpleField("futureField")).isEqualTo("keep-me");
    assertThat(store.version()).isEqualTo(0);
  }

  /// Minimal PropertyStore stand-in that versions a single ledger znode.
  private static final class InMemoryLedgerStore {
    private final AtomicReference<ZNRecord> _record = new AtomicReference<>();
    private final AtomicInteger _version = new AtomicInteger(-1);
    private final AtomicInteger _bumpAfterGet = new AtomicInteger(0);
    private final ZkHelixPropertyStore<ZNRecord> _propertyStore = mockPropertyStore();

    @SuppressWarnings("unchecked")
    private ZkHelixPropertyStore<ZNRecord> mockPropertyStore() {
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      when(propertyStore.get(eq(PATH), nullable(Stat.class), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
        Stat stat = invocation.getArgument(1);
        ZNRecord record = _record.get();
        if (record != null && stat != null) {
          stat.setVersion(_version.get());
        }
        if (record != null && _bumpAfterGet.getAndSet(0) > 0) {
          _version.incrementAndGet();
        }
        return record;
      });
      when(propertyStore.create(eq(PATH), any(ZNRecord.class), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
        if (_record.get() != null) {
          return false;
        }
        _record.set(invocation.getArgument(1));
        _version.set(0);
        return true;
      });
      when(propertyStore.set(eq(PATH), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT))).thenAnswer(
          invocation -> {
            int expectedVersion = invocation.getArgument(2);
            int currentVersion = _version.get();
            if (expectedVersion != -1 && expectedVersion != currentVersion) {
              throw new ZkBadVersionException("expected " + expectedVersion + " but was " + currentVersion);
            }
            _record.set(invocation.getArgument(1));
            _version.set(currentVersion < 0 ? 0 : currentVersion + 1);
            return true;
          });
      return propertyStore;
    }

    ZkHelixPropertyStore<ZNRecord> propertyStore() {
      return _propertyStore;
    }

    int version() {
      return _version.get();
    }

    ZNRecord record() {
      return _record.get();
    }

    void seed(ZNRecord record) {
      _record.set(record);
      _version.set(0);
    }

    void bumpVersionAfterNextGet() {
      _bumpAfterGet.set(1);
    }
  }
}
