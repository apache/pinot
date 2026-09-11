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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class ColumnDeletionMetadataTest {

  @Test
  public void testPropertyStorePath() {
    assertThat(ZKMetadataProvider.getPropertyStorePathForColumnDeletionMetadataPrefix())
        .isEqualTo("/COLUMN_DELETION_METADATA");
    assertThat(ZKMetadataProvider.constructPropertyStorePathForColumnDeletionMetadata("foo_OFFLINE"))
        .isEqualTo("/COLUMN_DELETION_METADATA/foo_OFFLINE");
    assertThat(ZKMetadataProvider.constructPropertyStorePathForColumnDeletionMetadata("foo_REALTIME"))
        .isEqualTo("/COLUMN_DELETION_METADATA/foo_REALTIME");
  }

  @Test
  public void testEmptyLedgerRoundTrip() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    ColumnDeletionMetadata decoded = ColumnDeletionMetadata.fromZNRecord(metadata.toZNRecord());
    assertThat(decoded).isEqualTo(metadata);
    assertThat(decoded.getFormatVersion()).isEqualTo(ColumnDeletionMetadata.CURRENT_FORMAT_VERSION);
    assertThat(decoded.getEntries()).isEmpty();
    assertThat(decoded.getActiveDeletedColumnNames(false)).isEmpty();
  }

  @Test
  public void testRoundTripPreservesImmutableEpoch() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    ColumnDeletionEntry entry =
        new ColumnDeletionEntry("city", "del-1", 1_700_000_000_000L, 7, 1_700_000_000_100L,
            ColumnDeletionState.RECLAIMING, "refresh timed out", 3);
    metadata.addEntry(entry);

    ColumnDeletionMetadata decoded = ColumnDeletionMetadata.fromZNRecord(metadata.toZNRecord());
    ColumnDeletionEntry decodedEntry = decoded.getEntry("del-1");
    assertThat(decodedEntry).isEqualTo(entry);
    assertThat(decodedEntry.getDeletionEpochMs()).isEqualTo(1_700_000_000_000L);
    assertThat(decodedEntry.getSchemaZkVersion()).isEqualTo(7);
    assertThat(decodedEntry.getSchemaZkMtimeMs()).isEqualTo(1_700_000_000_100L);
    assertThat(decodedEntry.getLastError()).isEqualTo("refresh timed out");
    assertThat(decodedEntry.getOutstandingSegmentCount()).isEqualTo(3);
  }

  @Test
  public void testProgressUpdateDoesNotChangeEpoch() {
    ColumnDeletionEntry original =
        new ColumnDeletionEntry("city", "del-1", 1000L, 3, ColumnDeletionState.RECLAIMING);
    ColumnDeletionEntry updated = original.withLastError("minion gone").withOutstandingSegmentCount(12);

    assertThat(updated.getDeletionEpochMs()).isEqualTo(1000L);
    assertThat(updated.getSchemaZkVersion()).isEqualTo(3);
    assertThat(updated.getColumnName()).isEqualTo("city");
    assertThat(updated.getLastError()).isEqualTo("minion gone");
    assertThat(updated.getOutstandingSegmentCount()).isEqualTo(12);
  }

  @Test
  public void testMultipleDeletionsAndReAddGate() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    metadata.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 1, ColumnDeletionState.RECLAIMING));
    metadata.addEntry(new ColumnDeletionEntry("zip", "del-2", 2000L, 2, ColumnDeletionState.COMPLETE));
    metadata.addEntry(new ColumnDeletionEntry("oldCol", "del-3", 3000L, 3, ColumnDeletionState.FAILED));

    assertThat(metadata.getActiveDeletedColumnNames(false)).containsExactly("city", "oldCol");
    assertThat(metadata.blocksReAdd("city", false)).isTrue();
    assertThat(metadata.blocksReAdd("zip", false)).isFalse();
    assertThat(metadata.blocksReAdd("oldCol", false)).isTrue();
    assertThat(metadata.blocksReAdd("other", false)).isFalse();
    assertThat(metadata.findActiveEntryForColumn("zip", false)).isNull();
    assertThat(metadata.findActiveEntryForColumn("city", false).getDeletionId()).isEqualTo("del-1");
  }

  @Test
  public void testIgnoreCaseReAddBlock() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    metadata.addEntry(new ColumnDeletionEntry("City", "del-1", 1000L, 1, ColumnDeletionState.PENDING));

    assertThat(metadata.blocksReAdd("city", true)).isTrue();
    assertThat(metadata.blocksReAdd("CITY", true)).isTrue();
    assertThat(metadata.blocksReAdd("city", false)).isFalse();
    assertThat(metadata.getActiveDeletedColumnNames(true)).containsExactly("city");
  }

  @Test
  public void testEveryNonCompleteStateBlocksReAdd() {
    for (ColumnDeletionState state : ColumnDeletionState.values()) {
      ColumnDeletionEntry entry = new ColumnDeletionEntry("col", "del-" + state, 1L, 0, state);
      assertThat(entry.blocksReAdd()).isEqualTo(state != ColumnDeletionState.COMPLETE);
    }
  }

  @Test
  public void testReconcilePreparedAbortsWhenColumnStillPresent() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    metadata.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 4, ColumnDeletionState.PREPARED));

    assertThat(metadata.reconcilePreparedEntry("del-1", true)).isNull();
    assertThat(metadata.getEntry("del-1")).isNull();
    assertThat(metadata.blocksReAdd("city", false)).isFalse();
  }

  @Test
  public void testReconcilePreparedAdvancesWhenColumnAlreadyGone() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    metadata.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 4, ColumnDeletionState.PREPARED));

    ColumnDeletionEntry advanced = metadata.reconcilePreparedEntry("del-1", false);
    assertThat(advanced.getState()).isEqualTo(ColumnDeletionState.RECLAIMING);
    assertThat(advanced.getDeletionEpochMs()).isEqualTo(1000L);
    assertThat(metadata.getEntry("del-1").getState()).isEqualTo(ColumnDeletionState.RECLAIMING);
  }

  @Test
  public void testReconcilePreparedLeavesOtherStatesAlone() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    metadata.addEntry(new ColumnDeletionEntry("city", "del-1", 1000L, 4, ColumnDeletionState.RECLAIMING));

    ColumnDeletionEntry unchanged = metadata.reconcilePreparedEntry("del-1", true);
    assertThat(unchanged.getState()).isEqualTo(ColumnDeletionState.RECLAIMING);
    assertThat(metadata.getEntry("del-1").getState()).isEqualTo(ColumnDeletionState.RECLAIMING);
  }

  @Test
  public void testMissingFormatVersionRejected() {
    ZNRecord record = new ZNRecord("foo_OFFLINE");
    assertThatThrownBy(() -> ColumnDeletionMetadata.fromZNRecord(record)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("formatVersion");
  }

  @Test
  public void testNewerFormatVersionRejected() {
    ZNRecord record = new ZNRecord("foo_OFFLINE");
    record.setSimpleField("formatVersion", Integer.toString(ColumnDeletionMetadata.CURRENT_FORMAT_VERSION + 1));
    assertThatThrownBy(() -> ColumnDeletionMetadata.fromZNRecord(record)).isInstanceOf(
        ColumnDeletionUnsupportedFormatException.class);
  }

  @Test
  public void testFormatVersionZeroRejected() {
    ZNRecord record = new ZNRecord("foo_OFFLINE");
    record.setSimpleField("formatVersion", "0");
    assertThatThrownBy(() -> ColumnDeletionMetadata.fromZNRecord(record)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid formatVersion");
  }

  @Test
  public void testToZNRecordRefusesNewerInMemoryFormat() {
    ColumnDeletionMetadata metadata =
        new ColumnDeletionMetadata("foo_OFFLINE", ColumnDeletionMetadata.CURRENT_FORMAT_VERSION + 1, Map.of());
    assertThatThrownBy(metadata::toZNRecord).isInstanceOf(ColumnDeletionUnsupportedFormatException.class);
  }

  @Test
  public void testUnknownStateRejected() {
    ZNRecord record = new ZNRecord("foo_OFFLINE");
    record.setSimpleField("formatVersion", "1");
    Map<String, String> fields = new HashMap<>();
    fields.put("columnName", "city");
    fields.put("deletionId", "del-1");
    fields.put("deletionEpochMs", "1");
    fields.put("schemaZkVersion", "1");
    fields.put("state", "VANISHED");
    record.setMapField("del-1", fields);

    assertThatThrownBy(() -> ColumnDeletionMetadata.fromZNRecord(record)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown state");
  }

  @Test
  public void testDeletionIdMismatchRejected() {
    ZNRecord record = new ZNRecord("foo_OFFLINE");
    record.setSimpleField("formatVersion", "1");
    Map<String, String> fields = new HashMap<>();
    fields.put("columnName", "city");
    fields.put("deletionId", "other-id");
    fields.put("deletionEpochMs", "1");
    fields.put("schemaZkVersion", "1");
    fields.put("state", "RECLAIMING");
    record.setMapField("del-1", fields);

    assertThatThrownBy(() -> ColumnDeletionMetadata.fromZNRecord(record)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not match");
  }

  @Test
  public void testUnknownMapKeysAreIgnored() {
    ZNRecord record = new ZNRecord("foo_OFFLINE");
    record.setSimpleField("formatVersion", "1");
    Map<String, String> fields = new HashMap<>();
    fields.put("columnName", "city");
    fields.put("deletionId", "del-1");
    fields.put("deletionEpochMs", "9");
    fields.put("schemaZkVersion", "2");
    fields.put("state", "PENDING");
    fields.put("futureField", "ok");
    record.setMapField("del-1", fields);

    ColumnDeletionEntry entry = ColumnDeletionMetadata.fromZNRecord(record).getEntry("del-1");
    assertThat(entry.getColumnName()).isEqualTo("city");
    assertThat(entry.getState()).isEqualTo(ColumnDeletionState.PENDING);
    assertThat(entry.getDeletionEpochMs()).isEqualTo(9L);
  }

  @Test
  public void testDuplicateDeletionIdRejected() {
    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    ColumnDeletionEntry entry = new ColumnDeletionEntry("city", "del-1", 1L, 0, ColumnDeletionState.PREPARED);
    metadata.addEntry(entry);
    assertThatThrownBy(() -> metadata.addEntry(entry)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testNewDeletionIdIsUnique() {
    assertThat(ColumnDeletionMetadata.newDeletionId()).isNotEqualTo(ColumnDeletionMetadata.newDeletionId());
  }
}
