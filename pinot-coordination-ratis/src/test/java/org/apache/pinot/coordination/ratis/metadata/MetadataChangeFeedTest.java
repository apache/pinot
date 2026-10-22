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
package org.apache.pinot.coordination.ratis.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MetadataCatalog.SnapshotData;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangeTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRevision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ResponseTooLargeException;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies coherent snapshots, complete transaction feeds, explicit retention loss and durable journal recovery.
public class MetadataChangeFeedTest {
  private static final FeedScope ALL = new FeedScope(Set.of(RecordKind.values()), "authorized-catalog-v1");
  private static final FeedScope SCHEMAS = new FeedScope(Set.of(RecordKind.SCHEMA), "authorized-catalog-v1");

  @Test
  public void testSnapshotInterleavingAndWholeTransactions()
      throws Exception {
    Fixture fixture = new Fixture();
    CatalogImage initial = fixture._catalog.captureImage(ALL);
    UUID configId = UUID.randomUUID();
    assertEquals(fixture.execute(fixture.transaction("create", 1,
        Mutation.createClusterConfig(fixture._history, configId, Map.of("key", "old")),
        fixture.schema("schema"))).status(), Status.OK);
    CatalogImage captured = fixture._catalog.captureImage(ALL);
    SnapshotData diskCapture = fixture._catalog.captureSnapshotData();
    assertEquals(fixture.execute(fixture.transaction("update", 1,
        Mutation.updateClusterConfig(new ObjectVersion(fixture._history, configId, 1), Map.of("key", "new"))))
        .status(), Status.OK);

    assertTrue(initial.records().isEmpty());
    assertEquals(captured.records().size(), 2);
    assertEquals(captured.records().getFirst().value().path("key").asText(), "old");
    ((ObjectNode) captured.records().getFirst().value()).put("key", "escaped");
    assertEquals(captured.records().getFirst().value().path("key").asText(), "old");
    ChangePage allChanges = fixture.changes(ALL, initial.cursor());
    assertEquals(allChanges.transactions().size(), 2);
    assertEquals(allChanges.transactions().getFirst().records().size(), 2);
    assertEquals(allChanges.transactions().getLast().records().getFirst().value().path("key").asText(), "new");
    assertChain(allChanges);
    assertEquals(fixture.changes(ALL, captured.cursor()).transactions().size(), 1);
    assertEquals(diskCapture.toJson().path("lastAppliedIndex").asLong(), captured.cursor().index());
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(diskCapture.toJson());
    assertEquals(restored.captureImage(ALL), captured);
  }

  @Test
  public void testFiltersAndFailuresAdvanceAcrossNonDataRaftIndexes()
      throws Exception {
    Fixture fixture = new Fixture();
    CatalogImage initial = fixture._catalog.captureImage(SCHEMAS);
    UUID configId = UUID.randomUUID();
    Transaction create = fixture.transaction("main", 1,
        Mutation.createClusterConfig(fixture._history, configId, Map.of("key", "v")));
    fixture.executeAt(create, 5);
    fixture.executeAt(create, 7);
    fixture.executeAt(fixture.transaction("failure", 1,
        Mutation.updateClusterConfig(new ObjectVersion(fixture._history, configId, 99), Map.of("key", "v"))), 10);
    fixture.executeAt(new Acknowledge(fixture._history, "client", 1, "main", 1), 11);
    fixture.executeAt(fixture.transaction("schema", 1, fixture.schema("schema")), 14);

    ChangePage first = fixture._catalog.changes(SCHEMAS, initial.cursor(), 2, 128,
        MetadataProtocol.MAX_CHANGE_BYTES);
    assertEquals(first.transactions().stream().map(ChangeTransaction::index).toList(), List.of(5L, 7L));
    assertTrue(first.transactions().stream().allMatch(transaction -> transaction.records().isEmpty()));
    assertTrue(first.hasMore());
    assertChain(first);
    ChangePage second = fixture.changes(SCHEMAS, first.next());
    assertEquals(second.transactions().stream().map(ChangeTransaction::index).toList(), List.of(10L, 11L, 14L));
    assertEquals(second.transactions().getLast().records().size(), 1);
    assertFalse(second.hasMore());
    assertChain(second);
    Result revision = MetadataProtocol.decodeResult(fixture._catalog.query(
        MetadataProtocol.encode(new GetRevision(fixture._history))));
    assertEquals(revision.revision().index(), second.next().index());
    assertEquals(fixture._catalog.changes(SCHEMAS,
        new FeedCursor(fixture._history, 6, SCHEMAS.scopeId()), 128, 128, MetadataProtocol.MAX_CHANGE_BYTES).status(),
        Status.INVALID_REQUEST);
  }

  @Test
  public void testCompleteTransactionNeverSplitsForRecordOrByteBudget()
      throws Exception {
    Fixture fixture = new Fixture();
    FeedCursor cursor = fixture._catalog.captureImage(ALL).cursor();
    fixture.execute(fixture.transaction("large", 1,
        Mutation.createClusterConfig(fixture._history, UUID.randomUUID(), Map.of("payload", "x".repeat(4096))),
        fixture.schema("schema")));
    ChangePage recordBound = fixture._catalog.changes(ALL, cursor, 128, 1, MetadataProtocol.MAX_CHANGE_BYTES);
    assertEquals(recordBound.status(), Status.RESPONSE_TOO_LARGE);
    assertTrue(recordBound.transactions().isEmpty());
    assertEquals(recordBound.next(), cursor);
    assertFalse(recordBound.hasMore());
    ChangePage byteBound = fixture._catalog.changes(ALL, cursor, 128, 128, MetadataProtocol.MIN_CHANGE_BYTES);
    assertEquals(byteBound.status(), Status.RESPONSE_TOO_LARGE);
    ChangePage complete = fixture.changes(ALL, cursor);
    assertEquals(complete.status(), Status.OK);
    assertEquals(complete.transactions().size(), 1);
    assertEquals(complete.transactions().getFirst().records().size(), 2);
    int exactBytes = MetadataProtocol.encodeValue(complete).length;
    assertEquals(fixture._catalog.changes(ALL, cursor, 128, 128, exactBytes), complete);
    assertEquals(fixture._catalog.changes(ALL, cursor, 128, 128, exactBytes - 1).status(),
        Status.RESPONSE_TOO_LARGE);
    FeedScope config = new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "authorized-catalog-v1");
    FeedCursor filteredCursor = new FeedCursor(fixture._history, cursor.index(), config.scopeId());
    ChangePage filtered = fixture.changes(config, filteredCursor);
    assertEquals(filtered.transactions().getFirst().records().size(), 1);
    int filteredBytes = MetadataProtocol.encodeValue(filtered).length;
    assertEquals(fixture._catalog.changes(config, filteredCursor, 128, 128, filteredBytes), filtered);
    assertEquals(fixture._catalog.changes(config, filteredCursor, 128, 128, filteredBytes - 1).status(),
        Status.RESPONSE_TOO_LARGE);
    MetadataCatalog recovered = new MetadataCatalog(fixture._cluster, fixture._history);
    recovered.restoreSnapshot(fixture._catalog.captureSnapshot());
    assertEquals(recovered.changes(config, filteredCursor, 128, 128, filteredBytes), filtered);
    assertEquals(recovered.changes(config, filteredCursor, 128, 128, filteredBytes - 1).status(),
        Status.RESPONSE_TOO_LARGE);
    assertThrows(ResponseTooLargeException.class, () -> fixture._catalog.captureImage(ALL, 1, 8192));
    assertThrows(ResponseTooLargeException.class, () -> fixture._catalog.captureImage(ALL, 128, 1024));
    assertEquals(fixture._catalog.captureImage(ALL, 128, 8192).records().size(), 2);
  }

  @Test
  public void testScopeHistoryAndCapturedCursorExpiry()
      throws Exception {
    Fixture fixture = new Fixture();
    FeedCursor initial = fixture._catalog.captureImage(ALL).cursor();
    FeedScope changedAccess = new FeedScope(ALL.kinds(), "authorized-catalog-v2");
    assertEquals(fixture.changes(changedAccess, initial).status(), Status.INVALID_REQUEST);
    assertEquals(fixture.changes(SCHEMAS, initial).status(), Status.INVALID_REQUEST);
    assertEquals(fixture.changes(ALL, new FeedCursor(UUID.randomUUID(), initial.index(), initial.scopeId())).status(),
        Status.HISTORY_MISMATCH);
    for (int i = 0; i <= MetadataCatalog.MAX_JOURNAL_TRANSACTIONS; i++) {
      fixture.execute(fixture.registration());
    }
    ChangePage expired = fixture.changes(ALL, initial);
    assertEquals(expired.status(), Status.CURSOR_EXPIRED);
    assertTrue(expired.retentionFloor() > initial.index());
    FeedCursor retained = new FeedCursor(fixture._history, expired.retentionFloor(), initial.scopeId());
    ChangePage available = fixture.changes(ALL, retained);
    assertEquals(available.status(), Status.OK);
    assertChain(available);
    MetadataCatalog recovered = new MetadataCatalog(fixture._cluster, fixture._history);
    recovered.restoreSnapshot(fixture._catalog.captureSnapshot());
    assertEquals(recovered.changes(ALL, initial, 128, 128, MetadataProtocol.MAX_CHANGE_BYTES), expired);
    assertEquals(recovered.changes(ALL, retained, 128, 128, MetadataProtocol.MAX_CHANGE_BYTES), available);
  }

  @Test
  public void testByteRetentionRetiresWholeTransactions()
      throws Exception {
    Fixture fixture = new Fixture();
    FeedCursor initial = fixture._catalog.captureImage(ALL).cursor();
    UUID id = UUID.randomUUID();
    String payload = "x".repeat(800_000);
    for (int sequence = 1; sequence <= 23; sequence++) {
      Mutation mutation = sequence == 1
          ? Mutation.createClusterConfig(fixture._history, id, Map.of("payload", payload))
          : Mutation.updateClusterConfig(new ObjectVersion(fixture._history, id, sequence - 1),
              Map.of("payload", payload));
      assertEquals(fixture.execute(fixture.transaction("main", sequence, mutation)).status(), Status.OK);
      fixture.execute(new Acknowledge(fixture._history, "client", 1, "main", sequence));
    }
    JsonNode snapshot = fixture._catalog.captureSnapshot();
    assertTrue(snapshot.path("journalFloor").asLong() > initial.index());
    assertTrue(snapshot.path("journal").size() < MetadataCatalog.MAX_JOURNAL_TRANSACTIONS);
    assertEquals(fixture.changes(ALL, initial).status(), Status.CURSOR_EXPIRED);
    long bytes = 0;
    for (JsonNode transaction : snapshot.path("journal")) {
      bytes += MetadataProtocol.encodeValue(transaction).length;
    }
    assertTrue(bytes <= MetadataCatalog.MAX_JOURNAL_BYTES);
    MetadataCatalog recovered = new MetadataCatalog(fixture._cluster, fixture._history);
    recovered.restoreSnapshot(snapshot);
    FeedCursor retained = new FeedCursor(fixture._history, snapshot.path("journalFloor").asLong(), ALL.scopeId());
    ChangePage page = recovered.changes(ALL, retained, 128, 128, MetadataProtocol.MAX_CHANGE_BYTES);
    assertChain(page);
    assertTrue(MetadataProtocol.encodeValue(page).length <= MetadataProtocol.MAX_CHANGE_BYTES);
  }

  @Test
  public void testLegacySnapshotMigrationDoesNotInventHistory()
      throws Exception {
    Fixture fixture = new Fixture();
    fixture.execute(fixture.transaction("schema", 1, fixture.schema("schema")));
    JsonNode current = fixture._catalog.captureSnapshot();
    for (int version = 2; version <= 7; version++) {
      ObjectNode prior = current.deepCopy();
      prior.put("formatVersion", version);
      if (version == 2) {
        prior.remove("membership");
      } else {
        ((ObjectNode) prior.at("/membership/control")).remove(
            List.of("minimumTickDelayMillis", "maxConfiguredPermitLifetimeMillis"));
      }
      MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
      restored.restoreSnapshot(prior);
      assertEquals(restored.captureSnapshot().path("formatVersion").asInt(), 10);
      assertEquals(restored.captureImage(ALL), fixture._catalog.captureImage(ALL));
      assertEquals(restored.captureSnapshot().path("journal"), current.path("journal"));
      assertEquals(restored.captureSnapshot().path("journalFloor"), current.path("journalFloor"));
    }
    ObjectNode legacy = current.deepCopy();
    legacy.put("formatVersion", 1);
    legacy.remove(List.of("journalPolicyVersion", "journalFloor", "journal", "membership"));
    MetadataCatalog recovered = new MetadataCatalog(fixture._cluster, fixture._history);
    recovered.restoreSnapshot(legacy);
    assertEquals(recovered.captureSnapshot().path("formatVersion").asInt(), 10);
    assertEquals(recovered.captureImage(ALL), fixture._catalog.captureImage(ALL));
    FeedCursor before = new FeedCursor(fixture._history, 1, ALL.scopeId());
    assertEquals(recovered.changes(ALL, before, 128, 128, MetadataProtocol.MAX_CHANGE_BYTES).status(),
        Status.CURSOR_EXPIRED);
    FeedCursor checkpoint = recovered.captureImage(ALL).cursor();
    Command next = fixture.transaction("next", 1, fixture.schema("another"));
    fixture.execute(next);
    recovered.execute(MetadataProtocol.encode(next), fixture._index);
    ChangePage replayed = recovered.changes(ALL, checkpoint, 128, 128, MetadataProtocol.MAX_CHANGE_BYTES);
    assertEquals(replayed.transactions(), fixture.changes(ALL, checkpoint).transactions());
    assertEquals(replayed.from(), checkpoint);
    assertEquals(replayed.next().index(), fixture._index);
  }

  @Test
  public void testSnapshotRejectsJournalGapOrStateMismatchAtomically()
      throws Exception {
    Fixture fixture = new Fixture();
    fixture.execute(fixture.transaction("schema", 1, fixture.schema("schema")));
    JsonNode original = fixture._catalog.captureSnapshot();
    ObjectNode gap = original.deepCopy();
    ((ObjectNode) gap.at("/journal/1")).put("previousIndex", 0);
    ObjectNode missingRecord = original.deepCopy();
    ((ArrayNode) missingRecord.at("/journal/1/records")).removeAll();
    ObjectNode differentValue = original.deepCopy();
    ((ObjectNode) differentValue.at("/journal/1/records/0/value")).put("description", "changed");
    ObjectNode wrongModification = original.deepCopy();
    ((ObjectNode) wrongModification.at("/journal/1/records/0")).put("lastModifiedIndex", 1);
    ObjectNode unknownPolicy = original.deepCopy();
    unknownPolicy.put("journalPolicyVersion", 2);
    for (ObjectNode invalid : List.of(gap, missingRecord, differentValue, wrongModification, unknownPolicy)) {
      assertThrows(IOException.class, () -> fixture._catalog.restoreSnapshot(invalid));
      assertEquals(fixture._catalog.captureSnapshot(), original);
    }
  }

  private static void assertChain(ChangePage page) {
    assertEquals(page.status(), Status.OK);
    long index = page.from().index();
    for (ChangeTransaction transaction : page.transactions()) {
      assertEquals(transaction.previousIndex(), index);
      assertTrue(transaction.index() > index);
      transaction.records().forEach(record -> assertEquals(record.lastModifiedIndex(), transaction.index()));
      index = transaction.index();
    }
    assertEquals(page.next().index(), index);
  }

  private static final class Fixture {
    private final UUID _cluster = UUID.randomUUID();
    private final UUID _history = UUID.randomUUID();
    private final UUID _incarnation = UUID.randomUUID();
    private final MetadataCatalog _catalog = new MetadataCatalog(_cluster, _history);
    private long _index;

    private Fixture()
        throws IOException {
      execute(registration());
    }

    private RegisterClient registration() {
      return new RegisterClient(_history, "client", _incarnation, 0);
    }

    private Result execute(Command command)
        throws IOException {
      return executeAt(command, _index + 1);
    }

    private Result executeAt(Command command, long index)
        throws IOException {
      _index = index;
      return MetadataProtocol.decodeResult(_catalog.execute(MetadataProtocol.encode(command), index));
    }

    private Transaction transaction(String stream, long sequence, Mutation... mutations) {
      return new Transaction(_history, "client", 1, stream, sequence, List.of(mutations));
    }

    private Mutation schema(String name) {
      Schema schema = new Schema.SchemaBuilder().setSchemaName(name)
          .addSingleValueDimension("id", DataType.STRING).build();
      return Mutation.createSchema(_history, UUID.randomUUID(), schema);
    }

    private ChangePage changes(FeedScope scope, FeedCursor cursor)
        throws IOException {
      return _catalog.changes(scope, cursor, 128, 128, MetadataProtocol.MAX_CHANGE_BYTES);
    }
  }
}
