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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Action;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangeTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ConfigTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetInstallationWork;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetOutcome;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRevision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetStreamTip;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.InstallationSessionCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.InstallationSessionQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.NativeInstallationCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.NativeOperationCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.NativeTaskCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OfflineSegmentCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OperationCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OwnershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Query;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ReadBarrier;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ResponseTooLargeException;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RetireClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.SequencedCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.StreamTipResult;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.TaskCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.TaskSessionCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.InstallationSnapshot;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.WorkResult;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.AssignedOfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.State;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Deterministic, synchronized metadata state owned by a single Ratis state machine. Catalog records and retry
/// outcomes commit together. Only affected records are staged; readers and snapshot capture share the same lock.
/// A query is coherent but becomes linearizable only when the caller supplies the required consensus read barrier.
/// Transports authenticate callers; owner-dependent commands validate committed authority here. Full table lifecycle
/// and participant execution remain separate integrations.
/// Tombstones and client epoch fences are intentionally retained without unsafe GC.
public final class MetadataCatalog {
  private static final int MAX_IDENTIFIER_LENGTH = 1024;
  private static final int MAX_RESULT_BYTES = 2 * MetadataProtocol.MAX_COMMAND_BYTES;
  private static final int SNAPSHOT_FORMAT_VERSION = 9;
  private static final int JOURNAL_POLICY_VERSION = 1;
  static final int MAX_JOURNAL_TRANSACTIONS = 1024;
  static final int MAX_JOURNAL_BYTES = 16 * MetadataProtocol.MAX_COMMAND_BYTES;

  private final UUID _clusterId;
  private final UUID _historyId;
  private long _lastAppliedIndex;
  // Retained records, including tombstones, reconstruct this fence on restore. Membership-only progress is excluded.
  private long _catalogMutationIndex;
  private Map<UUID, CatalogRecord> _records = new HashMap<>();
  private Map<UUID, Integer> _recordBytes = new HashMap<>();
  private Map<RecordKey, UUID> _names = new HashMap<>();
  private Map<String, ClientState> _clients = new HashMap<>();
  private ArrayDeque<JournalEntry> _journal = new ArrayDeque<>();
  private long _journalFloor;
  private long _journalBytes;
  private MembershipState _membership;

  public MetadataCatalog(UUID clusterId, UUID historyId) {
    _clusterId = Objects.requireNonNull(clusterId, "clusterId");
    _historyId = Objects.requireNonNull(historyId, "historyId");
    _membership = new MembershipState(historyId);
  }

  /// Applies exactly one committed command in increasing log-index order. Semantic rejection advances the applied
  /// revision but cannot partially mutate the catalog. Unexpected failures propagate rather than hiding corruption.
  public synchronized byte[] execute(byte[] bytes, long committedIndex)
      throws IOException {
    return execute(bytes, 0, committedIndex);
  }

  /// Membership commands require the actual committed log term supplied by the state machine, never a caller hint.
  public synchronized byte[] execute(byte[] bytes, long actualRaftTerm, long committedIndex)
      throws IOException {
    if (committedIndex <= _lastAppliedIndex) {
      throw new IOException("Committed metadata indexes must increase");
    }
    // Admission rejects unsupported wire commands before append. If one appears during committed replay, stopping
    // recovery is mandatory: treating a newer command as an ordinary rejection could diverge from another replica.
    Command command = MetadataProtocol.decodeCommand(bytes);
    if (command instanceof ReadBarrier read) {
      // Serialize the bounded read result before publishing progress. GetOutcome deliberately retains the original
      // operation result/revision, while current record, revision and membership reads use this barrier's index.
      byte[] response = query(read.request(), committedIndex);
      appendJournal(new ChangeTransaction(_lastAppliedIndex, committedIndex, List.of()));
      _lastAppliedIndex = committedIndex;
      return response;
    }
    if (command instanceof MembershipCommand membership) {
      return executeMembership(membership, actualRaftTerm, committedIndex);
    }
    Result result;
    try {
      require(_historyId.equals(command.historyId()), Status.HISTORY_MISMATCH, "Metadata history differs");
      identifier(command.clientId());
      result = switch (command) {
        case RegisterClient registration -> register(registration, committedIndex);
        case RetireClient retirement -> retire(retirement, committedIndex);
        case InstallationSessionCommand session -> executeInstallationSession(session, actualRaftTerm, committedIndex);
        case TaskSessionCommand session -> executeTaskSession(session, actualRaftTerm, committedIndex);
        case NativeTaskCommand ignored -> throw new Rejection(Status.INVALID_REQUEST,
            "Worker task commands require an authenticated session envelope");
        case NativeInstallationCommand ignored -> throw new Rejection(Status.INVALID_REQUEST,
            "Installation reports require an authenticated session envelope");
        case SequencedCommand sequenced -> transact(sequenced, actualRaftTerm, committedIndex);
        case Acknowledge acknowledgement -> acknowledge(acknowledgement, committedIndex);
        case MembershipCommand ignored -> throw new IllegalStateException("Membership dispatch failed");
        case ReadBarrier ignored -> throw new IllegalStateException("Read barrier dispatch failed");
      };
    } catch (Rejection e) {
      result = result(e._status, e.getMessage(), committedIndex);
    } catch (CoordinationRejection e) {
      result = result(e.status(), e.getMessage(), committedIndex);
    }
    List<CatalogRecord> changes = List.of();
    if (result.status() == Status.OK && result.revision().index() == committedIndex && !result.versions().isEmpty()) {
      changes = result.versions().stream().map(version -> _records.get(version.objectId())).toList();
    }
    appendJournal(new ChangeTransaction(_lastAppliedIndex, committedIndex, changes));
    _lastAppliedIndex = committedIndex;
    return encode(result);
  }

  private Result executeInstallationSession(InstallationSessionCommand session, long term, long index)
      throws IOException {
    OfflineInstallationCatalog.validateSession(session.server(), coordinationView());
    require(session.request() != null
        && OfflineInstallationProtocol.clientId(session.server()).equals(session.request().clientId()),
        Status.FENCED, "Installation request belongs to another participant session");
    return switch (session.request()) {
      case RegisterClient registration -> register(registration, index);
      case Acknowledge acknowledgement -> acknowledge(acknowledgement, index);
      case NativeInstallationCommand installation -> {
        require(installation.request() != null && installation.request().installed() != null
            && session.server().equals(installation.request().installed().server()), Status.FENCED,
            "Installation report belongs to another participant session");
        yield transact(installation, term, index);
      }
      default -> throw new Rejection(Status.INVALID_REQUEST, "Unsupported participant installation command");
    };
  }

  private Result executeTaskSession(TaskSessionCommand session, long term, long index) throws IOException {
    TaskCatalog.validateSession(session.minion(), coordinationView());
    require(session.request() != null && TaskProtocol.clientId(session.minion()).equals(session.request().clientId()),
        Status.FENCED, "Task request belongs to another participant session");
    return switch (session.request()) {
      case RegisterClient registration -> {
        require(session.minion().incarnation().equals(registration.incarnation()), Status.FENCED,
            "Task request client incarnation differs from its participant");
        yield register(registration, index);
      }
      case Acknowledge acknowledgement -> acknowledge(acknowledgement, index);
      case NativeTaskCommand task -> {
        require(session.minion().equals(task.minion()), Status.FENCED,
            "Task report belongs to another participant session");
        yield transact(task, term, index);
      }
      default -> throw new Rejection(Status.INVALID_REQUEST, "Unsupported participant task command");
    };
  }

  /// Returns one record or a retained outcome. General listing and subscription are deliberately a separate,
  /// bounded snapshot/change-feed protocol; this method does not return an unbounded cluster image to a client.
  public synchronized byte[] query(byte[] bytes)
      throws IOException {
    Query query;
    try {
      query = MetadataProtocol.decodeQuery(bytes);
    } catch (IOException | IllegalArgumentException e) {
      return encode(result(Status.INVALID_REQUEST, "Malformed or unsupported metadata query", _lastAppliedIndex));
    }
    return query(query, _lastAppliedIndex);
  }

  private byte[] query(Query query, long readIndex)
      throws IOException {
    if (query == null) {
      return encode(result(Status.INVALID_REQUEST, "Metadata query is required", readIndex));
    }
    if (query instanceof MembershipQuery membership) {
      try {
        return encodeMembership(_historyId.equals(query.historyId())
            ? _membership.read(membership.request(), readIndex)
            : _membership.error(Status.HISTORY_MISMATCH, "Metadata history differs", readIndex));
      } catch (ResponseTooLargeException e) {
        return encodeMembership(_membership.error(Status.RESPONSE_TOO_LARGE, e.getMessage(), readIndex));
      }
    }
    if (query instanceof GetInstallationWork work) {
      return installationWork(work, readIndex);
    }
    if (query instanceof GetStreamTip tip) {
      return streamTip(tip, readIndex);
    }
    try {
      require(_historyId.equals(query.historyId()), Status.HISTORY_MISMATCH, "Metadata history differs");
      return encode(switch (query) {
        case GetRecord read -> readRecord(read, readIndex);
        case GetOutcome lookup -> readOutcome(lookup);
        case GetRevision ignored -> result(Status.OK, "", readIndex);
        case InstallationSessionQuery session -> {
          OfflineInstallationCatalog.validateSession(session.server(), coordinationView());
          require(session.request() != null
              && OfflineInstallationProtocol.clientId(session.server()).equals(session.request().clientId()),
              Status.FENCED, "Installation outcome belongs to another participant session");
          yield readOutcome(session.request());
        }
        case GetInstallationWork ignored -> throw new IllegalStateException("Installation work dispatch failed");
        case GetStreamTip ignored -> throw new IllegalStateException("Stream tip dispatch failed");
        case MembershipQuery ignored -> throw new IllegalStateException("Membership dispatch failed");
      });
    } catch (Rejection e) {
      return encode(result(e._status, e.getMessage(), readIndex));
    } catch (CoordinationRejection e) {
      return encode(result(e.status(), e.getMessage(), readIndex));
    } catch (ResponseTooLargeException e) {
      return encode(result(Status.RESPONSE_TOO_LARGE, e.getMessage(), readIndex));
    }
  }

  private byte[] streamTip(GetStreamTip query, long index)
      throws IOException {
    StreamTipResult result;
    try {
      require(_historyId.equals(query.historyId()), Status.HISTORY_MISMATCH, "Metadata history differs");
      ClientState client = currentClient(query.clientId(), query.clientEpoch());
      identifier(query.streamId());
      StreamState stream = client.streams().get(query.streamId());
      Outcome outcome = stream == null ? null : stream.outcome();
      result = new StreamTipResult(Status.OK, revision(index), "", client.epoch(),
          stream == null ? 0 : stream.sequence(), stream == null ? 0 : stream.floor(),
          outcome == null ? null : outcome.digest(), outcome == null ? null : outcome.result());
    } catch (Rejection e) {
      result = new StreamTipResult(e._status, revision(index), e.getMessage(), 0, 0, 0, null, null);
    }
    byte[] encoded = MetadataProtocol.encodeValue(result);
    return encoded.length <= MAX_RESULT_BYTES ? encoded : MetadataProtocol.encodeValue(
        new StreamTipResult(Status.RESPONSE_TOO_LARGE, revision(index), "Stream tip exceeds the supported size",
            0, 0, 0, null, null));
  }

  private byte[] installationWork(GetInstallationWork query, long index)
      throws IOException {
    WorkResult result;
    try {
      require(_historyId.equals(query.historyId()), Status.HISTORY_MISMATCH, "Metadata history differs");
      result = new WorkResult(Status.OK, revision(index), "",
          OfflineInstallationCatalog.work(query.operationId(), query.server(), coordinationView()));
    } catch (Rejection e) {
      result = new WorkResult(e._status, revision(index), e.getMessage(), null);
    } catch (CoordinationRejection e) {
      result = new WorkResult(e.status(), revision(index), e.getMessage(), null);
    }
    byte[] encoded = MetadataProtocol.encodeValue(result);
    return encoded.length <= MAX_RESULT_BYTES
        ? encoded
        : MetadataProtocol.encodeValue(new WorkResult(Status.RESPONSE_TOO_LARGE, revision(index),
            "Installation work exceeds the supported size", null));
  }

  /// Returns immutable committed control state for the local leader clock. This is not a linearizable remote read.
  public synchronized MembershipProtocol.ControlState membershipControl() {
    return _membership.control();
  }

  /// Captures one complete own-session projection after the caller's consensus read barrier. Readiness is not
  /// required for bootstrap. The revision describes this whole projection, not arbitrary operation lookup progress.
  /// Fixed bounds deliberately reject a large image; pagination must not be simulated by dropping records here.
  public synchronized InstallationSnapshot installationSnapshot(ParticipantToken server, int maxRecords, int maxBytes)
      throws IOException {
    try {
      require(maxRecords > 0 && maxRecords <= OfflineInstallationProtocol.MAX_SNAPSHOT_RECORDS
          && maxBytes >= OfflineInstallationProtocol.MIN_SNAPSHOT_BYTES
          && maxBytes <= OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES, Status.INVALID_REQUEST,
          "Invalid installation snapshot bounds");
      OperationCatalog.View view = coordinationView();
      OfflineInstallationCatalog.validateProjectionSession(server, view);
      InstallationSnapshot header = new InstallationSnapshot(Status.OK, revision(_lastAppliedIndex), "",
          view.participant(server.participantId()), List.of(), List.of(), view.control());
      InstallationProjection projection = new InstallationProjection(header, maxRecords, maxBytes);
      for (UUID objectId : _names.values()) {
        CatalogRecord record = _records.get(objectId);
        if (record.kind() == RecordKind.CLUSTER_CONFIG) {
          projection.add(record);
        } else if (record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT) {
          Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
          if (assignment.server().equals(server)) {
            projection.add(record);
            projection.addReferenced(assignment.segmentObjectId(), RecordKind.OFFLINE_SEGMENT_CONTENT);
            projection.addReferenced(assignment.compatibility().table().objectId(), RecordKind.TABLE_CONFIG);
            projection.addReferenced(assignment.compatibility().schema().objectId(), RecordKind.SCHEMA);
          }
        } else if (record.kind() == RecordKind.CONTROLLER_OPERATION) {
          OperationState operation = OperationProtocol.decodeOperation(record.value());
          if ((operation.status() == State.ACCEPTED || operation.status() == State.RUNNING)
              && operation.input() instanceof AssignedOfflineSegmentReload assigned
              && assigned.reload().targets().stream().anyMatch(target -> target.server().equals(server))) {
            projection.addOperation(operation.operationId());
          }
        }
      }
      return projection.result();
    } catch (Rejection e) {
      return installationSnapshotError(e._status, e.getMessage());
    } catch (CoordinationRejection e) {
      return installationSnapshotError(e.status(), e.getMessage());
    }
  }

  private InstallationSnapshot installationSnapshotError(Status status, String detail) {
    return new InstallationSnapshot(status, revision(_lastAppliedIndex), detail, null, List.of(), List.of(), null);
  }

  /// Uses maintained canonical record sizes; capturing an image never serializes large configuration values under
  /// the apply lock. The transport checks its own encoding against the same budget before emitting any response.
  private final class InstallationProjection {
    private final InstallationSnapshot _header;
    private final int _maxRecords;
    private final int _maxBytes;
    private final Map<UUID, CatalogRecord> _selected = new HashMap<>();
    private final List<UUID> _operations = new ArrayList<>();
    private long _bytes;

    private InstallationProjection(InstallationSnapshot header, int maxRecords, int maxBytes)
        throws IOException {
      _header = header;
      _maxRecords = maxRecords;
      _maxBytes = maxBytes;
      _bytes = MetadataProtocol.encodeValue(header).length;
      checkBounds();
    }

    private void add(CatalogRecord record) {
      if (_selected.containsKey(record.objectId())) {
        return;
      }
      _bytes += _recordBytes.get(record.objectId()) + (_selected.isEmpty() ? 0 : 1);
      require(_selected.size() + _operations.size() < _maxRecords, Status.RESPONSE_TOO_LARGE,
          "Installation snapshot exceeds its record bound");
      checkBounds();
      _selected.put(record.objectId(), record);
    }

    private void addReferenced(UUID objectId, RecordKind kind) {
      CatalogRecord record = _records.get(objectId);
      require(record != null && record.kind() == kind
          && (!record.deleted() || kind == RecordKind.TABLE_CONFIG || kind == RecordKind.SCHEMA), Status.NOT_READY,
          "Installation assignment references unavailable current metadata");
      // Retained assignments may outlive their table/schema. Deliver the exact tombstone so consumers can fence
      // that target without losing unrelated metadata or borrowing a recreated name's new object incarnation.
      add(record);
    }

    private void addOperation(UUID operationId) {
      _bytes += 38 + (_operations.isEmpty() ? 0 : 1);
      require(_selected.size() + _operations.size() < _maxRecords, Status.RESPONSE_TOO_LARGE,
          "Installation snapshot exceeds its record bound");
      checkBounds();
      _operations.add(operationId);
    }

    private void checkBounds() {
      require(_bytes <= _maxBytes, Status.RESPONSE_TOO_LARGE, "Installation snapshot exceeds its byte bound");
    }

    private InstallationSnapshot result() {
      List<CatalogRecord> records = new ArrayList<>(_selected.values());
      records.sort(Comparator.comparing(CatalogRecord::kind).thenComparing(CatalogRecord::name)
          .thenComparing(CatalogRecord::objectId));
      _operations.sort(Comparator.naturalOrder());
      return new InstallationSnapshot(Status.OK, _header.revision(), "", _header.participant(), records, _operations,
          _header.control());
    }
  }

  private OperationCatalog.View coordinationView() {
    return coordinationView(_records, _names, _membership);
  }

  private OperationCatalog.View coordinationView(Map<UUID, CatalogRecord> records, Map<RecordKey, UUID> names,
      MembershipState membership) {
    return coordinationView(records, names, membership, Map.of());
  }

  private OperationCatalog.View coordinationView(Map<UUID, CatalogRecord> records, Map<RecordKey, UUID> names,
      MembershipState membership, Map<ObjectVersion, CatalogRecord> retainedVersions) {
    return new OperationCatalog.View() {
      @Override
      public CatalogRecord record(RecordKind kind, String name) {
        return records.get(names.get(new RecordKey(kind, name)));
      }

      @Override
      public CatalogRecord record(UUID objectId) {
        return records.get(objectId);
      }

      @Override
      public CatalogRecord retainedRecord(ObjectVersion version) {
        CatalogRecord current = records.get(version.objectId());
        return current != null && current.version() == version.version() ? current : retainedVersions.get(version);
      }

      @Override
      public MembershipProtocol.ParticipantState participant(String id) {
        return membership.read(new MembershipProtocol.GetParticipant(id), 0).participant();
      }

      @Override
      public MembershipProtocol.ControlState control() {
        return membership.control();
      }

      @Override
      public UUID historyId() {
        return _historyId;
      }

      @Override
      public CatalogRecord stage(Mutation mutation, long index) {
        return MetadataCatalog.this.stage(mutation, index);
      }
    };
  }

  private byte[] executeMembership(MembershipCommand command, long actualTerm, long index)
      throws IOException {
    MembershipState.Plan plan = null;
    MembershipProtocol.Response response;
    try {
      require(_historyId.equals(command.historyId()), Status.HISTORY_MISMATCH, "Metadata history differs");
      identifier(command.clientId());
      plan = _membership.stage(command.request(), actualTerm, index);
      response = plan.response();
    } catch (Rejection e) {
      response = _membership.error(e._status, e.getMessage(), index);
    }
    List<CatalogRecord> changed = new ArrayList<>();
    try {
      if (plan != null) {
        if (!plan.control().equals(_membership.control())) {
          changed.add(membershipRecord(RecordKind.MEMBERSHIP_CONTROL, MembershipProtocol.CONTROL_NAME,
              MetadataProtocol.toTree(plan.control()), index));
        }
        for (String id : plan.members().keySet().stream().sorted().toList()) {
          changed.add(membershipRecord(RecordKind.PARTICIPANT, id,
              MetadataProtocol.toTree(plan.members().get(id).state()), index));
        }
      }
    } catch (Rejection e) {
      plan = null;
      changed.clear();
      response = _membership.error(e._status, e.getMessage(), index);
    }
    byte[] encoded = encodeMembership(response);
    List<Integer> sizes = new ArrayList<>();
    for (CatalogRecord record : changed) {
      sizes.add(MetadataProtocol.encodeValue(record).length);
    }
    if (plan != null) {
      for (int i = 0; i < changed.size(); i++) {
        CatalogRecord record = changed.get(i);
        _records.put(record.objectId(), record);
        _recordBytes.put(record.objectId(), sizes.get(i));
        _names.put(new RecordKey(record.kind(), record.name()), record.objectId());
      }
      _membership.commit(plan);
    }
    appendJournal(new ChangeTransaction(_lastAppliedIndex, index, changed));
    _lastAppliedIndex = index;
    return encoded;
  }

  private CatalogRecord membershipRecord(RecordKind kind, String name, JsonNode value, long index) {
    UUID id = membershipObjectId(kind, name);
    CatalogRecord previous = _records.get(id);
    if (previous != null && (previous.kind() != kind || !previous.name().equals(name)
        || previous.version() == Long.MAX_VALUE)) {
      throw new Rejection(Status.CONFLICT, "Membership record identity collision or version exhausted");
    }
    return new CatalogRecord(kind, name, id, previous == null ? 1 : previous.version() + 1,
        MetadataProtocol.FORMAT_VERSION, index, false, value);
  }

  private UUID membershipObjectId(RecordKind kind, String name) {
    return UUID.nameUUIDFromBytes((_historyId + "\0" + kind + "\0" + name).getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] encodeMembership(MembershipProtocol.Response response)
      throws IOException {
    byte[] bytes = MetadataProtocol.encodeValue(response);
    if (bytes.length > MAX_RESULT_BYTES) {
      throw new ResponseTooLargeException("Membership result exceeds the supported size");
    }
    return bytes;
  }

  /// Returns the local applied catalog revision. A caller requiring a linearizable image first obtains a consensus
  /// read barrier and waits for this local revision, then captures the image or change page under this catalog's lock.
  public synchronized Revision currentRevision() {
    return revision(_lastAppliedIndex);
  }

  /// Captures immutable live-record values and their cursor together. Fixed journal bounds may expire this cursor
  /// during paging; the transport must check the original cursor on every page/final handoff and require resnapshot
  /// on CURSOR_EXPIRED. Local snapshot handles never pin unbounded consensus history or alter replicated retention.
  public synchronized CatalogImage captureImage(FeedScope scope)
      throws IOException {
    return captureImage(scope, MetadataProtocol.MAX_IMAGE_RECORDS, MetadataProtocol.MAX_IMAGE_BYTES);
  }

  /// The transport reserves this bounded image budget before capture. Larger caller limits do not exceed the engine
  /// caps; smaller limits are enforced before collecting the complete image or encoding it in the transport.
  public synchronized CatalogImage captureImage(FeedScope scope, int maxRecords, int maxBytes)
      throws IOException {
    Objects.requireNonNull(scope, "scope");
    if (maxRecords <= 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("Metadata image limits must be positive");
    }
    int recordLimit = Math.min(maxRecords, MetadataProtocol.MAX_IMAGE_RECORDS);
    int byteLimit = Math.min(maxBytes, MetadataProtocol.MAX_IMAGE_BYTES);
    FeedCursor cursor = new FeedCursor(_historyId, _lastAppliedIndex, scope.scopeId());
    List<CatalogRecord> records = new ArrayList<>();
    long bytes = MetadataProtocol.encodeValue(new CatalogImage(cursor, List.of())).length;
    if (bytes > byteLimit) {
      throw new ResponseTooLargeException("Metadata image header exceeds the reserved export budget");
    }
    for (UUID objectId : _names.values()) {
      CatalogRecord record = _records.get(objectId);
      if (!scope.kinds().contains(record.kind())) {
        continue;
      }
      bytes += _recordBytes.get(record.objectId()) + (records.isEmpty() ? 0 : 1);
      if (bytes > byteLimit || records.size() == recordLimit) {
        throw new ResponseTooLargeException("Metadata image exceeds the reserved record or byte export budget");
      }
      records.add(record);
    }
    records.sort(Comparator.comparing(CatalogRecord::kind).thenComparing(CatalogRecord::name));
    return new CatalogImage(cursor, records);
  }

  /// Atomically reads the recoverable journal floor and whole filtered transactions. Progress events retain the
  /// index chain even when the selected kinds did not change. Bounds cover this page's canonical protocol encoding;
  /// transports must also apply their own wire-message bounds. An expired/invalid cursor never silently skips work.
  public synchronized ChangePage changes(FeedScope scope, FeedCursor cursor, int maxTransactions, int maxRecords,
      int maxBytes)
      throws IOException {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(cursor, "cursor");
    if (!_historyId.equals(cursor.historyId())) {
      return changeError(Status.HISTORY_MISMATCH, cursor);
    }
    if (!scope.scopeId().equals(cursor.scopeId()) || cursor.index() < 0 || cursor.index() > _lastAppliedIndex
        || maxTransactions <= 0 || maxTransactions > MetadataProtocol.MAX_CHANGE_TRANSACTIONS
        || maxRecords <= 0 || maxRecords > MetadataProtocol.MAX_CHANGE_RECORDS
        || maxBytes < MetadataProtocol.MIN_CHANGE_BYTES || maxBytes > MetadataProtocol.MAX_CHANGE_BYTES) {
      return changeError(Status.INVALID_REQUEST, cursor);
    }
    if (cursor.index() < _journalFloor) {
      return changeError(Status.CURSOR_EXPIRED, cursor);
    }
    List<ChangeTransaction> selected = new ArrayList<>();
    FeedCursor next = cursor;
    int records = 0;
    long transactionBytes = 0;
    for (JournalEntry entry : _journal) {
      ChangeTransaction transaction = entry.transaction();
      if (transaction.index() <= cursor.index()) {
        continue;
      }
      if (transaction.previousIndex() != next.index()) {
        return changeError(Status.INVALID_REQUEST, cursor);
      }
      List<CatalogRecord> filtered = transaction.records().stream()
          .filter(record -> scope.kinds().contains(record.kind())).toList();
      if (selected.size() == maxTransactions || records + filtered.size() > maxRecords) {
        if (selected.isEmpty()) {
          return changeError(Status.RESPONSE_TOO_LARGE, cursor);
        }
        break;
      }
      ChangeTransaction filteredTransaction = new ChangeTransaction(transaction.previousIndex(), transaction.index(),
          filtered);
      FeedCursor candidateNext = new FeedCursor(_historyId, transaction.index(), cursor.scopeId());
      ChangePage envelope = new ChangePage(Status.OK, cursor, candidateNext, _journalFloor, List.of(),
          transaction.index() < _lastAppliedIndex);
      int filteredBytes = entry.encodedBytes(scope.kinds());
      long candidateBytes = MetadataProtocol.encodeValue(envelope).length + transactionBytes + filteredBytes
          + selected.size();
      if (candidateBytes > maxBytes) {
        if (selected.isEmpty()) {
          return changeError(Status.RESPONSE_TOO_LARGE, cursor);
        }
        break;
      }
      selected.add(filteredTransaction);
      records += filtered.size();
      transactionBytes += filteredBytes;
      next = candidateNext;
    }
    return new ChangePage(Status.OK, cursor, next, _journalFloor, selected, next.index() < _lastAppliedIndex);
  }

  private ChangePage changeError(Status status, FeedCursor cursor) {
    return new ChangePage(status, cursor, cursor, _journalFloor, List.of(), false);
  }

  private void appendJournal(ChangeTransaction transaction)
      throws IOException {
    Map<RecordKind, Integer> recordBytesByKind = new EnumMap<>(RecordKind.class);
    for (CatalogRecord record : transaction.records()) {
      recordBytesByKind.merge(record.kind(), _recordBytes.get(record.objectId()) + 1, Integer::sum);
    }
    JournalEntry entry = journalEntry(transaction, recordBytesByKind);
    int bytes = entry.encodedBytes();
    if (bytes > MAX_JOURNAL_BYTES) {
      throw new IOException("Committed metadata transaction exceeds journal retention capacity");
    }
    _journal.addLast(entry);
    _journalBytes += bytes;
    while (_journal.size() > MAX_JOURNAL_TRANSACTIONS || _journalBytes > MAX_JOURNAL_BYTES) {
      JournalEntry retired = _journal.removeFirst();
      _journalFloor = retired.transaction().index();
      _journalBytes -= retired.encodedBytes();
    }
  }

  private static JournalEntry journalEntry(ChangeTransaction transaction, Map<RecordKind, Integer> recordBytesByKind)
      throws IOException {
    int envelopeBytes = MetadataProtocol.encodeValue(
        new ChangeTransaction(transaction.previousIndex(), transaction.index(), List.of())).length;
    return new JournalEntry(transaction, envelopeBytes, Map.copyOf(recordBytesByKind));
  }

  /// Captures detached snapshot data under the catalog lock. File encoding, checksums and persistence happen outside
  /// this lock in the Ratis snapshot implementation. A snapshot is not the public metadata query protocol.
  public JsonNode captureSnapshot() {
    return captureSnapshotData().toJson();
  }

  /// Captures only immutable record references and detached index/client-map structures. Potentially large JSON
  /// encoding happens through [SnapshotData#toJson()] after both the catalog and Ratis apply monitors are released.
  public synchronized SnapshotData captureSnapshotData() {
    List<ClientSnapshot> clients = new ArrayList<>();
    _clients.forEach((id, state) -> clients.add(new ClientSnapshot(id, state.epoch(), state.incarnation(),
        state.retired(), Map.copyOf(state.streams()))));
    return new SnapshotData(new Snapshot(SNAPSHOT_FORMAT_VERSION, _clusterId, _historyId,
        _lastAppliedIndex, new ArrayList<>(_records.values()), clients, JOURNAL_POLICY_VERSION, _journalFloor,
        _journal.stream().map(JournalEntry::transaction).toList(), _membership.snapshot()));
  }

  /// Opaque immutable snapshot captured at one catalog revision; encoding is independent of later writes.
  public static final class SnapshotData {
    private final Snapshot _snapshot;

    private SnapshotData(Snapshot snapshot) {
      _snapshot = snapshot;
    }

    public JsonNode toJson() {
      return MetadataProtocol.toTree(_snapshot);
    }
  }

  /// Restores only a complete, internally consistent snapshot of this exact cluster/history. No existing state is
  /// changed on failure. Destructive restore into a new history requires a separate explicit migration protocol.
  public synchronized void restoreSnapshot(JsonNode snapshot)
      throws IOException {
    Snapshot restored;
    try {
      JsonNode detached = snapshot.deepCopy();
      if (detached.path("formatVersion").asInt() == 1) {
        LegacySnapshot legacy = MetadataProtocol.fromTree(detached, LegacySnapshot.class);
        // v1 never retained overwritten changes. Certify only the restored image's index, never invent past events.
        restored = new Snapshot(SNAPSHOT_FORMAT_VERSION, legacy.clusterId(), legacy.historyId(),
            legacy.lastAppliedIndex(), legacy.records(), legacy.clients(), JOURNAL_POLICY_VERSION,
            legacy.lastAppliedIndex(), List.of(), new MembershipState(_historyId).snapshot());
      } else if (detached.path("formatVersion").asInt() == 2) {
        JournalSnapshot legacy = MetadataProtocol.fromTree(detached, JournalSnapshot.class);
        restored = new Snapshot(SNAPSHOT_FORMAT_VERSION, legacy.clusterId(), legacy.historyId(),
            legacy.lastAppliedIndex(), legacy.records(), legacy.clients(), legacy.journalPolicyVersion(),
            legacy.journalFloor(), legacy.journal(), new MembershipState(_historyId).snapshot());
      } else if (detached.path("formatVersion").asInt() >= 3 && detached.path("formatVersion").asInt() <= 7) {
        PreTimingSnapshot legacy = MetadataProtocol.fromTree(detached, PreTimingSnapshot.class);
        PreTimingMembership membership = legacy.membership();
        PreTimingControl control = membership == null ? null : membership.control();
        if (control == null || membership.members() == null || !membership.members().isEmpty()
            || control.controlEpoch() != null || control.policy() != null || control.round() != 0
            || control.clockBarrierIndex() != 0) {
          throw new IOException("Legacy membership snapshot requires an explicit maintenance-window migration "
              + "to a new history");
        }
        restored = new Snapshot(SNAPSHOT_FORMAT_VERSION, legacy.clusterId(), legacy.historyId(),
            legacy.lastAppliedIndex(), legacy.records(), legacy.clients(), legacy.journalPolicyVersion(),
            legacy.journalFloor(), legacy.journal(), new MembershipState(_historyId).snapshot());
      } else {
        Snapshot decoded = MetadataProtocol.fromTree(detached, Snapshot.class);
        restored = decoded.formatVersion() == 8
            ? new Snapshot(SNAPSHOT_FORMAT_VERSION, decoded.clusterId(), decoded.historyId(),
                decoded.lastAppliedIndex(), decoded.records(), decoded.clients(), decoded.journalPolicyVersion(),
                decoded.journalFloor(), decoded.journal(), decoded.membership())
            : decoded;
      }
      require(restored != null && restored.formatVersion() == SNAPSHOT_FORMAT_VERSION
          && restored.journalPolicyVersion() == JOURNAL_POLICY_VERSION,
          Status.INVALID_REQUEST, "Unsupported snapshot or journal-policy format");
      require(_clusterId.equals(restored.clusterId()) && _historyId.equals(restored.historyId()),
          Status.HISTORY_MISMATCH, "Snapshot cluster or history differs");
      require(restored.lastAppliedIndex() >= 0 && restored.records() != null && restored.clients() != null,
          Status.INVALID_REQUEST, "Invalid snapshot header");
      Map<UUID, CatalogRecord> records = new HashMap<>();
      long catalogMutationIndex = 0;
      Map<UUID, Integer> recordBytes = new HashMap<>();
      Map<RecordKey, UUID> names = new HashMap<>();
      for (CatalogRecord record : restored.records()) {
        require(record != null && record.kind() != null
            && detached.path("formatVersion").asInt() >= minimumSnapshotFormat(record),
            Status.INVALID_REQUEST, "Snapshot contains a record family newer than its format");
        validateRecord(record, restored.lastAppliedIndex());
        if (isCatalogMutation(record)) {
          catalogMutationIndex = Math.max(catalogMutationIndex, record.lastModifiedIndex());
        }
        require(records.putIfAbsent(record.objectId(), record) == null, Status.INVALID_REQUEST,
            "Duplicate object ID in snapshot");
        int encodedBytes = MetadataProtocol.encodeValue(record).length;
        require(encodedBytes <= MAX_RESULT_BYTES, Status.INVALID_REQUEST, "Snapshot record exceeds supported size");
        recordBytes.put(record.objectId(), encodedBytes);
        if (!record.deleted()) {
          require(names.putIfAbsent(new RecordKey(record.kind(), record.name()), record.objectId()) == null,
              Status.INVALID_REQUEST, "Duplicate live record name in snapshot");
        }
      }
      Map<String, ClientState> clients = new HashMap<>();
      for (ClientSnapshot client : restored.clients()) {
        identifier(client.clientId());
        require(client.epoch() > 0 && client.incarnation() != null && client.streams() != null,
            Status.INVALID_REQUEST, "Invalid client epoch");
        require(client.streams().size() <= MetadataProtocol.MAX_STREAMS_PER_CLIENT
            && (!client.retired() || client.streams().isEmpty()), Status.INVALID_REQUEST, "Invalid client streams");
        Map<String, StreamState> streams = new HashMap<>();
        for (Map.Entry<String, StreamState> entry : client.streams().entrySet()) {
          identifier(entry.getKey());
          validateStream(entry.getValue(), restored.lastAppliedIndex());
          streams.put(entry.getKey(), entry.getValue());
        }
        require(clients.putIfAbsent(client.clientId(), new ClientState(client.epoch(), client.incarnation(),
            client.retired(), streams)) == null, Status.INVALID_REQUEST, "Duplicate client in snapshot");
      }
      ArrayDeque<JournalEntry> journal = validateJournal(restored, records);
      Map<ObjectVersion, CatalogRecord> retainedVersions = new HashMap<>();
      long journalBytes = 0;
      for (JournalEntry entry : journal) {
        journalBytes += entry.encodedBytes();
        for (CatalogRecord record : entry.transaction().records()) {
          retainedVersions.put(new ObjectVersion(_historyId, record.objectId(), record.version()), record);
        }
      }
      MembershipState membership = MembershipState.restore(_historyId, restored.membership(),
          restored.lastAppliedIndex());
      validateMembershipMirror(membership.snapshot(), records, names);
      OperationCatalog.View view = coordinationView(records, names, membership, retainedVersions);
      for (CatalogRecord record : records.values()) {
        validateCoordinationReferences(record, view);
      }
      for (JournalEntry entry : journal) {
        for (CatalogRecord record : entry.transaction().records()) {
          validateCoordinationReferences(record, view);
        }
      }
      _membership = membership;
      _records = records;
      _recordBytes = recordBytes;
      _names = names;
      _clients = clients;
      _lastAppliedIndex = restored.lastAppliedIndex();
      _catalogMutationIndex = catalogMutationIndex;
      _journal = journal;
      _journalFloor = restored.journalFloor();
      _journalBytes = journalBytes;
    } catch (Rejection | MembershipState.Rejected | IllegalArgumentException | NullPointerException e) {
      throw new IOException("Invalid metadata snapshot", e);
    }
  }

  private static void validateCoordinationReferences(CatalogRecord record, OperationCatalog.View view)
      throws IOException {
    if (TaskCatalog.taskKind(record.kind())) {
      TaskCatalog.validateReferences(record, view);
    } else if (record.kind() == RecordKind.OWNER_LAYOUT || record.kind() == RecordKind.OWNER_ASSIGNMENT) {
      OwnershipCatalog.validateReferences(record, view);
    } else if (record.kind() == RecordKind.CONTROLLER_OPERATION) {
      OperationCatalog.validateReferences(record, view);
    } else if (record.kind() == RecordKind.OFFLINE_INSTALLATION) {
      OfflineInstallationCatalog.validateReferences(record, view);
    } else if (record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT
        || record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT) {
      OfflineSegmentCatalog.validateReferences(record, view);
    }
  }

  private void validateMembershipMirror(MembershipState.Snapshot membership, Map<UUID, CatalogRecord> records,
      Map<RecordKey, UUID> names) throws IOException {
    int expected = membership.members().size() + (membership.control().controlEpoch() == null ? 0 : 1);
    long actual = records.values().stream().filter(record -> record.kind() == RecordKind.MEMBERSHIP_CONTROL
        || record.kind() == RecordKind.PARTICIPANT).count();
    require(actual == expected, Status.INVALID_REQUEST, "Membership mirror record count differs");
    if (membership.control().controlEpoch() != null) {
      validateMembershipRecord(RecordKind.MEMBERSHIP_CONTROL, MembershipProtocol.CONTROL_NAME,
          MetadataProtocol.toTree(membership.control()), membership.control().clockBarrierIndex(), records, names);
    }
    for (Map.Entry<String, MembershipState.Member> entry : membership.members().entrySet()) {
      MembershipProtocol.ParticipantState participant = entry.getValue().state();
      validateMembershipRecord(RecordKind.PARTICIPANT, entry.getKey(), MetadataProtocol.toTree(participant),
          participant.lastModifiedIndex(), records, names);
    }
  }

  private void validateMembershipRecord(RecordKind kind, String name, JsonNode value, long index,
      Map<UUID, CatalogRecord> records, Map<RecordKey, UUID> names) throws IOException {
    UUID id = membershipObjectId(kind, name);
    CatalogRecord record = records.get(id);
    // Disk JSON may represent a small long as IntNode. Compare canonical JSON bytes instead of Java node width,
    // retaining exact membership values without changing arbitrary catalog JSON number semantics.
    require(record != null && !record.deleted() && id.equals(names.get(new RecordKey(kind, name)))
        && record.kind() == kind && record.name().equals(name) && record.lastModifiedIndex() == index
        && Arrays.equals(MetadataProtocol.encodeValue(record.value()), MetadataProtocol.encodeValue(value)),
        Status.INVALID_REQUEST, "Membership mirror differs from durable state");
  }

  private Result register(RegisterClient command, long index) {
    require(command.incarnation() != null && command.expectedEpoch() >= 0, Status.INVALID_REQUEST,
        "Client registration requires an incarnation and expected epoch");
    ClientState previous = _clients.get(command.clientId());
    if (previous != null && previous.incarnation().equals(command.incarnation())) {
      require(!previous.retired(), Status.FENCED, "Client incarnation is retired");
      require(command.expectedEpoch() == previous.epoch() - 1, Status.CONFLICT,
          "Registration does not match the current incarnation");
      return new Result(Status.OK, revision(index), "", previous.epoch(), List.of(), null);
    }
    long epoch = previous == null ? 0 : previous.epoch();
    require(command.expectedEpoch() == epoch, Status.CONFLICT, "Client epoch changed");
    require(epoch < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Client epoch exhausted");
    _clients.put(command.clientId(), new ClientState(epoch + 1, command.incarnation(), false, new HashMap<>()));
    return new Result(Status.OK, revision(index), "", epoch + 1, List.of(), null);
  }

  private Result retire(RetireClient command, long index) {
    ClientState client = _clients.get(command.clientId());
    require(client != null && client.epoch() == command.clientEpoch(), Status.FENCED, "Client epoch differs");
    _clients.put(command.clientId(), new ClientState(client.epoch(), client.incarnation(), true, new HashMap<>()));
    return result(Status.OK, "", index);
  }

  private Result transact(SequencedCommand command, long actualTerm, long index)
      throws IOException {
    ClientState client = currentClient(command.clientId(), command.clientEpoch());
    identifier(command.streamId());
    require(command.sequence() > 0, Status.INVALID_REQUEST, "Sequence must be positive");
    StreamState stream = client.streams().get(command.streamId());
    if (stream == null) {
      require(client.streams().size() < MetadataProtocol.MAX_STREAMS_PER_CLIENT, Status.LIMIT_EXCEEDED,
          "Client stream limit reached");
      stream = new StreamState(0, 0, null);
    }
    require(command.sequence() > stream.floor(), Status.RESULT_EXPIRED, "Outcome has been acknowledged and retired");
    String digest = MetadataProtocol.digest(command);
    if (command.sequence() == stream.sequence()) {
      require(stream.outcome() != null && stream.outcome().digest().equals(digest), Status.CONFLICT,
          "Request identity was reused for a different command");
      return stream.outcome().result();
    }
    require(stream.sequence() < Long.MAX_VALUE && command.sequence() == stream.sequence() + 1,
        Status.OUT_OF_ORDER, "Sequences must be contiguous");
    require(stream.floor() == stream.sequence(), Status.UNACKNOWLEDGED_RESULT,
        "Acknowledge the previous outcome before advancing this stream");
    List<CatalogRecord> staged = new ArrayList<>();
    List<Integer> encodedSizes = new ArrayList<>();
    Result outcome;
    try {
      switch (command) {
        case Transaction transaction -> stageMutations(transaction.mutations(), index, staged);
        case ConfigTransaction transaction -> {
          Revision validated = transaction.validatedSnapshot();
          require(validated != null && validated.index() >= 0 && validated.index() <= _lastAppliedIndex,
              Status.INVALID_REQUEST, "Validation snapshot must be an applied catalog revision");
          require(_historyId.equals(validated.historyId()), Status.HISTORY_MISMATCH,
              "Validation snapshot history differs");
          require(_catalogMutationIndex <= validated.index(), Status.CONFLICT,
              "Catalog changed after validation; read and validate a fresh image");
          stageMutations(transaction.mutations(), index, staged);
        }
        case OwnershipCommand ownership -> staged.addAll(
            OwnershipCatalog.stage(ownership.request(), coordinationView(), actualTerm, index));
        case OperationCommand operation -> staged.addAll(
            OperationCatalog.stage(operation.request(), coordinationView(), index));
        case NativeOperationCommand operation -> staged.addAll(
            OperationCatalog.stageNative(operation.request(), coordinationView(), index));
        case OfflineSegmentCommand segment -> staged.addAll(
            OfflineSegmentCatalog.stage(segment.request(), coordinationView(), index));
        case NativeInstallationCommand installation -> staged.addAll(
            OfflineInstallationCatalog.stage(installation.request(), coordinationView(), index));
        case TaskCommand task -> staged.addAll(TaskCatalog.stage(task.request(), coordinationView(), index));
        case NativeTaskCommand task -> staged.addAll(
            TaskCatalog.stage(task.request(), task.minion(), coordinationView(), index));
      }
      require(staged.size() <= MetadataProtocol.MAX_MUTATIONS, Status.LIMIT_EXCEEDED,
          "Controller transaction exceeds the supported record count");
      Set<RecordKey> changedNames = new HashSet<>();
      Set<UUID> changedIds = new HashSet<>();
      for (CatalogRecord record : staged) {
        RecordKey key = new RecordKey(record.kind(), record.name());
        UUID activeId = _names.get(key);
        CatalogRecord previous = _records.get(record.objectId());
        require(activeId == null || activeId.equals(record.objectId()), Status.CONFLICT,
            "Staged record name belongs to another object");
        require(previous == null ? record.version() == 1
            : !previous.deleted() && previous.kind() == record.kind() && previous.name().equals(record.name())
                && previous.version() < Long.MAX_VALUE && record.version() == previous.version() + 1,
            Status.CONFLICT, "Staged record identity or version conflicts with an existing object");
        require(changedNames.add(new RecordKey(record.kind(), record.name())) && changedIds.add(record.objectId()),
            Status.INVALID_REQUEST, "A transaction may change each object and name only once");
        int encodedSize = MetadataProtocol.encodeValue(record).length;
        require(encodedSize <= MAX_RESULT_BYTES, Status.LIMIT_EXCEEDED,
            "Staged record exceeds the supported size");
        encodedSizes.add(encodedSize);
      }
      List<ObjectVersion> versions = staged.stream()
          .map(record -> new ObjectVersion(_historyId, record.objectId(), record.version())).toList();
      outcome = new Result(Status.OK, revision(index), "", null, versions, null);
    } catch (Rejection e) {
      staged.clear();
      encodedSizes.clear();
      outcome = result(e._status, e.getMessage(), index);
    } catch (CoordinationRejection e) {
      staged.clear();
      encodedSizes.clear();
      outcome = result(e.status(), e.getMessage(), index);
    }
    // Encode before committing so an unsupported/oversized result cannot leave a partial metadata transaction.
    encode(outcome);
    int recordIndex = 0;
    for (CatalogRecord record : staged) {
      _records.put(record.objectId(), record);
      if (isCatalogMutation(record)) {
        _catalogMutationIndex = index;
      }
      _recordBytes.put(record.objectId(), encodedSizes.get(recordIndex++));
      RecordKey key = new RecordKey(record.kind(), record.name());
      if (record.deleted()) {
        _names.remove(key);
      } else {
        _names.put(key, record.objectId());
      }
    }
    client.streams().put(command.streamId(), new StreamState(command.sequence(), stream.floor(),
        new Outcome(digest, outcome)));
    return outcome;
  }

  private static boolean isCatalogMutation(CatalogRecord record) {
    return record.kind() != RecordKind.MEMBERSHIP_CONTROL && record.kind() != RecordKind.PARTICIPANT;
  }

  private void stageMutations(List<Mutation> mutations, long index, List<CatalogRecord> staged) {
    require(mutations != null && !mutations.isEmpty() && mutations.size() <= MetadataProtocol.MAX_MUTATIONS,
        Status.INVALID_REQUEST, "Transaction mutation count is outside the supported bounds");
    for (Mutation mutation : mutations) {
      staged.add(stage(mutation, index));
    }
  }

  private CatalogRecord stage(Mutation mutation, long index) {
    require(mutation != null && mutation.action() != null && mutation.kind() != null && mutation.expected() != null,
        Status.INVALID_REQUEST, "Mutation type and expected version are required");
    require(mutation.kind() == RecordKind.CLUSTER_CONFIG || mutation.kind() == RecordKind.SCHEMA
        || mutation.kind() == RecordKind.TABLE_CONFIG, Status.INVALID_REQUEST,
        "Coordination records require typed coordination commands");
    identifier(mutation.name());
    ObjectVersion expected = mutation.expected();
    require(_historyId.equals(expected.historyId()), Status.HISTORY_MISMATCH, "Object version history differs");
    require(expected.objectId() != null && expected.version() >= 0, Status.INVALID_REQUEST, "Invalid object version");
    UUID activeId = _names.get(new RecordKey(mutation.kind(), mutation.name()));
    if (mutation.action() == Action.CREATE) {
      require(expected.version() == 0, Status.INVALID_REQUEST, "Create expects version zero");
      require(activeId == null && !_records.containsKey(expected.objectId()), Status.CONFLICT,
          "Record name or object ID already exists");
    } else {
      CatalogRecord current = _records.get(expected.objectId());
      require(current != null && !current.deleted() && expected.objectId().equals(activeId)
          && current.kind() == mutation.kind() && current.name().equals(mutation.name())
          && current.version() == expected.version(), Status.CONFLICT, "Object incarnation or version changed");
    }
    require(expected.version() < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Object version exhausted");
    JsonNode value = mutation.value();
    boolean deleted = mutation.action() == Action.DELETE;
    if (deleted) {
      require(value == null || value.isNull(), Status.INVALID_REQUEST, "Delete must not supply a value");
    } else {
      validateValue(mutation.kind(), mutation.name(), value);
    }
    return new CatalogRecord(mutation.kind(), mutation.name(), expected.objectId(), expected.version() + 1,
        MetadataProtocol.FORMAT_VERSION, index, deleted, deleted ? null : MetadataProtocol.canonical(value));
  }

  private Result acknowledge(Acknowledge command, long index) {
    ClientState client = currentClient(command.clientId(), command.clientEpoch());
    identifier(command.streamId());
    StreamState stream = client.streams().get(command.streamId());
    require(stream != null && command.throughSequence() >= 0 && command.throughSequence() <= stream.sequence(),
        Status.OUT_OF_ORDER, "Cannot acknowledge an unexecuted sequence");
    if (command.throughSequence() > stream.floor()) {
      client.streams().put(command.streamId(), new StreamState(stream.sequence(), command.throughSequence(), null));
    }
    return result(Status.OK, "", index);
  }

  private Result readRecord(GetRecord query, long readIndex) {
    identifier(query.name());
    require(query.kind() != null, Status.INVALID_REQUEST, "Record kind is required");
    UUID id = _names.get(new RecordKey(query.kind(), query.name()));
    require(id != null, Status.NOT_FOUND, "Record is absent");
    return new Result(Status.OK, revision(readIndex), "", null, List.of(), _records.get(id));
  }

  private Result readOutcome(GetOutcome query) {
    ClientState client = currentClient(query.clientId(), query.clientEpoch());
    identifier(query.streamId());
    require(query.sequence() > 0, Status.INVALID_REQUEST, "Sequence must be positive");
    StreamState stream = client.streams().get(query.streamId());
    require(stream != null, Status.NOT_FOUND, "Request stream is absent");
    require(query.sequence() > stream.floor(), Status.RESULT_EXPIRED, "Outcome has been acknowledged and retired");
    require(query.sequence() == stream.sequence() && stream.outcome() != null, Status.NOT_FOUND,
        "Request outcome is absent");
    return stream.outcome().result();
  }

  private ClientState currentClient(String id, long epoch) {
    identifier(id);
    ClientState client = _clients.get(id);
    require(client != null && !client.retired() && client.epoch() == epoch, Status.FENCED,
        "Client is unregistered, retired or has a different epoch");
    return client;
  }

  private ArrayDeque<JournalEntry> validateJournal(Snapshot snapshot, Map<UUID, CatalogRecord> records)
      throws IOException {
    require(snapshot.journal() != null && snapshot.journalFloor() >= 0
        && snapshot.journalFloor() <= snapshot.lastAppliedIndex()
        && snapshot.journal().size() <= MAX_JOURNAL_TRANSACTIONS, Status.INVALID_REQUEST,
        "Invalid retained journal header");
    ArrayDeque<JournalEntry> journal = new ArrayDeque<>();
    Map<UUID, CatalogRecord> latestRecords = new HashMap<>();
    long previous = snapshot.journalFloor();
    long bytes = 0;
    for (ChangeTransaction transaction : snapshot.journal()) {
      require(transaction != null && transaction.previousIndex() == previous && transaction.index() > previous
          && transaction.index() <= snapshot.lastAppliedIndex()
          && transaction.records().size() <= MetadataProtocol.MAX_MUTATIONS, Status.INVALID_REQUEST,
          "Invalid retained journal sequence");
      Set<UUID> changedIds = new HashSet<>();
      Set<RecordKey> changedNames = new HashSet<>();
      Map<RecordKind, Integer> recordBytesByKind = new EnumMap<>(RecordKind.class);
      for (CatalogRecord record : transaction.records()) {
        validateRecord(record, transaction.index());
        CatalogRecord current = records.get(record.objectId());
        require(record.lastModifiedIndex() == transaction.index() && changedIds.add(record.objectId())
            && changedNames.add(new RecordKey(record.kind(), record.name())) && current != null
            && current.kind() == record.kind() && current.name().equals(record.name())
            && current.version() >= record.version(), Status.INVALID_REQUEST, "Invalid retained journal record");
        CatalogRecord previousRecord = latestRecords.put(record.objectId(), record);
        require(previousRecord == null || (!previousRecord.deleted()
            && previousRecord.version() + 1 == record.version()), Status.INVALID_REQUEST,
            "Retained journal skips an object version or resurrects a tombstone");
        recordBytesByKind.merge(record.kind(), MetadataProtocol.encodeValue(record).length + 1, Integer::sum);
      }
      JournalEntry entry = journalEntry(transaction, recordBytesByKind);
      bytes += entry.encodedBytes();
      require(bytes <= MAX_JOURNAL_BYTES, Status.INVALID_REQUEST, "Retained journal exceeds byte bound");
      journal.addLast(entry);
      previous = transaction.index();
    }
    require(previous == snapshot.lastAppliedIndex(), Status.INVALID_REQUEST,
        "Retained journal does not cover the certified image revision");
    for (CatalogRecord record : records.values()) {
      CatalogRecord latest = latestRecords.get(record.objectId());
      require(latest != null ? record.equals(latest) : record.lastModifiedIndex() <= snapshot.journalFloor(),
          Status.INVALID_REQUEST, "Retained journal does not reproduce the current record");
    }
    return journal;
  }

  private void validateRecord(CatalogRecord record, long snapshotIndex)
      throws IOException {
    require(record != null && record.kind() != null && record.objectId() != null && record.version() > 0
        && record.schemaVersion() == MetadataProtocol.FORMAT_VERSION && record.lastModifiedIndex() > 0
        && record.lastModifiedIndex() <= snapshotIndex, Status.INVALID_REQUEST, "Invalid snapshot record");
    identifier(record.name());
    if (TaskCatalog.taskKind(record.kind())) {
      TaskCatalog.validateRecord(record, _historyId, snapshotIndex);
      return;
    }
    if (record.kind() == RecordKind.OWNER_LAYOUT || record.kind() == RecordKind.OWNER_ASSIGNMENT) {
      OwnershipCatalog.validateRecord(record, _historyId, snapshotIndex);
      return;
    }
    if (record.kind() == RecordKind.CONTROLLER_OPERATION) {
      OperationCatalog.validateRecord(record, _historyId, snapshotIndex);
      return;
    }
    if (record.kind() == RecordKind.OFFLINE_INSTALLATION) {
      OfflineInstallationCatalog.validateRecord(record, _historyId, snapshotIndex);
      return;
    }
    if (record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT || record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT) {
      OfflineSegmentCatalog.validateRecord(record, _historyId, snapshotIndex);
      return;
    }
    if (record.kind() == RecordKind.MEMBERSHIP_CONTROL || record.kind() == RecordKind.PARTICIPANT) {
      require(!record.deleted(), Status.INVALID_REQUEST, "Membership identity records cannot be tombstones");
      require(record.value() != null && record.value().isObject(), Status.INVALID_REQUEST,
          "Membership value must be an object");
      String revisionField = record.kind() == RecordKind.MEMBERSHIP_CONTROL ? "clockBarrierIndex" : "lastModifiedIndex";
      require(record.value().path(revisionField).isIntegralNumber()
          && record.value().path(revisionField).longValue() == record.lastModifiedIndex(), Status.INVALID_REQUEST,
          "Membership payload revision differs from record");
    }
    if (record.deleted()) {
      require(record.value() == null || record.value().isNull(), Status.INVALID_REQUEST, "Tombstone contains a value");
    } else {
      validateValue(record.kind(), record.name(), record.value());
    }
  }

  private static int minimumSnapshotFormat(CatalogRecord record) {
    return switch (record.kind()) {
      case CLUSTER_CONFIG, SCHEMA, TABLE_CONFIG -> 1;
      case MEMBERSHIP_CONTROL, PARTICIPANT -> 8;
      case OWNER_LAYOUT, OWNER_ASSIGNMENT -> 4;
      case CONTROLLER_OPERATION -> {
        JsonNode value = record.value();
        if (value != null && (value.at("/input/reconcile").asBoolean()
            || value.at("/input/reload/reconcile").asBoolean()
            || value.at("/input/submittedAtMs").asLong() != 0
            || value.at("/input/reload/submittedAtMs").asLong() != 0)) {
          yield 7;
        }
        if (value != null && (value.hasNonNull("completionProof") || value.at("/input/initialLoad").asBoolean()
            || value.at("/input/reload/initialLoad").asBoolean())) {
          yield 6;
        }
        yield value != null && "ASSIGNED_OFFLINE_SEGMENT_RELOAD".equals(value.at("/input/specType").asText()) ? 5 : 4;
      }
      case OFFLINE_SEGMENT_CONTENT, OFFLINE_REPLICA_ASSIGNMENT -> 5;
      case OFFLINE_INSTALLATION -> 6;
      case TASK_QUEUE, TASK, TASK_ATTEMPT -> 9;
    };
  }

  private void validateStream(StreamState stream, long snapshotIndex)
      throws IOException {
    require(stream != null && stream.sequence() > 0 && stream.floor() >= 0 && stream.floor() <= stream.sequence(),
        Status.INVALID_REQUEST, "Invalid stream sequence");
    require(stream.outcome() == null ? stream.floor() == stream.sequence()
        : stream.floor() == stream.sequence() - 1, Status.INVALID_REQUEST, "Invalid stream retirement floor");
    if (stream.outcome() != null) {
      Outcome outcome = stream.outcome();
      Result result = outcome.result();
      require(outcome.digest() != null && outcome.digest().matches("[0-9a-f]{64}") && result != null
          && result.status() != null && result.revision() != null && result.record() == null
          && _historyId.equals(result.revision().historyId()) && result.revision().index() > 0
          && result.revision().index() <= snapshotIndex && result.versions() != null
          && result.versions().size() <= MetadataProtocol.MAX_MUTATIONS, Status.INVALID_REQUEST,
          "Invalid retained request outcome");
      for (ObjectVersion version : result.versions()) {
        require(version != null && _historyId.equals(version.historyId()) && version.objectId() != null
            && version.version() > 0, Status.INVALID_REQUEST, "Invalid retained object version");
      }
      encode(result);
    }
  }

  private void validateValue(RecordKind kind, String name, JsonNode value) {
    require(value != null && value.isObject(), Status.INVALID_REQUEST, "Catalog values must be typed JSON objects");
    try {
      switch (kind) {
        case CLUSTER_CONFIG -> {
          require(MetadataProtocol.CLUSTER_CONFIG_NAME.equals(name), Status.INVALID_REQUEST,
              "Cluster config has one fixed name");
          value.properties().forEach(entry -> {
            identifier(entry.getKey());
            require(entry.getValue().isTextual(), Status.INVALID_REQUEST, "Cluster config values must be strings");
          });
        }
        case SCHEMA -> {
          Schema schema = JsonUtils.jsonNodeToObject(value, Schema.class);
          require(name.equals(schema.getSchemaName()), Status.INVALID_REQUEST, "Schema name differs from record");
          schema.validate();
        }
        case TABLE_CONFIG -> {
          TableConfig table = JsonUtils.jsonNodeToObject(value, TableConfig.class);
          TableType type = table.getTableType();
          require(name.equals(table.getTableName()) && type != null
              && name.equals(TableNameBuilder.forType(type).tableNameWithType(name)), Status.INVALID_REQUEST,
              "Table config requires its canonical typed table name");
        }
        case MEMBERSHIP_CONTROL -> {
          require(MembershipProtocol.CONTROL_NAME.equals(name), Status.INVALID_REQUEST, "Control name differs");
          MembershipState.validateControl(MembershipProtocol.decodeControl(value), _historyId, Long.MAX_VALUE);
        }
        case PARTICIPANT -> {
          MembershipProtocol.ParticipantState participant = MembershipProtocol.decodeParticipant(value);
          MembershipState.validateParticipant(participant, _historyId, Long.MAX_VALUE);
          require(name.equals(participant.participant().participantId()), Status.INVALID_REQUEST,
              "Participant name differs");
        }
        default -> throw new Rejection(Status.INVALID_REQUEST, "Unsupported catalog record family");
      }
    } catch (IOException | RuntimeException e) {
      throw new Rejection(Status.INVALID_REQUEST, "Invalid typed catalog value");
    }
  }

  private static void identifier(String value) {
    require(value != null && !value.isBlank() && value.length() <= MAX_IDENTIFIER_LENGTH
        && value.chars().noneMatch(Character::isISOControl), Status.INVALID_REQUEST, "Invalid metadata identifier");
  }

  private Revision revision(long index) {
    return new Revision(_historyId, index);
  }

  private Result result(Status status, String detail, long index) {
    return new Result(status, revision(index), detail, null, List.of(), null);
  }

  private static byte[] encode(Result result)
      throws IOException {
    byte[] bytes = MetadataProtocol.encodeValue(result);
    if (bytes.length > MAX_RESULT_BYTES) {
      throw new ResponseTooLargeException("Metadata result exceeds the supported size");
    }
    return bytes;
  }

  private static void require(boolean condition, Status status, String message) {
    if (!condition) {
      throw new Rejection(status, message);
    }
  }

  private record RecordKey(RecordKind kind, String name) {
  }

  private record JournalEntry(ChangeTransaction transaction, int envelopeBytes,
                              Map<RecordKind, Integer> recordBytesByKind) {
    private int encodedBytes() {
      return encodedBytes(recordBytesByKind.keySet());
    }

    private int encodedBytes(Set<RecordKind> kinds) {
      int bytes = 0;
      for (RecordKind kind : kinds) {
        bytes += recordBytesByKind.getOrDefault(kind, 0);
      }
      // Per-kind totals include one comma per record; a nonempty JSON array has one fewer comma than records.
      return envelopeBytes + (bytes == 0 ? 0 : bytes - 1);
    }
  }

  private record Outcome(String digest, Result result) {
  }

  private record StreamState(long sequence, long floor, Outcome outcome) {
  }

  private record ClientState(long epoch, UUID incarnation, boolean retired, Map<String, StreamState> streams) {
  }

  private record ClientSnapshot(String clientId, long epoch, UUID incarnation, boolean retired,
                                Map<String, StreamState> streams) {
  }

  private record LegacySnapshot(int formatVersion, UUID clusterId, UUID historyId, long lastAppliedIndex,
                                List<CatalogRecord> records, List<ClientSnapshot> clients) {
  }

  private record JournalSnapshot(int formatVersion, UUID clusterId, UUID historyId, long lastAppliedIndex,
                                 List<CatalogRecord> records, List<ClientSnapshot> clients, int journalPolicyVersion,
                                 long journalFloor, List<ChangeTransaction> journal) {
  }

  /// Only a never-initialized legacy membership image can be recovered without guessing prior grant lifetimes.
  private record PreTimingControl(MembershipProtocol.ControlToken controlEpoch, long round,
                                  MembershipProtocol.Policy policy, long clockBarrierIndex) {
  }

  private record PreTimingMembership(PreTimingControl control, Map<String, JsonNode> members) {
  }

  private record PreTimingSnapshot(int formatVersion, UUID clusterId, UUID historyId, long lastAppliedIndex,
                                   List<CatalogRecord> records, List<ClientSnapshot> clients, int journalPolicyVersion,
                                   long journalFloor, List<ChangeTransaction> journal, PreTimingMembership membership) {
  }

  private record Snapshot(int formatVersion, UUID clusterId, UUID historyId, long lastAppliedIndex,
                          List<CatalogRecord> records, List<ClientSnapshot> clients, int journalPolicyVersion,
                          long journalFloor, List<ChangeTransaction> journal, MembershipState.Snapshot membership) {
  }

  private static final class Rejection extends RuntimeException {
    private final Status _status;

    private Rejection(Status status, String message) {
      super(message);
      _status = status;
    }
  }
}
