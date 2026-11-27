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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Action;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Cancel;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Checkpoint;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Claim;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Complete;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Spec;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.State;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Submit;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.TargetMaterialization;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Stateless operation staging over a coherent catalog. The caller atomically publishes the returned ledger and
/// domain records with its durable request outcome. Validation and staging never perform participant I/O.
final class OperationCatalog {
  private OperationCatalog() {
  }

  interface View extends OwnershipCatalog.View {
    CatalogRecord record(UUID objectId);

    /// Validate and return a proposed record without modifying this view.
    CatalogRecord stage(Mutation mutation, long index);
  }

  static List<CatalogRecord> stage(Operation request, View view, long index)
      throws IOException {
    require(request != null && request.operationId() != null && index > 0, Status.INVALID_REQUEST,
        "Operation and committed index are required");
    return switch (request) {
      case Submit submit -> submit(submit, view, index);
      case Claim claim -> claim(claim, view, index);
      case Checkpoint checkpoint -> checkpoint(checkpoint, view, index);
      case Complete complete -> complete(complete, view, index);
      case Cancel cancel -> cancel(cancel, view, index);
    };
  }

  private static List<CatalogRecord> submit(Submit request, View view, long index) {
    validateSpec(request.input(), view.historyId());
    require(view.record(RecordKind.CONTROLLER_OPERATION, recordName(request.operationId())) == null
        && view.record(objectId(view.historyId(), request.operationId())) == null, Status.CONFLICT,
        "Operation IDs cannot be reused");
    currentTable(request.input(), view);
    validateTargetReferences(request.input(), view, true);
    OperationState state = new OperationState(view.historyId(), request.operationId(), request.input(),
        State.ACCEPTED, 0, null, List.of(), new Progress(0, 0, ""), null);
    return List.of(record(state, 1, index));
  }

  private static List<CatalogRecord> claim(Claim request, View view, long index)
      throws IOException {
    CatalogRecord previous = current(request.operationId(), request.expectedVersion(), view);
    OperationState state = OperationProtocol.decodeOperation(previous.value());
    requireActive(state);
    OwnershipCatalog.validateTableOwner(request.owner(), state.input().tableName(), view);
    currentTable(state.input(), view);
    validateDependencies(request.dependencies(), state.input(), view.historyId(), true);
    for (ObjectVersion dependency : request.dependencies()) {
      require(!dependency.objectId().equals(previous.objectId()), Status.INVALID_REQUEST,
          "An operation cannot depend on its own changing ledger version");
    }
    currentDependencies(request.dependencies(), view);
    OperationState claimed = new OperationState(state.historyId(), state.operationId(), state.input(), State.RUNNING,
        nextAttempt(state), request.owner(), request.dependencies(), new Progress(0, 0, ""), null);
    return List.of(record(claimed, nextVersion(previous), index));
  }

  private static List<CatalogRecord> checkpoint(Checkpoint request, View view, long index)
      throws IOException {
    CatalogRecord previous = current(request.operationId(), request.expectedVersion(), view);
    OperationState state = OperationProtocol.decodeOperation(previous.value());
    currentAttempt(state, request.owner(), request.attempt(), view);
    validateProgress(request.progress(), targetCount(state.input()));
    monotonicProgress(state.progress(), request.progress());
    currentDependencies(state.dependencies(), view);
    OperationState next = new OperationState(state.historyId(), state.operationId(), state.input(), State.RUNNING,
        state.attempt(), state.owner(), state.dependencies(), request.progress(), null);
    return List.of(record(next, nextVersion(previous), index));
  }

  private static List<CatalogRecord> complete(Complete request, View view, long index)
      throws IOException {
    CatalogRecord previous = current(request.operationId(), request.expectedVersion(), view);
    OperationState state = OperationProtocol.decodeOperation(previous.value());
    currentAttempt(state, request.owner(), request.attempt(), view);
    require(request.terminal() == State.SUCCEEDED || request.terminal() == State.FAILED, Status.INVALID_REQUEST,
        "Completion requires SUCCEEDED or FAILED");
    validateProgress(request.result(), targetCount(state.input()));
    monotonicProgress(state.progress(), request.result());
    if (request.terminal() == State.SUCCEEDED) {
      require(request.result().completedTargets() == targetCount(state.input())
          && request.result().failedTargets() == 0, Status.INVALID_REQUEST,
          "Successful completion requires every target to succeed");
    }
    require(request.effects() != null && request.effects().size() < MetadataProtocol.MAX_MUTATIONS,
        Status.INVALID_REQUEST, "Operation effects exceed the atomic mutation bound");
    require(request.terminal() == State.SUCCEEDED || request.effects().isEmpty(), Status.INVALID_REQUEST,
        "Failed operations cannot publish domain effects");
    currentDependencies(state.dependencies(), view);
    List<CatalogRecord> staged = new ArrayList<>();
    Set<UUID> changed = new HashSet<>();
    for (Mutation effect : request.effects()) {
      require(effect != null && effect.action() == Action.UPDATE && effect.kind() == RecordKind.TABLE_CONFIG
          && state.input().tableName().equals(effect.name()) && effect.expected() != null
          && state.input().tableObjectId().equals(effect.expected().objectId()), Status.INVALID_REQUEST,
          "Operation effects must update the captured target table configuration");
      require(state.dependencies().contains(effect.expected()), Status.CONFLICT,
          "Operation effect version differs from the captured target table version");
      require(changed.add(effect.expected().objectId()), Status.INVALID_REQUEST,
          "An operation cannot publish multiple effects for one object");
      staged.add(view.stage(effect, index));
    }
    OperationState completed = new OperationState(state.historyId(), state.operationId(), state.input(),
        request.terminal(), state.attempt(), state.owner(), state.dependencies(), request.result(), request.result());
    staged.add(record(completed, nextVersion(previous), index));
    return List.copyOf(staged);
  }

  private static List<CatalogRecord> cancel(Cancel request, View view, long index)
      throws IOException {
    CatalogRecord previous = current(request.operationId(), request.expectedVersion(), view);
    OperationState state = OperationProtocol.decodeOperation(previous.value());
    requireActive(state);
    // A new owner can retire obsolete intent after table deletion/recreation. This only fences future publication.
    OwnershipCatalog.validateTableOwner(request.owner(), state.input().tableName(), view);
    detail(request.detail());
    Progress result = new Progress(state.progress().completedTargets(), state.progress().failedTargets(),
        request.detail());
    OperationState cancelled = new OperationState(state.historyId(), state.operationId(), state.input(),
        State.CANCELLED, nextAttempt(state), request.owner(), state.dependencies(), result, result);
    return List.of(record(cancelled, nextVersion(previous), index));
  }

  private static CatalogRecord current(UUID operationId, long expectedVersion, View view)
      throws IOException {
    require(expectedVersion > 0, Status.INVALID_REQUEST, "Expected operation version must be positive");
    CatalogRecord record = view.record(RecordKind.CONTROLLER_OPERATION, recordName(operationId));
    require(record != null && !record.deleted(), Status.NOT_FOUND, "Operation is absent");
    require(record.version() == expectedVersion, Status.CONFLICT, "Operation version changed");
    validateRecord(record, view.historyId(), Long.MAX_VALUE);
    return record;
  }

  private static void requireActive(OperationState state) {
    require(state.status() == State.ACCEPTED || state.status() == State.RUNNING, Status.CONFLICT,
        "Terminal operations cannot be reopened");
  }

  private static void currentAttempt(OperationState state, OwnerToken owner, long attempt, View view)
      throws IOException {
    requireActive(state);
    require(state.status() == State.RUNNING && attempt > 0 && state.attempt() == attempt
        && state.owner().equals(owner), Status.FENCED, "Operation attempt or claimed owner changed");
    OwnershipCatalog.validateTableOwner(owner, state.input().tableName(), view);
    currentTable(state.input(), view);
  }

  private static void currentTable(Spec input, View view) {
    CatalogRecord table = view.record(RecordKind.TABLE_CONFIG, input.tableName());
    require(table != null && !table.deleted() && input.tableObjectId().equals(table.objectId()), Status.FENCED,
        "Operation target table incarnation changed or was deleted");
  }

  private static void currentDependencies(List<ObjectVersion> dependencies, View view) {
    for (ObjectVersion dependency : dependencies) {
      CatalogRecord record = view.record(dependency.objectId());
      require(record != null && !record.deleted() && record.version() == dependency.version(), Status.CONFLICT,
          "An operation dependency changed or was deleted");
    }
  }

  private static long nextAttempt(OperationState state) {
    require(state.attempt() < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Operation attempts exhausted");
    return state.attempt() + 1;
  }

  private static long nextVersion(CatalogRecord previous) {
    require(previous.version() < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Operation versions exhausted");
    return previous.version() + 1;
  }

  /// Validate every persisted record, including retained journal entries, without requiring historical authority
  /// or dependency versions to remain current. Whole-image references are checked separately.
  static void validateRecord(CatalogRecord record, UUID historyId, long snapshotIndex)
      throws IOException {
    require(record != null && record.kind() == RecordKind.CONTROLLER_OPERATION && !record.deleted()
        && record.version() > 0 && record.schemaVersion() == MetadataProtocol.FORMAT_VERSION
        && record.lastModifiedIndex() > 0 && record.lastModifiedIndex() <= snapshotIndex && record.value() != null,
        Status.INVALID_REQUEST, "Invalid operation record envelope");
    OperationState state = OperationProtocol.decodeOperation(record.value());
    require(state != null && historyId.equals(state.historyId()) && state.operationId() != null
        && recordName(state.operationId()).equals(record.name())
        && objectId(historyId, state.operationId()).equals(record.objectId()), Status.INVALID_REQUEST,
        "Operation record identity or history differs");
    validateSpec(state.input(), historyId);
    require(state.status() != null && state.attempt() >= 0 && state.attempt() < record.version(),
        Status.INVALID_REQUEST, "Invalid operation status or attempt");
    validateProgress(state.progress(), targetCount(state.input()));
    boolean accepted = state.status() == State.ACCEPTED;
    validateDependencies(state.dependencies(), state.input(), historyId,
        !accepted && state.status() != State.CANCELLED);
    if (accepted) {
      require(record.version() == 1 && state.attempt() == 0 && state.owner() == null
          && state.dependencies().isEmpty() && state.progress().equals(new Progress(0, 0, ""))
          && state.result() == null, Status.INVALID_REQUEST, "Accepted operation contains claimed state");
      return;
    }
    require(state.attempt() > 0 && state.owner() != null, Status.INVALID_REQUEST, "Claimed operation has no owner");
    OwnershipCatalog.validateStoredTableOwnerShape(state.owner(), state.input().tableName(), historyId);
    if (state.status() == State.RUNNING) {
      require(state.result() == null, Status.INVALID_REQUEST, "Running operation contains a terminal result");
    } else {
      validateProgress(state.result(), targetCount(state.input()));
      require(state.result().equals(state.progress()), Status.INVALID_REQUEST,
          "Terminal operation progress differs from its result");
      if (state.status() == State.SUCCEEDED) {
        require(state.result().completedTargets() == targetCount(state.input()) && state.result().failedTargets() == 0,
            Status.INVALID_REQUEST, "Successful operation has incomplete targets");
      }
    }
  }

  static void validateReferences(CatalogRecord record, View view)
      throws IOException {
    OperationState state = OperationProtocol.decodeOperation(record.value());
    CatalogRecord table = view.record(state.input().tableObjectId());
    require(table != null && table.kind() == RecordKind.TABLE_CONFIG && table.name().equals(state.input().tableName()),
        Status.INVALID_REQUEST, "Operation references an absent or mismatched table incarnation");
    validateTargetReferences(state.input(), view, false);
    if (state.owner() != null) {
      OwnershipCatalog.validateStoredTableOwner(state.owner(), state.input().tableName(), view);
    }
    for (ObjectVersion dependency : state.dependencies()) {
      CatalogRecord referenced = view.record(dependency.objectId());
      require(referenced != null && referenced.version() >= dependency.version()
          && !dependency.objectId().equals(record.objectId()), Status.INVALID_REQUEST,
          "Operation references an absent or future dependency version");
    }
  }

  private static void validateTargetReferences(Spec spec, View view, boolean current) {
    switch (spec) {
      case OfflineSegmentReload reload -> {
        for (TargetMaterialization target : reload.targets()) {
          ParticipantToken server = target.server();
          ParticipantState registered = view.participant(server.participantId());
          if (current) {
            require(registered != null && registered.participant().equals(server), Status.FENCED,
                "Reload target server registration changed or is absent");
          } else {
            require(registered != null && registered.participant().historyId().equals(server.historyId())
                && registered.participant().role() == Role.SERVER
                && registered.participant().registrationEpoch() >= server.registrationEpoch()
                && (registered.participant().registrationEpoch() > server.registrationEpoch()
                || registered.participant().equals(server)), Status.INVALID_REQUEST,
                "Operation references an absent or future server registration");
          }
        }
      }
    }
  }

  private static void validateSpec(Spec spec, UUID historyId) {
    require(spec != null && spec.tableObjectId() != null, Status.INVALID_REQUEST,
        "Operation table identity is required");
    identifier(spec.tableName(), 1024);
    require(TableNameBuilder.getTableTypeFromTableName(spec.tableName()) == TableType.OFFLINE
        && spec.tableName().chars().noneMatch(Character::isWhitespace), Status.INVALID_REQUEST,
        "Offline reload requires a canonical OFFLINE table name");
    String rawName = TableNameBuilder.extractRawTableName(spec.tableName());
    int separator = rawName.indexOf('.');
    require(!rawName.isEmpty() && (separator < 0 || separator > 0 && separator < rawName.length() - 1
        && separator == rawName.lastIndexOf('.')), Status.INVALID_REQUEST, "Invalid database-qualified table name");
    switch (spec) {
      case OfflineSegmentReload reload -> {
        identifier(reload.segmentName(), 1024);
        require(reload.segmentObjectId() != null && reload.contentSha256() != null
            && reload.contentSha256().matches("[0-9a-f]{64}") && reload.targets() != null && !reload.targets().isEmpty()
            && reload.targets().size() <= OperationProtocol.MAX_TARGETS, Status.INVALID_REQUEST,
            "Offline reload requires bounded immutable content and target identities");
        Set<String> servers = new HashSet<>();
        Set<UUID> materializations = new HashSet<>();
        for (TargetMaterialization target : reload.targets()) {
          require(target != null && target.server() != null && target.assignmentEpoch() > 0
              && target.materializationId() != null, Status.INVALID_REQUEST, "Invalid reload target identity");
          ParticipantToken server = target.server();
          identifier(server.participantId(), 1024);
          require(historyId.equals(server.historyId()) && server.role() == Role.SERVER && server.incarnation() != null
              && server.registrationEpoch() > 0 && servers.add(server.participantId())
              && materializations.add(target.materializationId()), Status.INVALID_REQUEST,
              "Reload targets require unique complete server and materialization identities in this history");
        }
      }
    }
  }

  private static void validateDependencies(List<ObjectVersion> dependencies, Spec input, UUID historyId,
      boolean requireTable) {
    require(dependencies != null && dependencies.size() <= OperationProtocol.MAX_DEPENDENCIES, Status.INVALID_REQUEST,
        "Operation dependencies exceed the supported bound");
    Set<UUID> ids = new HashSet<>();
    for (ObjectVersion dependency : dependencies) {
      require(dependency != null && historyId.equals(dependency.historyId()) && dependency.objectId() != null
          && dependency.version() > 0 && ids.add(dependency.objectId()), Status.INVALID_REQUEST,
          "Operation dependencies require unique complete versions in this history");
    }
    require(!requireTable || ids.contains(input.tableObjectId()), Status.INVALID_REQUEST,
        "Operation must capture its target table version");
    require(dependencies.isEmpty() || ids.contains(input.tableObjectId()), Status.INVALID_REQUEST,
        "Captured operation dependencies omit the target table");
  }

  private static int targetCount(Spec spec) {
    return switch (spec) {
      case OfflineSegmentReload reload -> reload.targets().size();
    };
  }

  private static void validateProgress(Progress progress, int targets) {
    require(progress != null && progress.completedTargets() >= 0 && progress.failedTargets() >= 0
        && progress.failedTargets() <= targets && progress.completedTargets() <= targets - progress.failedTargets(),
        Status.INVALID_REQUEST, "Operation progress exceeds its immutable target count");
    detail(progress.detail());
  }

  private static void monotonicProgress(Progress previous, Progress next) {
    require(next.completedTargets() >= previous.completedTargets() && next.failedTargets() >= previous.failedTargets(),
        Status.CONFLICT, "Operation progress cannot regress within an attempt");
  }

  private static void detail(String detail) {
    require(detail != null && detail.length() <= OperationProtocol.MAX_DETAIL_LENGTH, Status.INVALID_REQUEST,
        "Operation detail exceeds the supported bound");
  }

  private static void identifier(String value, int maximum) {
    require(value != null && !value.isBlank() && value.length() <= maximum
        && value.chars().noneMatch(Character::isISOControl), Status.INVALID_REQUEST, "Invalid operation identifier");
  }

  static String recordName(UUID operationId) {
    return OperationProtocol.recordName(operationId);
  }

  static UUID objectId(UUID historyId, UUID operationId) {
    return UUID.nameUUIDFromBytes((historyId + "\0" + RecordKind.CONTROLLER_OPERATION + "\0" + recordName(operationId))
        .getBytes(StandardCharsets.UTF_8));
  }

  private static CatalogRecord record(OperationState state, long version, long index) {
    return new CatalogRecord(RecordKind.CONTROLLER_OPERATION, recordName(state.operationId()),
        objectId(state.historyId(), state.operationId()), version, MetadataProtocol.FORMAT_VERSION, index, false,
        MetadataProtocol.toTree(state));
  }

  private static void require(boolean condition, Status status, String message) {
    if (!condition) {
      throw new CoordinationRejection(status, message);
    }
  }
}
