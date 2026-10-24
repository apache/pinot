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
import java.util.List;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.AssignOwner;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.ClusterWork;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.ConfigureLayout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.HashVersion;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Layout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Scope;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TableShard;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TablelessTask;


/// Stateless ownership validation/staging over one coherent catalog view. Callers commit the returned records
/// atomically with the durable request outcome; this helper never changes the view or performs external work.
final class OwnershipCatalog {
  private static final Layout SUPPORTED_LAYOUT = new Layout(1, 24, HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);

  private OwnershipCatalog() {
  }

  interface View {
    CatalogRecord record(RecordKind kind, String name);

    ParticipantState participant(String participantId);

    ControlState control();

    UUID historyId();
  }

  static List<CatalogRecord> stage(Operation operation, View view, long actualTerm, long index)
      throws IOException {
    require(operation != null && index > 0 && actualTerm > 0, Status.INVALID_REQUEST,
        "Ownership apply requires an operation and the actual committed Raft term/index");
    return switch (operation) {
      case ConfigureLayout configure -> configure(configure, view, actualTerm, index);
      case AssignOwner assign -> assign(assign, view, actualTerm, index);
    };
  }

  private static List<CatalogRecord> configure(ConfigureLayout request, View view, long actualTerm, long index)
      throws IOException {
    validateControl(request.control(), view, actualTerm);
    validateLayout(request.layout());
    CatalogRecord previous = view.record(RecordKind.OWNER_LAYOUT, OwnershipProtocol.LAYOUT_NAME);
    if (previous != null) {
      require(!previous.deleted() && request.layout().equals(OwnershipProtocol.decodeLayout(previous.value())),
          Status.CONFLICT, "Changing the ownership layout requires a fenced migration protocol");
      return List.of();
    }
    return List.of(record(RecordKind.OWNER_LAYOUT, OwnershipProtocol.LAYOUT_NAME, 1, request.layout(), view, index));
  }

  private static List<CatalogRecord> assign(AssignOwner request, View view, long actualTerm, long index)
      throws IOException {
    validateControl(request.control(), view, actualTerm);
    Layout layout = layout(view);
    require(request.layoutGeneration() == layout.generation(), Status.FENCED, "Ownership layout generation differs");
    validateScope(request.scope(), layout);
    require(request.expectedEpoch() >= 0, Status.INVALID_REQUEST, "Expected owner epoch must be nonnegative");
    String name = OwnershipProtocol.name(request.scope());
    CatalogRecord previous = view.record(RecordKind.OWNER_ASSIGNMENT, name);
    long epoch = 0;
    if (previous != null) {
      require(!previous.deleted(), Status.CONFLICT, "Ownership assignments cannot be deleted");
      Assignment assignment = OwnershipProtocol.decodeAssignment(previous.value());
      validateAssignment(assignment, layout, view.historyId());
      require(request.scope().equals(assignment.scope()), Status.CONFLICT, "Ownership record scope differs");
      epoch = assignment.ownerEpoch();
    }
    require(request.expectedEpoch() == epoch, Status.CONFLICT, "Owner epoch changed");
    require(epoch < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Owner epochs exhausted");
    if (request.owner() != null) {
      eligible(request.owner(), view);
    }
    Assignment assignment = new Assignment(layout.generation(), request.scope(), epoch + 1, request.owner());
    return List.of(record(RecordKind.OWNER_ASSIGNMENT, name, epoch + 1, assignment, view, index));
  }

  static Assignment validateOwner(OwnerToken token, View view)
      throws IOException {
    require(token != null, Status.INVALID_REQUEST, "Owner token is required");
    require(view.historyId().equals(token.historyId()), Status.HISTORY_MISMATCH, "Owner history differs");
    Layout layout = layout(view);
    require(token.layoutGeneration() == layout.generation(), Status.FENCED, "Ownership layout generation differs");
    validateScope(token.scope(), layout);
    require(token.ownerEpoch() > 0, Status.INVALID_REQUEST, "Owner epoch must be positive");
    CatalogRecord record = view.record(RecordKind.OWNER_ASSIGNMENT, OwnershipProtocol.name(token.scope()));
    require(record != null && !record.deleted(), Status.FENCED, "Ownership assignment is absent");
    Assignment assignment = OwnershipProtocol.decodeAssignment(record.value());
    validateAssignment(assignment, layout, view.historyId());
    require(assignment.layoutGeneration() == token.layoutGeneration() && assignment.scope().equals(token.scope())
        && assignment.ownerEpoch() == token.ownerEpoch() && assignment.owner() != null
        && assignment.owner().equals(token.participant()), Status.FENCED, "Ownership assignment or token changed");
    eligible(token.participant(), view);
    return assignment;
  }

  static Assignment validateTableOwner(OwnerToken token, String tableName, View view)
      throws IOException {
    Assignment assignment = validateOwner(token, view);
    require(OwnershipProtocol.tableScope(layout(view), tableName).equals(token.scope()), Status.FENCED,
        "Owner scope does not cover the table");
    return assignment;
  }

  /// Validate a retained operation's historical owner without reviving its authority. Assignment and participant
  /// epochs may have advanced, while references to nonexistent future epochs indicate an inconsistent image.
  static void validateStoredOwner(OwnerToken token, View view)
      throws IOException {
    validateStoredOwnerShape(token, view.historyId());
    Layout layout = layout(view);
    require(token.layoutGeneration() == layout.generation(), Status.INVALID_REQUEST,
        "Stored owner layout generation differs");
    validateScope(token.scope(), layout);
    CatalogRecord record = view.record(RecordKind.OWNER_ASSIGNMENT, OwnershipProtocol.name(token.scope()));
    require(record != null && !record.deleted(), Status.INVALID_REQUEST, "Stored owner assignment is absent");
    Assignment assignment = OwnershipProtocol.decodeAssignment(record.value());
    validateAssignment(assignment, layout, view.historyId());
    require(assignment.scope().equals(token.scope()) && assignment.ownerEpoch() >= token.ownerEpoch()
        && (assignment.ownerEpoch() != token.ownerEpoch() || token.participant().equals(assignment.owner())),
        Status.INVALID_REQUEST, "Stored owner references an inconsistent assignment epoch");
    validateParticipantReference(token.participant(), view);
  }

  static void validateStoredOwnerShape(OwnerToken token, UUID historyId) {
    require(token != null, Status.INVALID_REQUEST, "Stored owner token is required");
    require(historyId.equals(token.historyId()), Status.HISTORY_MISMATCH, "Stored owner history differs");
    require(token.layoutGeneration() == SUPPORTED_LAYOUT.generation() && token.ownerEpoch() > 0, Status.INVALID_REQUEST,
        "Invalid stored owner layout generation or epoch");
    validateScope(token.scope());
    validateParticipantToken(token.participant(), historyId);
  }

  static void validateStoredTableOwner(OwnerToken token, String tableName, View view)
      throws IOException {
    validateStoredTableOwnerShape(token, tableName, view.historyId());
    validateStoredOwner(token, view);
  }

  static void validateStoredTableOwnerShape(OwnerToken token, String tableName, UUID historyId) {
    validateStoredOwnerShape(token, historyId);
    require(OwnershipProtocol.tableScope(SUPPORTED_LAYOUT, tableName).equals(token.scope()), Status.INVALID_REQUEST,
        "Stored owner scope does not cover the table");
  }

  private static void validateControl(ControlToken token, View view, long actualTerm) {
    require(token != null, Status.INVALID_REQUEST, "Control token is required");
    require(view.historyId().equals(token.historyId()), Status.HISTORY_MISMATCH, "Control history differs");
    ControlState control = view.control();
    require(control != null && control.controlEpoch() != null && control.policy() != null,
        Status.NOT_READY, "Ownership requires an activated control epoch and membership policy");
    require(control.controlEpoch().equals(token) && token.raftTerm() == actualTerm, Status.FENCED,
        "Control token does not match the actual committed Raft term");
  }

  private static void eligible(ParticipantToken token, View view) {
    validateParticipantToken(token, view.historyId());
    ParticipantState participant = view.participant(token.participantId());
    require(participant != null && token.equals(participant.participant()), Status.FENCED,
        "Owner participant incarnation or registration epoch changed");
    ControlState control = view.control();
    require(control != null && control.controlEpoch() != null && control.policy() != null,
        Status.NOT_READY, "Ownership requires committed membership control");
    require(participant.ready() && !participant.draining() && !participant.retired()
        && participant.expiryRound() > control.round(), Status.FENCED,
        "Owner participant is not ready or its lease is invalid");
  }

  private static Layout layout(View view)
      throws IOException {
    CatalogRecord record = view.record(RecordKind.OWNER_LAYOUT, OwnershipProtocol.LAYOUT_NAME);
    require(record != null && !record.deleted(), Status.NOT_READY, "Ownership layout is not configured");
    Layout layout = OwnershipProtocol.decodeLayout(record.value());
    validateLayout(layout);
    return layout;
  }

  static void validateLayout(Layout layout) {
    require(SUPPORTED_LAYOUT.equals(layout), Status.INVALID_REQUEST,
        "Only explicit ownership layout generation 1 with 24 Murmur2 shards is supported");
  }

  static void validateScope(Scope scope) {
    require(scope != null, Status.INVALID_REQUEST, "Owner scope is required");
    switch (scope) {
      case TableShard table -> require(table.shard() >= 0 && table.shard() < SUPPORTED_LAYOUT.tableShards(),
          Status.INVALID_REQUEST,
          "Owner table shard is outside the supported layout");
      case ClusterWork work -> identifier(work.name(), 256);
      case TablelessTask task -> identifier(task.taskType(), 256);
    }
  }

  private static void validateScope(Scope scope, Layout layout) {
    validateScope(scope);
    if (scope instanceof TableShard table) {
      require(table.shard() < layout.tableShards(), Status.INVALID_REQUEST, "Owner table shard exceeds the layout");
    }
  }

  private static void validateAssignment(Assignment assignment, Layout layout, UUID historyId) {
    require(assignment != null && assignment.layoutGeneration() == layout.generation()
        && assignment.ownerEpoch() > 0, Status.INVALID_REQUEST, "Invalid ownership assignment");
    validateScope(assignment.scope(), layout);
    if (assignment.owner() != null) {
      validateParticipantToken(assignment.owner(), historyId);
    }
  }

  private static void validateParticipantToken(ParticipantToken token, UUID historyId) {
    require(token != null && historyId.equals(token.historyId()), Status.HISTORY_MISMATCH,
        "Owner participant history differs");
    identifier(token.participantId(), 1024);
    require(token.role() == Role.CONTROLLER && token.incarnation() != null && token.registrationEpoch() > 0,
        Status.FENCED, "Ownership requires a complete controller participant token");
  }

  /// Validate persisted shape and identity without requiring an assignment's former owner to remain eligible.
  static void validateRecord(CatalogRecord record, UUID historyId, long snapshotIndex)
      throws IOException {
    require(record != null && (record.kind() == RecordKind.OWNER_LAYOUT
        || record.kind() == RecordKind.OWNER_ASSIGNMENT), Status.INVALID_REQUEST, "Invalid ownership record kind");
    require(!record.deleted() && record.schemaVersion() == MetadataProtocol.FORMAT_VERSION && record.version() > 0
        && record.lastModifiedIndex() > 0 && record.lastModifiedIndex() <= snapshotIndex && record.value() != null
        && objectId(historyId, record.kind(), record.name()).equals(record.objectId()), Status.INVALID_REQUEST,
        "Invalid ownership record envelope");
    if (record.kind() == RecordKind.OWNER_LAYOUT) {
      require(OwnershipProtocol.LAYOUT_NAME.equals(record.name()) && record.version() == 1, Status.INVALID_REQUEST,
          "Invalid ownership layout record identity/version");
      validateLayout(OwnershipProtocol.decodeLayout(record.value()));
    } else {
      Assignment assignment = OwnershipProtocol.decodeAssignment(record.value());
      validateAssignment(assignment, SUPPORTED_LAYOUT, historyId);
      require(OwnershipProtocol.name(assignment.scope()).equals(record.name())
          && assignment.ownerEpoch() == record.version(), Status.INVALID_REQUEST,
          "Ownership assignment identity/version differs");
    }
  }

  /// Whole-image validation permits expired, drained, retired and replaced owners. Such assignments remain durable
  /// fences but cannot authorize new work; their referenced registration must not come from an invented future epoch.
  static void validateReferences(CatalogRecord record, View view)
      throws IOException {
    if (record.kind() == RecordKind.OWNER_LAYOUT) {
      return;
    }
    require(record.kind() == RecordKind.OWNER_ASSIGNMENT, Status.INVALID_REQUEST, "Invalid ownership record kind");
    Assignment assignment = OwnershipProtocol.decodeAssignment(record.value());
    validateAssignment(assignment, layout(view), view.historyId());
    ParticipantToken owner = assignment.owner();
    if (owner != null) {
      validateParticipantReference(owner, view);
    }
  }

  private static void validateParticipantReference(ParticipantToken owner, View view) {
    ParticipantState participant = view.participant(owner.participantId());
    require(participant != null && participant.participant().role() == Role.CONTROLLER
        && participant.participant().registrationEpoch() >= owner.registrationEpoch()
        && (participant.participant().registrationEpoch() != owner.registrationEpoch()
            || participant.participant().equals(owner)), Status.INVALID_REQUEST,
        "Owner references an absent or inconsistent participant registration");
  }

  private static CatalogRecord record(RecordKind kind, String name, long version, Object value, View view, long index) {
    return new CatalogRecord(kind, name, objectId(view.historyId(), kind, name), version,
        MetadataProtocol.FORMAT_VERSION, index, false, MetadataProtocol.toTree(value));
  }

  private static UUID objectId(UUID historyId, RecordKind kind, String name) {
    return UUID.nameUUIDFromBytes((historyId + "\0" + kind + "\0" + name).getBytes(StandardCharsets.UTF_8));
  }

  static void identifier(String value, int maxLength) {
    require(value != null && !value.isBlank() && value.length() <= maxLength
        && value.chars().noneMatch(Character::isISOControl), Status.INVALID_REQUEST, "Invalid ownership identifier");
  }

  private static void require(boolean condition, Status status, String message) {
    if (!condition) {
      throw new CoordinationRejection(status, message);
    }
  }
}
