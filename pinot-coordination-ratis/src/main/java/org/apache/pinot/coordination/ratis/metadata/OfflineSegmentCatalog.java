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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.AssignReplica;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.PublishContent;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Stateless OFFLINE publication and assignment staging over a coherent catalog view. Each command stages at most
/// one record; the caller atomically commits it with the durable request outcome. No clocks or external I/O are used.
final class OfflineSegmentCatalog {
  private OfflineSegmentCatalog() {
  }

  interface View extends OwnershipCatalog.View {
    CatalogRecord record(UUID objectId);
  }

  static List<CatalogRecord> stage(Operation operation, View view, long index)
      throws IOException {
    require(operation != null && index > 0, Status.INVALID_REQUEST,
        "Offline operation and committed index are required");
    return switch (operation) {
      case PublishContent publish -> publish(publish, view, index);
      case AssignReplica assign -> assign(assign, view, index);
    };
  }

  private static List<CatalogRecord> publish(PublishContent request, View view, long index)
      throws IOException {
    Content content = request.content();
    validateContent(content);
    OwnershipCatalog.validateTableOwner(request.owner(), content.tableName(), view);
    validateVersion(request.expectedTable(), view.historyId());
    require(content.tableObjectId().equals(request.expectedTable().objectId()), Status.INVALID_REQUEST,
        "Publication expected table differs from its content table");
    currentVersion(request.expectedTable(), RecordKind.TABLE_CONFIG, content.tableName(), view);
    String name = OfflineSegmentProtocol.contentName(content.tableObjectId(), content.segmentName());
    require(view.record(RecordKind.OFFLINE_SEGMENT_CONTENT, name) == null
        && view.record(content.segmentObjectId()) == null, Status.CONFLICT,
        "Offline segment content names and object IDs cannot be overwritten or reused");
    return List.of(new CatalogRecord(RecordKind.OFFLINE_SEGMENT_CONTENT, name, content.segmentObjectId(), 1,
        MetadataProtocol.FORMAT_VERSION, index, false, MetadataProtocol.toTree(content)));
  }

  private static List<CatalogRecord> assign(AssignReplica request, View view, long index)
      throws IOException {
    require(request.segmentObjectId() != null && request.expectedEpoch() >= 0, Status.INVALID_REQUEST,
        "Replica assignment requires a segment identity and nonnegative expected epoch");
    Content content = storedContent(request.segmentObjectId(), view);
    OwnershipCatalog.validateTableOwner(request.owner(), content.tableName(), view);
    validateServerToken(request.server(), view.historyId());
    validateCompatibility(request.compatibility(), content.tableObjectId(), view.historyId());
    String name = OfflineSegmentProtocol.assignmentName(content.segmentObjectId(), request.server().participantId());
    CatalogRecord previous = view.record(RecordKind.OFFLINE_REPLICA_ASSIGNMENT, name);
    Assignment oldAssignment = null;
    if (previous != null) {
      require(!previous.deleted(), Status.CONFLICT, "Replica assignments cannot be deleted");
      oldAssignment = OfflineSegmentProtocol.decodeAssignment(previous.value());
      validateAssignment(oldAssignment, view.historyId());
      matchingContent(oldAssignment, content);
      require(oldAssignment.server().participantId().equals(request.server().participantId()), Status.CONFLICT,
          "Replica assignment name belongs to another server identity");
    }
    long epoch = oldAssignment == null ? 0 : oldAssignment.assignmentEpoch();
    require(request.expectedEpoch() == epoch, Status.CONFLICT, "Replica assignment epoch changed");
    require(epoch < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Replica assignment epochs exhausted");
    if (request.desiredOnline()) {
      currentTable(content, view);
      currentServer(request.server(), view);
      currentCompatibility(request.compatibility(), content, view);
    } else {
      require(oldAssignment != null && oldAssignment.server().equals(request.server())
          && oldAssignment.compatibility().equals(request.compatibility()), Status.FENCED,
          "Replica release requires the exact prior server registration and compatibility");
    }
    UUID objectId = OfflineSegmentProtocol.assignmentObjectId(view.historyId(), content.segmentObjectId(),
        request.server().participantId());
    Assignment assignment = new Assignment(content.tableName(), content.tableObjectId(), content.segmentName(),
        content.segmentObjectId(), content.contentSha256(), epoch + 1, request.server(),
        OfflineSegmentProtocol.materializationId(view.historyId(), objectId, epoch + 1), request.compatibility(),
        request.desiredOnline());
    return List.of(new CatalogRecord(RecordKind.OFFLINE_REPLICA_ASSIGNMENT, name, objectId, epoch + 1,
        MetadataProtocol.FORMAT_VERSION, index, false, MetadataProtocol.toTree(assignment)));
  }

  static Content currentContent(UUID segmentObjectId, View view)
      throws IOException {
    Content content = storedContent(segmentObjectId, view);
    currentTable(content, view);
    return content;
  }

  /// Returns authority to target a current desired replica, not proof of its installed or serving state.
  static Assignment currentAssignment(UUID segmentObjectId, ParticipantToken server, View view)
      throws IOException {
    validateServerToken(server, view.historyId());
    Content content = currentContent(segmentObjectId, view);
    CatalogRecord record = view.record(RecordKind.OFFLINE_REPLICA_ASSIGNMENT,
        OfflineSegmentProtocol.assignmentName(segmentObjectId, server.participantId()));
    require(record != null && !record.deleted(), Status.FENCED, "Desired replica assignment is absent");
    Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
    validateAssignment(assignment, view.historyId());
    matchingContent(assignment, content);
    require(assignment.desiredOnline() && assignment.server().equals(server), Status.FENCED,
        "Desired replica is offline or its target registration changed");
    currentServer(server, view);
    currentCompatibility(assignment.compatibility(), content, view);
    return assignment;
  }

  private static Content storedContent(UUID segmentObjectId, View view)
      throws IOException {
    require(segmentObjectId != null, Status.INVALID_REQUEST, "Segment identity is required");
    CatalogRecord record = view.record(segmentObjectId);
    require(record != null && record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT && !record.deleted(),
        Status.NOT_FOUND, "Immutable offline segment content is absent");
    Content content = OfflineSegmentProtocol.decodeContent(record.value());
    validateContent(content);
    require(segmentObjectId.equals(content.segmentObjectId()), Status.CONFLICT, "Offline segment identity differs");
    return content;
  }

  private static void currentTable(Content content, View view) {
    CatalogRecord table = view.record(RecordKind.TABLE_CONFIG, content.tableName());
    require(table != null && !table.deleted() && table.objectId().equals(content.tableObjectId()), Status.FENCED,
        "Offline content table incarnation changed or was deleted");
  }

  private static void currentServer(ParticipantToken token, View view) {
    ParticipantState server = view.participant(token.participantId());
    require(server != null && server.participant().equals(token), Status.FENCED,
        "Replica target server registration changed or is absent");
    ControlState control = view.control();
    require(control != null && control.controlEpoch() != null && control.policy() != null, Status.NOT_READY,
        "Replica assignment requires committed membership control");
    require(server.ready() && !server.retired() && !server.draining() && server.expiryRound() > control.round(),
        Status.FENCED, "Replica target server is not ready or its lease is invalid");
  }

  private static void currentCompatibility(Compatibility compatibility, Content content, View view) {
    currentVersion(compatibility.table(), RecordKind.TABLE_CONFIG, content.tableName(), view);
    currentVersion(compatibility.schema(), RecordKind.SCHEMA,
        TableNameBuilder.extractRawTableName(content.tableName()), view);
  }

  private static void currentVersion(ObjectVersion version, RecordKind kind, String name, View view) {
    CatalogRecord record = view.record(version.objectId());
    CatalogRecord active = view.record(kind, name);
    require(record != null && !record.deleted() && record.kind() == kind && name.equals(record.name())
        && record.version() == version.version() && active != null && active.objectId().equals(version.objectId()),
        Status.CONFLICT, "Offline materialization dependency kind, identity or version changed");
  }

  private static void matchingContent(Assignment assignment, Content content) {
    require(assignment.segmentObjectId().equals(content.segmentObjectId())
        && assignment.tableObjectId().equals(content.tableObjectId())
        && assignment.tableName().equals(content.tableName())
        && assignment.segmentName().equals(content.segmentName())
        && assignment.contentSha256().equals(content.contentSha256()), Status.INVALID_REQUEST,
        "Replica assignment differs from its immutable content");
  }

  /// Per-record validation also applies to superseded retained journal entries, independent of current liveness.
  static void validateRecord(CatalogRecord record, UUID historyId, long snapshotIndex)
      throws IOException {
    require(record != null && (record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT
        || record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT) && !record.deleted()
        && record.schemaVersion() == MetadataProtocol.FORMAT_VERSION && record.version() > 0
        && record.lastModifiedIndex() > 0 && record.lastModifiedIndex() <= snapshotIndex && record.value() != null,
        Status.INVALID_REQUEST, "Invalid offline catalog record envelope");
    if (record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT) {
      Content content = OfflineSegmentProtocol.decodeContent(record.value());
      validateContent(content);
      require(record.version() == 1 && content.segmentObjectId().equals(record.objectId())
          && OfflineSegmentProtocol.contentName(content.tableObjectId(), content.segmentName()).equals(record.name()),
          Status.INVALID_REQUEST, "Immutable offline content record identity differs");
    } else {
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
      validateAssignment(assignment, historyId);
      require(record.version() == assignment.assignmentEpoch()
          && OfflineSegmentProtocol.assignmentObjectId(historyId, assignment.segmentObjectId(),
              assignment.server().participantId()).equals(record.objectId())
          && OfflineSegmentProtocol.assignmentName(assignment.segmentObjectId(), assignment.server().participantId())
              .equals(record.name()), Status.INVALID_REQUEST, "Replica assignment record identity or epoch differs");
    }
  }

  /// Historical references survive table/schema deletion, participant replacement and dependency version changes.
  /// The retained object and registration epochs still have to exist; none of these references restores authority.
  static void validateReferences(CatalogRecord record, View view)
      throws IOException {
    if (record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT) {
      Content content = OfflineSegmentProtocol.decodeContent(record.value());
      historicalVersion(new ObjectVersion(view.historyId(), content.tableObjectId(), 1), RecordKind.TABLE_CONFIG,
          content.tableName(), view);
    } else {
      require(record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT, Status.INVALID_REQUEST,
          "Invalid offline record kind");
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
      Content content = storedContent(assignment.segmentObjectId(), view);
      matchingContent(assignment, content);
      CatalogRecord contentRecord = view.record(content.segmentObjectId());
      require(contentRecord.lastModifiedIndex() <= record.lastModifiedIndex(), Status.INVALID_REQUEST,
          "Replica assignment precedes publication of its immutable content");
      historicalVersion(assignment.compatibility().table(), RecordKind.TABLE_CONFIG, content.tableName(), view);
      historicalVersion(assignment.compatibility().schema(), RecordKind.SCHEMA,
          TableNameBuilder.extractRawTableName(content.tableName()), view);
      ParticipantToken token = assignment.server();
      ParticipantState server = view.participant(token.participantId());
      require(server != null && server.participant().role() == Role.SERVER
          && server.participant().registrationEpoch() >= token.registrationEpoch()
          && (server.participant().registrationEpoch() > token.registrationEpoch()
              || server.participant().equals(token)), Status.INVALID_REQUEST,
          "Replica assignment references an absent or future server registration");
    }
  }

  private static void historicalVersion(ObjectVersion version, RecordKind kind, String name, View view) {
    CatalogRecord record = view.record(version.objectId());
    require(record != null && record.kind() == kind && record.name().equals(name)
        && record.version() >= version.version(), Status.INVALID_REQUEST,
        "Offline catalog references an absent, mismatched or future dependency");
  }

  static void validateContent(Content content) {
    require(content != null && content.tableObjectId() != null && content.segmentObjectId() != null,
        Status.INVALID_REQUEST, "Offline content requires complete table and segment identities");
    tableName(content.tableName());
    identifier(content.segmentName(), 1024);
    digest(content.contentSha256());
    identifier(content.downloadUri(), 8192);
    try {
      URI uri = new URI(content.downloadUri());
      require(uri.isAbsolute() && uri.getFragment() == null, Status.INVALID_REQUEST,
          "Offline content requires an absolute immutable content URI without a fragment");
    } catch (URISyntaxException e) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, "Invalid offline content URI");
    }
    LoaderDescriptor loader = content.loader();
    require(loader != null && loader.crc() >= 0 && loader.dataCrc() >= -1
        && (!loader.useDataCrc() || loader.dataCrc() >= 0) && loader.creationTimeMs() >= -1 && loader.pushTimeMs() >= -1
        && loader.customConfigs() != null && loader.customConfigs().size() <= OfflineSegmentProtocol.MAX_CUSTOM_CONFIGS,
        Status.INVALID_REQUEST, "Invalid or oversized offline loader descriptor");
    if (loader.tier() != null) {
      identifier(loader.tier(), 1024);
    }
    if (loader.crypterName() != null) {
      identifier(loader.crypterName(), 1024);
    }
    for (Map.Entry<String, String> entry : loader.customConfigs().entrySet()) {
      identifier(entry.getKey(), 1024);
      require(entry.getValue() != null && entry.getValue().length() <= 4096
          && StandardCharsets.UTF_8.newEncoder().canEncode(entry.getValue()), Status.INVALID_REQUEST,
          "Invalid or oversized offline loader custom value");
    }
  }

  private static void validateAssignment(Assignment assignment, UUID historyId) {
    require(assignment != null && assignment.tableObjectId() != null && assignment.segmentObjectId() != null
        && assignment.assignmentEpoch() > 0, Status.INVALID_REQUEST, "Invalid offline replica assignment");
    tableName(assignment.tableName());
    identifier(assignment.segmentName(), 1024);
    digest(assignment.contentSha256());
    validateServerToken(assignment.server(), historyId);
    validateCompatibility(assignment.compatibility(), assignment.tableObjectId(), historyId);
    UUID objectId = OfflineSegmentProtocol.assignmentObjectId(historyId, assignment.segmentObjectId(),
        assignment.server().participantId());
    require(OfflineSegmentProtocol.materializationId(historyId, objectId, assignment.assignmentEpoch())
        .equals(assignment.materializationId()), Status.INVALID_REQUEST, "Replica materialization identity differs");
  }

  private static void validateCompatibility(Compatibility compatibility, UUID tableId, UUID historyId) {
    require(compatibility != null, Status.INVALID_REQUEST, "Replica materialization compatibility is required");
    validateVersion(compatibility.table(), historyId);
    validateVersion(compatibility.schema(), historyId);
    require(tableId.equals(compatibility.table().objectId())
        && !compatibility.table().objectId().equals(compatibility.schema().objectId()), Status.INVALID_REQUEST,
        "Replica compatibility requires distinct exact table and schema identities");
  }

  private static void validateVersion(ObjectVersion version, UUID historyId) {
    require(version != null && historyId.equals(version.historyId()), Status.HISTORY_MISMATCH,
        "Offline dependency history differs");
    require(version.objectId() != null && version.version() > 0, Status.INVALID_REQUEST,
        "Offline dependency identity and positive version are required");
  }

  private static void validateServerToken(ParticipantToken token, UUID historyId) {
    require(token != null && historyId.equals(token.historyId()), Status.HISTORY_MISMATCH,
        "Replica target server history differs");
    identifier(token.participantId(), 1024);
    require(token.role() == Role.SERVER && token.incarnation() != null && token.registrationEpoch() > 0, Status.FENCED,
        "Replica target requires a complete SERVER participant token");
  }

  private static void tableName(String name) {
    identifier(name, 1024);
    require(TableNameBuilder.getTableTypeFromTableName(name) == TableType.OFFLINE
        && name.chars().noneMatch(Character::isWhitespace), Status.INVALID_REQUEST,
        "Offline content requires a canonical OFFLINE table name");
    String raw = TableNameBuilder.extractRawTableName(name);
    int separator = raw.indexOf('.');
    require(!raw.isEmpty() && (separator < 0 || separator > 0 && separator < raw.length() - 1
        && separator == raw.lastIndexOf('.')), Status.INVALID_REQUEST, "Invalid database-qualified offline table name");
  }

  private static void digest(String value) {
    require(value != null && value.matches("[0-9a-f]{64}"), Status.INVALID_REQUEST,
        "Offline content requires a lowercase SHA-256 digest");
  }

  static void identifier(String value, int maximum) {
    require(value != null && !value.isBlank() && value.length() <= maximum
        && value.chars().noneMatch(Character::isISOControl) && StandardCharsets.UTF_8.newEncoder().canEncode(value),
        Status.INVALID_REQUEST, "Invalid offline catalog identifier");
  }

  private static void require(boolean condition, Status status, String message) {
    if (!condition) {
      throw new CoordinationRejection(status, message);
    }
  }
}
